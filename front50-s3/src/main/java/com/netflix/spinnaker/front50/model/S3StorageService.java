/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.front50.model;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.policy.Condition;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ReceiptHandleIsInvalidException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.front50.exception.NotFoundException;
import com.netflix.spinnaker.security.AuthenticatedRequest;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class S3StorageService implements StorageService {
  private static final Logger log = LoggerFactory.getLogger(S3StorageService.class);

  private final ObjectMapper objectMapper;
  private final AmazonS3 amazonS3;
  private final AmazonSQS amazonSQS;
  private final AmazonSNS amazonSNS;
  private final String bucket;
  private final String rootFolder;
  private final Boolean readOnlyMode;
  private final String region;

  private final ObjectKeyLoader objectKeyLoader;

  public S3StorageService(ObjectMapper objectMapper,
                          AmazonS3 amazonS3,
                          AmazonSQS amazonSQS,
                          AmazonSNS amazonSNS,
                          ObjectKeyLoader objectKeyLoader,
                          String bucket,
                          String rootFolder,
                          Boolean readOnlyMode,
                          String region) {
    this.objectMapper = objectMapper;
    this.amazonS3 = amazonS3;
    this.amazonSQS = amazonSQS;
    this.amazonSNS = amazonSNS;
    this.objectKeyLoader = objectKeyLoader;
    this.bucket = bucket;
    this.rootFolder = rootFolder;
    this.readOnlyMode = readOnlyMode;
    this.region = region;
  }

  @PreDestroy
  void shutdown() {
    objectKeyLoader.shutdown();
  }

  @Override
  public void ensureBucketExists() {
    HeadBucketRequest request = new HeadBucketRequest(bucket);
    try {
      amazonS3.headBucket(request);
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() == 404) {
        if (StringUtils.isNullOrEmpty(region)) {
          log.info("Creating bucket " + bucket + " in default region");
          amazonS3.createBucket(bucket);
        } else {
          log.info("Creating bucket " + bucket + " in region " + region + "...");
          amazonS3.createBucket(bucket, region);
        }

        log.info("Enabling versioning of the S3 bucket " + bucket);
        BucketVersioningConfiguration configuration =
          new BucketVersioningConfiguration().withStatus("Enabled");

        SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest =
          new SetBucketVersioningConfigurationRequest(bucket, configuration);

        amazonS3.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest);

      } else {
        throw e;
      }
    }
  }

  @Override
  public boolean supportsVersioning() {
    return true;
  }

  @Override
  public <T extends Timestamped> T loadObject(ObjectType objectType, String objectKey) throws NotFoundException {
    try {
      S3Object s3Object = amazonS3.getObject(bucket, buildS3Key(objectType.group, objectKey, objectType.defaultMetadataFilename));
      T item = deserialize(s3Object, (Class<T>) objectType.clazz);
      item.setLastModified(s3Object.getObjectMetadata().getLastModified().getTime());
      return item;
    } catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 404) {
        throw new NotFoundException("Object not found (key: " + objectKey + ")");
      }
      throw e;
    } catch (IOException e) {
      throw new IllegalStateException("Unable to deserialize object (key: " + objectKey + ")", e);
    }
  }

  @Override
  public void deleteObject(ObjectType objectType, String objectKey) {
    if (readOnlyMode) {
      throw new ReadOnlyModeException();
    }
    amazonS3.deleteObject(bucket, buildS3Key(objectType.group, objectKey, objectType.defaultMetadataFilename));
    writeLastModified(objectType.group);
  }

  @Override
  public <T extends Timestamped> void storeObject(ObjectType objectType, String objectKey, T item) {
    if (readOnlyMode) {
      throw new ReadOnlyModeException();
    }
    try {
      item.setLastModifiedBy(AuthenticatedRequest.getSpinnakerUser().orElse("anonymous"));
      byte[] bytes = objectMapper.writeValueAsBytes(item);

      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentLength(bytes.length);
      objectMetadata.setContentMD5(new String(org.apache.commons.codec.binary.Base64.encodeBase64(DigestUtils.md5(bytes))));

      amazonS3.putObject(
        bucket,
        buildS3Key(objectType.group, objectKey, objectType.defaultMetadataFilename),
        new ByteArrayInputStream(bytes),
        objectMetadata
      );
      writeLastModified(objectType.group);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Map<String, Long> listObjectKeys(ObjectType objectType) {
    return objectKeyLoader.listObjectKeys(objectType);
  }

  @Override
  public <T extends Timestamped> Collection<T> listObjectVersions(ObjectType objectType,
                                                                  String objectKey,
                                                                  int maxResults) throws NotFoundException {
    try {
      VersionListing versionListing = amazonS3.listVersions(
        new ListVersionsRequest(
          bucket,
          buildS3Key(objectType.group, objectKey, objectType.defaultMetadataFilename),
          null,
          null,
          null,
          maxResults
        )
      );
      return versionListing.getVersionSummaries().stream().map(s3VersionSummary -> {
        try {
          S3Object s3Object = amazonS3.getObject(
            new GetObjectRequest(bucket, buildS3Key(objectType.group, objectKey, objectType.defaultMetadataFilename), s3VersionSummary.getVersionId())
          );
          T item = deserialize(s3Object, (Class<T>) objectType.clazz);
          item.setLastModified(s3Object.getObjectMetadata().getLastModified().getTime());
          return item;
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }).collect(Collectors.toList());
    } catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 404) {
        throw new NotFoundException(String.format("No item found with id of %s", objectKey.toLowerCase()));
      }

      throw e;
    }
  }

  @Override
  public long getLastModified(ObjectType objectType) {
    try {
      Map<String, Long> lastModified = objectMapper.readValue(
        amazonS3.getObject(bucket, buildTypedFolder(rootFolder, objectType.group) + "/last-modified.json").getObjectContent(),
        Map.class
      );

      return lastModified.get("lastModified");
    } catch (Exception e) {
      return 0L;
    }
  }

  @Override
  public long getHealthIntervalMillis() {
    return Duration.ofSeconds(2).toMillis();
  }

  private void writeLastModified(String group) {
    if (readOnlyMode) {
      throw new ReadOnlyModeException();
    }
    try {
      byte[] bytes = objectMapper.writeValueAsBytes(Collections.singletonMap("lastModified", System.currentTimeMillis()));

      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentLength(bytes.length);
      objectMetadata.setContentMD5(new String(org.apache.commons.codec.binary.Base64.encodeBase64(DigestUtils.md5(bytes))));

      amazonS3.putObject(
        bucket,
        buildTypedFolder(rootFolder, group) + "/last-modified.json",
        new ByteArrayInputStream(bytes),
        objectMetadata
      );
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  private <T extends Timestamped> T deserialize(S3Object s3Object, Class<T> clazz) throws IOException {
    return objectMapper.readValue(s3Object.getObjectContent(), clazz);
  }

  private String buildS3Key(String group, String objectKey, String metadataFilename) {
    if (objectKey.endsWith(metadataFilename)) {
      return objectKey;
    }

    return (buildTypedFolder(rootFolder, group) + "/" + objectKey.toLowerCase() + "/" + metadataFilename).replace("//", "/");
  }

  static String buildTypedFolder(String rootFolder, String type) {
    return (rootFolder + "/" + type).replaceAll("//", "/");
  }
}
