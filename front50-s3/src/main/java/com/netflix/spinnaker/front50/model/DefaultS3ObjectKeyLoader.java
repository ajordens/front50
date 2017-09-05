/*
 * Copyright 2017 Netflix, Inc.
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultS3ObjectKeyLoader implements ObjectKeyLoader {
  private final Logger log = LoggerFactory.getLogger(getClass());

  private final AmazonS3 amazonS3;
  private final String bucket;
  private final String rootFolder;

  public DefaultS3ObjectKeyLoader(AmazonS3 amazonS3, String bucket, String rootFolder) {
    this.amazonS3 = amazonS3;
    this.bucket = bucket;
    this.rootFolder = rootFolder;
  }

  @Override
  public Map<String, Long> listObjectKeys(ObjectType objectType) {
    long startTime = System.currentTimeMillis();

    ObjectListing bucketListing = amazonS3.listObjects(
      new ListObjectsRequest(bucket, S3StorageService.buildTypedFolder(rootFolder, objectType.group), null, null, 10000)
    );
    List<S3ObjectSummary> summaries = bucketListing.getObjectSummaries();

    while (bucketListing.isTruncated()) {
      bucketListing = amazonS3.listNextBatchOfObjects(bucketListing);
      summaries.addAll(bucketListing.getObjectSummaries());
    }

    log.debug("Took {}ms to fetch {} object keys for {}", (System.currentTimeMillis() - startTime), summaries.size(), objectType);

    return summaries
      .stream()
      .filter(s -> filterS3ObjectSummary(s, objectType.defaultMetadataFilename))
      .collect(Collectors.toMap((s -> buildObjectKey(objectType, s.getKey())), (s -> s.getLastModified().getTime())));
  }

  @Override
  public void shutdown() {
    // do nothing
  }

  private String buildObjectKey(ObjectType objectType, String s3Key) {
    return s3Key
      .replaceAll(S3StorageService.buildTypedFolder(rootFolder, objectType.group) + "/", "")
      .replaceAll("/" + objectType.defaultMetadataFilename, "");
  }

  private boolean filterS3ObjectSummary(S3ObjectSummary s3ObjectSummary, String metadataFilename) {
    return s3ObjectSummary.getKey().endsWith(metadataFilename);
  }
}
