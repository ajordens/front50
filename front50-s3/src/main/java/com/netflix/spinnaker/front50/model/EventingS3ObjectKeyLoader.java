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

import com.amazonaws.auth.policy.Condition;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ReceiptHandleIsInvalidException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.front50.model.events.S3Event;
import com.netflix.spinnaker.front50.model.events.S3EventWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class EventingS3ObjectKeyLoader implements ObjectKeyLoader, Runnable {
  private static final Logger log = LoggerFactory.getLogger(EventingS3ObjectKeyLoader.class);

  private final ObjectMapper objectMapper;
  private final AmazonS3 amazonS3;
  private final AmazonSQS amazonSQS;
  private final AmazonSNS amazonSNS;

  private final DefaultS3ObjectKeyLoader delegate;

  private final String rootFolder;
  private final String sqsQueueUrl;
  private final String subscriptionArn;

  private boolean pollForMessages = true;

//  ConcurrentHashMap<ObjectType, AtomicReference<Map<String, Long>>> objectKeysByObjectType = new ConcurrentHashMap<>();

  public EventingS3ObjectKeyLoader(TaskScheduler taskScheduler,
                                   ObjectMapper objectMapper,
                                   AmazonS3 amazonS3,
                                   AmazonSQS amazonSQS,
                                   AmazonSNS amazonSNS,
                                   DefaultS3ObjectKeyLoader delegate,
                                   String rootFolder,
                                   String snsTopicArn) {
    this.objectMapper = objectMapper;
    this.amazonS3 = amazonS3;
    this.amazonSQS = amazonSQS;
    this.amazonSNS = amazonSNS;

    this.delegate = delegate;
    this.rootFolder = rootFolder;

    QueueSupport queueSupport = new QueueSupport();
    QueueSupport.QueueDetails queueDetails = queueSupport.createQueue();

    taskScheduler.schedule(this, new Date());
  }

  @Override
  public void shutdown() {
    log.debug("Stopping ...");
    pollForMessages = false;

    try {
      log.debug("Removing Temporary S3 Notification Queue: {}", sqsQueueUrl);
      amazonSQS.deleteQueue(sqsQueueUrl);
      log.debug("Removed Temporary S3 Notification Queue: {}", sqsQueueUrl);
    } catch (Exception e) {
      log.error("Unable to remove queue: {} (reason: {})", sqsQueueUrl, e.getMessage(), e);
    }

    try {
      log.debug("Removing S3 Notification Subscription: {}", subscriptionArn);
      amazonSNS.unsubscribe(subscriptionArn);
      log.debug("Removed S3 Notification Subscription: {}", subscriptionArn);
    } catch (Exception e) {
      log.error("Unable to unsubscribe queue from topic: {} (reason: {})", subscriptionArn, e.getMessage(), e);
    }

    log.debug("Stopped");
    delegate.shutdown();
  }

  @Override
  public Map<String, Long> listObjectKeys(ObjectType objectType) {
    return delegate.listObjectKeys(objectType);
  }

  @Override
  public void run() {
    while(pollForMessages) {
      ReceiveMessageResult receiveMessageResult = amazonSQS.receiveMessage(
        new ReceiveMessageRequest(sqsQueueUrl)
          .withMaxNumberOfMessages(10)
          .withVisibilityTimeout(1)
          .withWaitTimeSeconds(20)
      );

      if (receiveMessageResult.getMessages().isEmpty()) {
        log.info("No messages");
        // No messages
        continue;
      }

      receiveMessageResult.getMessages().forEach(message -> {
        S3Event notificationMessage = unmarshall(objectMapper, message.getBody());
        if (notificationMessage != null) {
          notificationMessage.records.forEach(record -> {
            if (record.s3.object.key.endsWith("last-modified.json")) {
              return;
            }

            String eventType = record.eventName;
            KeyWithObjectType keyWithObjectType = buildObjectKey(rootFolder, record.s3.object.key);
            log.info("Message ==> " + eventType + " --> " + keyWithObjectType.objectType + "[" + keyWithObjectType.key + "]");
          });
        }

        try {
          amazonSQS.deleteMessage(sqsQueueUrl, message.getReceiptHandle());
        } catch (ReceiptHandleIsInvalidException e) {
          log.warn("Error deleting lifecycle message, reason: {} (receiptHandle: {})", e.getMessage(), message.getReceiptHandle());
        }
      });
    }
  }

  private static KeyWithObjectType buildObjectKey(String rootFolder, String s3Key) {
    s3Key = s3Key.replace(rootFolder, "");
    s3Key = s3Key.substring(s3Key.indexOf("/") + 1);

    String metadataFilename = s3Key.substring(s3Key.lastIndexOf("/") + 1);
    s3Key = s3Key.substring(0, s3Key.lastIndexOf("/"));

    try {
      s3Key = URLDecoder.decode(s3Key, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("Invalid key '" + s3Key + "' (non utf-8)");
    }

    ObjectType objectType = Arrays.stream(ObjectType.values())
      .filter(o -> o.defaultMetadataFilename.equalsIgnoreCase(metadataFilename))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException("No ObjectType found (defaultMetadataFileName: " + metadataFilename + ")"));

    return new KeyWithObjectType(objectType, s3Key);
  }

  private static S3Event unmarshall(ObjectMapper objectMapper, String messageBody) {
    S3EventWrapper notificationMessageWrapper = null;
    try {
      notificationMessageWrapper = objectMapper.readValue(messageBody, S3EventWrapper.class);
    } catch (IOException e) {
      log.debug("Unable unmarshal NotificationMessageWrapper (body: {})", messageBody, e);
      return null;
    }

    try {
      return objectMapper.readValue(notificationMessageWrapper.message, S3Event.class);
    } catch (IOException e) {
      log.debug("Unable unmarshal NotificationMessage (body: {})", notificationMessageWrapper.message, e);
      return null;
    }
  }

  private static class KeyWithObjectType {
    final ObjectType objectType;
    final String key;

    KeyWithObjectType(ObjectType objectType, String key) {
      this.objectType = objectType;
      this.key = key;
    }
  }
}
