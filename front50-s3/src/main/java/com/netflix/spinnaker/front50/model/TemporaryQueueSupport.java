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
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.GetTopicAttributesRequest;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiptHandleIsInvalidException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

class TemporaryQueueSupport {
  private final Logger log = LoggerFactory.getLogger(TemporaryQueueSupport.class);

  private final AmazonSQS amazonSQS;
  private final AmazonSNS amazonSNS;

  private final TemporaryQueue temporaryQueue;

  TemporaryQueueSupport(AmazonSQS amazonSQS,
                        AmazonSNS amazonSNS,
                        String snsTopicArn) {
    this.amazonSQS = amazonSQS;
    this.amazonSNS = amazonSNS;

    String topicName = snsTopicArn.substring(snsTopicArn.lastIndexOf(":") + 1);
    String sqsQueueName = topicName + "-" + UUID.randomUUID().toString();
    String sqsQueueArn = snsTopicArn.substring(0, snsTopicArn.lastIndexOf(":") + 1).replace("sns", "sqs") + sqsQueueName;

    this.temporaryQueue = createQueue(snsTopicArn, sqsQueueArn, sqsQueueName);
  }

  List<Message> fetchMessages() {
    ReceiveMessageResult receiveMessageResult = amazonSQS.receiveMessage(
      new ReceiveMessageRequest(temporaryQueue.sqsQueueUrl)
        .withMaxNumberOfMessages(10)
        .withVisibilityTimeout(1)
        .withWaitTimeSeconds(20)
    );

    return receiveMessageResult.getMessages();
  }

  void markMessageAsHandled(String receiptHandle) {
    try {
      amazonSQS.deleteMessage(temporaryQueue.sqsQueueUrl, receiptHandle);
    } catch (ReceiptHandleIsInvalidException e) {
      log.warn("Error deleting message, reason: {} (receiptHandle: {})", e.getMessage(), receiptHandle);
    }
  }

  void shutdown() {
    try {
      log.debug("Removing Temporary S3 Notification Queue: {}", temporaryQueue.sqsQueueUrl);
      amazonSQS.deleteQueue(temporaryQueue.sqsQueueUrl);
      log.debug("Removed Temporary S3 Notification Queue: {}", temporaryQueue.sqsQueueUrl);
    } catch (Exception e) {
      log.error("Unable to remove queue: {} (reason: {})", temporaryQueue.sqsQueueUrl, e.getMessage(), e);
    }

    try {
      log.debug("Removing S3 Notification Subscription: {}", temporaryQueue.snsTopicSubscriptionArn);
      amazonSNS.unsubscribe(temporaryQueue.snsTopicSubscriptionArn);
      log.debug("Removed S3 Notification Subscription: {}", temporaryQueue.snsTopicSubscriptionArn);
    } catch (Exception e) {
      log.error("Unable to unsubscribe queue from topic: {} (reason: {})", temporaryQueue.snsTopicSubscriptionArn, e.getMessage(), e);
    }
  }

  private TemporaryQueue createQueue(String snsTopicArn, String sqsQueueArn, String sqsQueueName) {
    // TODO-AJ ensure that message retention is set to 60s
    String sqsQueueUrl = amazonSQS.createQueue(sqsQueueName).getQueueUrl();
    log.info("Created Temporary S3 Notification Queue: {}", sqsQueueUrl);

    String snsTopicSubscriptionArn = amazonSNS.subscribe(snsTopicArn, "sqs", sqsQueueArn).getSubscriptionArn();

    Statement snsStatement = new Statement(Statement.Effect.Allow).withActions(SQSActions.SendMessage);
    snsStatement.setPrincipals(Principal.All);
    snsStatement.setResources(Collections.singletonList(new Resource(sqsQueueArn)));
    snsStatement.setConditions(Collections.singletonList(
      new Condition().withType("ArnEquals").withConditionKey("aws:SourceArn").withValues(snsTopicArn)
    ));

    Policy allowSnsPolicy = new Policy("allow-sns", Collections.singletonList(snsStatement));

    HashMap<String, String> attributes = new HashMap<>();
    attributes.put("Policy", allowSnsPolicy.toJson());
    amazonSQS.setQueueAttributes(
      sqsQueueUrl,
      attributes
    );

    return new TemporaryQueue(snsTopicArn, sqsQueueArn, sqsQueueUrl, snsTopicSubscriptionArn);
  }

  private static class TemporaryQueue {
    final String snsTopicArn;
    final String sqsQueueArn;
    final String sqsQueueUrl;
    final String snsTopicSubscriptionArn;

    TemporaryQueue(String snsTopicArn, String sqsQueueArn, String sqsQueueUrl, String snsTopicSubscriptionArn) {
      this.snsTopicArn = snsTopicArn;
      this.sqsQueueArn = sqsQueueArn;
      this.sqsQueueUrl = sqsQueueUrl;
      this.snsTopicSubscriptionArn = snsTopicSubscriptionArn;
    }
  }
}
