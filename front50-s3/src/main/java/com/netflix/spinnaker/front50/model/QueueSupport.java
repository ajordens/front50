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
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

class QueueSupport {
  private final Logger log = LoggerFactory.getLogger(QueueSupport.class);

  QueueDetails uniqueQueue(AmazonSQS amazonSQS, AmazonSNS amazonSNS, String snsTopicArn) {
    String sqsQueueName = snsTopicArn.substring(snsTopicArn.lastIndexOf(':') + 1) + "-" + UUID.randomUUID().toString();

    String sqsQueueUrl = createQueue(amazonSQS, sqsQueueName);
    log.info("Created Temporary S3 Notification Queue: {}", sqsQueueUrl);

    String queueArn = snsTopicArn.replaceFirst("sns", "sqs").substring(0, snsTopicArn.lastIndexOf(':') + 1) + sqsQueueName;
    String subscriptionArn = amazonSNS.subscribe(snsTopicArn, "sqs", queueArn).getSubscriptionArn();

    Statement snsStatement = new Statement(Statement.Effect.Allow).withActions(SQSActions.SendMessage);
    snsStatement.setPrincipals(Principal.All);
    snsStatement.setResources(Collections.singletonList(new Resource(queueArn)));
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

    return new QueueDetails(sqsQueueUrl, subscriptionArn);
  }

  private String createQueue(AmazonSQS amazonSQS, String queueName) {
    // TODO-AJ ensure that message retention is set to 60s
    return amazonSQS.createQueue(queueName).getQueueUrl();
  }

  static class QueueDetails {
    final String sqsQueueUrl;
    final String snsTopicSubscriptionArn;

    public QueueDetails(String sqsQueueUrl, String snsTopicSubscriptionArn) {
      this.sqsQueueUrl = sqsQueueUrl;
      this.snsTopicSubscriptionArn = snsTopicSubscriptionArn;
    }
  }
}
