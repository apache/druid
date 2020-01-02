/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PubsubRecordSupplier implements Closeable
{
  private static final Logger log = new Logger(PubsubRecordSupplier.class);

  private boolean closed;

  public PubsubRecordSupplier(
      ObjectMapper sortingMapper
  )
  {
  }

  @Nonnull
  public List<ReceivedMessage> poll(long timeout)
  {
    try {
      SubscriberStubSettings subscriberStubSettings =
          SubscriberStubSettings.newBuilder()
                                .setTransportChannelProvider(
                                    SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                                          .setMaxInboundMessageSize(20 << 20) // 20MiB
                                                          .build())
                                .build();

      try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
        String projectId = "fifth-shine-238813";
        String subscriptionId = "druid";
        int numOfMessages = 10;   // max number of messages to be pulled
        String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
        PullRequest pullRequest =
            PullRequest.newBuilder()
                       .setMaxMessages(numOfMessages)
                       .setReturnImmediately(false) // return immediately if messages are not available
                       .setSubscription(subscriptionName)
                       .build();

        // use pullCallable().futureCall to asynchronously perform this operation
        PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
        List<String> ackIds = new ArrayList<>();
        for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
          ackIds.add(message.getAckId());
        }
        if (ackIds.size() > 0) {
          // acknowledge received messages
          AcknowledgeRequest acknowledgeRequest =
              AcknowledgeRequest.newBuilder()
                                .setSubscription(subscriptionName)
                                .addAllAckIds(ackIds)
                                .build();
          // use acknowledgeCallable().futureCall to asynchronously perform this operation
          subscriber.acknowledgeCallable().call(acknowledgeRequest);
        }
        return pullResponse.getReceivedMessagesList();
      }
    }
    catch (IOException e) {
      log.error("failed to get the data " + e);
    }
    return null;
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;
  }

}
