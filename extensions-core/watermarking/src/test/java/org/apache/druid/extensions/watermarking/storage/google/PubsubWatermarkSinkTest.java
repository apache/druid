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

package org.apache.druid.extensions.watermarking.storage.google;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.common.base.Preconditions;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class PubsubWatermarkSinkTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private static final String PROJECT = "someproject";
  private static Pubsub pubsubEmulator = null;

  @BeforeClass
  public static void setUp() throws IOException
  {
    if (null != System.getenv("PUBSUB_EMULATOR_HOST")) {
      final HttpTransport httpTransport = new NetHttpTransport();
      final JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
      final HttpRequestInitializer httpRequestInitializer = GoogleCredential.getApplicationDefault(
          httpTransport,
          jsonFactory
      );
      pubsubEmulator = new Pubsub.Builder(httpTransport, jsonFactory, httpRequestInitializer)
          .setRootUrl(
              StringUtils.format("http://%s/", System.getenv("PUBSUB_EMULATOR_HOST"))
          )
          .build();
    }
  }

  @Test
  public void testSimpleSend() throws Exception
  {
    Assume.assumeNotNull(pubsubEmulator, "no emulator");
    final String topicUuid = "t" + UUID.randomUUID().toString().substring(1, 15);
    final String topicName = StringUtils.format("projects/%s/topics/%s", PROJECT, topicUuid);
    final Topic topic = new Topic();
    pubsubEmulator
        .projects()
        .topics()
        .create(topicName, topic)
        .execute();
    final String subscriptionUuid = "s" + UUID.randomUUID().toString().substring(1, 15);
    final String subscriptionName = StringUtils.format("projects/%s/subscriptions/%s", PROJECT, subscriptionUuid);
    final Subscription subscription = new Subscription()
        .setRetainAckedMessages(false)
        .setTopic(topicName)
        .setAckDeadlineSeconds(10);
    pubsubEmulator
        .projects()
        .subscriptions()
        .create(subscriptionName, subscription)
        .execute();
    final PubsubWatermarkSink pubsubWatermarkSink = new PubsubWatermarkSink(new PubsubWatermarkSinkConfig()
    {
      @Override
      public String getProjectId()
      {
        return "someproject";
      }

      @Override
      public String getTopic()
      {
        return topicUuid;
      }
    }, pubsubEmulator, OBJECT_MAPPER);
    final DateTime mark = DateTimes.nowUtc();
    pubsubWatermarkSink.update("somedatasource", "sometype", mark);
    final PullRequest pullRequest = new PullRequest().setMaxMessages(10).setReturnImmediately(true);
    final PullResponse pullResponse = pubsubEmulator
        .projects()
        .subscriptions()
        .pull(subscriptionName, pullRequest)
        .execute();
    Assert.assertNotNull(pullResponse.getReceivedMessages());
    pullResponse.getReceivedMessages().forEach(m -> {
      final AcknowledgeRequest acknowledgeRequest = new AcknowledgeRequest().setAckIds(Collections.singletonList(m.getAckId()));
      try {
        pubsubEmulator.projects().subscriptions().acknowledge(subscriptionName, acknowledgeRequest).execute();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      try {
        final Map<String, Object> data = OBJECT_MAPPER.readValue(
            m.getMessage().decodeData(),
            new TypeReference<Map<String, Object>>()
            {
            }
        );
        Assert.assertEquals(
            mark,
            DateTimes.of(Preconditions.checkNotNull(data.get("timestamp"), "timestamp").toString())
        );
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    });
  }
}
