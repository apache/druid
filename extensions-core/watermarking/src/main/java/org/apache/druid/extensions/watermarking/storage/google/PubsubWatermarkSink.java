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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.inject.Inject;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PubsubWatermarkSink implements WatermarkSink
{
  private static final Logger LOG = new Logger(PubsubWatermarkSink.class);
  private static final String EVENT_KIND = "timelineMetadata";
  private static final String EVENT_DATASOURCE_PROPERTY = "dataSource";
  private static final String EVENT_KIND_PROPERTY = "kind";
  private static final String EVENT_TYPE_PROPERTY = "type";
  private static final String EVENT_TIME_PROPERTY = "timestamp";
  private static final String EVENT_SYS_TIME_PROPERTY = "system";
  private final PubsubWatermarkSinkConfig config;
  private final Pubsub pubsub;
  private final ObjectMapper mapper;

  @Inject
  public PubsubWatermarkSink(
      PubsubWatermarkSinkConfig config,
      Pubsub pubsub,
      ObjectMapper mapper
  )
  {
    this.pubsub = pubsub;
    this.config = config;
    this.mapper = mapper;
  }

  public PubsubWatermarkSinkConfig getConfig()
  {
    return config;
  }

  @Override
  public void update(String datasource, String type, DateTime timestamp)
  {
    try {
      final Map<String, Object> event = new HashMap<>();
      event.put(EVENT_DATASOURCE_PROPERTY, datasource);
      event.put(EVENT_KIND_PROPERTY, EVENT_KIND);
      event.put(EVENT_TYPE_PROPERTY, type);
      event.put(EVENT_TIME_PROPERTY, timestamp);
      event.put(EVENT_SYS_TIME_PROPERTY, DateTimes.nowUtc());

      final PubsubMessage pubsubMessage = new PubsubMessage().encodeData(mapper.writeValueAsBytes(event));

      final PublishResponse response = pubsub
          .projects()
          .topics()
          .publish(
              config.getFormattedTopic(),
              new PublishRequest().setMessages(Collections.singletonList(pubsubMessage))
          )
          .execute();
      LOG.debug(
          "Published to %s with message ID: %s",
          config.getFormattedTopic(),
          Arrays.toString(response.getMessageIds().toArray())
      );
    }
    catch (Exception ex) {
      LOG.error(ex, "Publish to %s failed", config.getTopic());
    }
  }

  @Override
  public void rollback(String datasource, String type, DateTime timestamp)
  {
  }

  @Override
  public void purgeHistory(String datasource, String type, DateTime timestamp)
  {
  }

  @Override
  public void initialize()
  {

  }
}
