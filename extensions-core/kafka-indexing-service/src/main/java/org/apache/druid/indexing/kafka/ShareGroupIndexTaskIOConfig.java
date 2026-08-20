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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.segment.indexing.IOConfig;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * IO configuration for {@link ShareGroupIndexTask}.
 *
 * Unlike {@link KafkaIndexTaskIOConfig}, this config does not carry start/end
 * offsets because the Kafka broker manages offset tracking for share groups.
 * The task only needs the topic, group ID, consumer properties, and input format.
 */
public class ShareGroupIndexTaskIOConfig implements IOConfig
{
  private static final long DEFAULT_POLL_TIMEOUT_MILLIS = KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;

  private final String topic;
  private final String groupId;
  private final Map<String, Object> consumerProperties;
  private final InputFormat inputFormat;
  private final long pollTimeout;

  @JsonCreator
  public ShareGroupIndexTaskIOConfig(
      @JsonProperty("topic") String topic,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
      @JsonProperty("inputFormat") @Nullable InputFormat inputFormat,
      @JsonProperty("pollTimeout") @Nullable Long pollTimeout
  )
  {
    this.topic = Preconditions.checkNotNull(topic, "topic");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    this.inputFormat = inputFormat;
    this.pollTimeout = pollTimeout != null ? pollTimeout : DEFAULT_POLL_TIMEOUT_MILLIS;
  }

  @JsonProperty
  public String getTopic()
  {
    return topic;
  }

  @JsonProperty
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty
  public Map<String, Object> getConsumerProperties()
  {
    return consumerProperties;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public InputFormat getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  @Override
  public String toString()
  {
    return "ShareGroupIndexTaskIOConfig{" +
           "topic='" + topic + '\'' +
           ", groupId='" + groupId + '\'' +
           ", consumerProperties=" + consumerProperties +
           ", inputFormat=" + inputFormat +
           ", pollTimeout=" + pollTimeout +
           '}';
  }
}
