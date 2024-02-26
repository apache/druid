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

package org.apache.druid.indexing.rabbitstream.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.seekablestream.supervisor.IdleConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;

import java.util.Map;

public class RabbitStreamSupervisorIOConfig extends SeekableStreamSupervisorIOConfig
{
  public static final String DRUID_DYNAMIC_CONFIG_PROVIDER_KEY = "druid.dynamic.config.provider";
  
  public static final String USERNAME_KEY = "username";
  public static final String PASSWORD_KEY = "password";

  private final Map<String, Object> consumerProperties;

  public static final long DEFAULT_POLL_TIMEOUT_MILLIS = 100;

  private final String uri;
  private final long pollTimeout;

  @JsonCreator
  public RabbitStreamSupervisorIOConfig(
      @JsonProperty("stream") String stream,
      @JsonProperty("uri") String uri,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("replicas") Integer replicas,
      @JsonProperty("taskCount") Integer taskCount,
      @JsonProperty("taskDuration") Period taskDuration,
      @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
      @Nullable @JsonProperty("autoScalerConfig") AutoScalerConfig autoScalerConfig,
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("startDelay") Period startDelay,
      @JsonProperty("period") Period period,
      @JsonProperty("completionTimeout") Period completionTimeout,
      @JsonProperty("useEarliestOffset") Boolean useEarliestOffset,
      @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
      @JsonProperty("earlyMessageRejectionPeriod") Period earlyMessageRejectionPeriod,
      @JsonProperty("lateMessageRejectionStartDateTime") DateTime lateMessageRejectionStartDateTime,
      @JsonProperty("stopTaskCount") Integer stopTaskCount
  )
  {
    super(
        Preconditions.checkNotNull(stream, "stream"),
        inputFormat,
        replicas,
        taskCount,
        taskDuration,
        startDelay,
        period,
        useEarliestOffset,
        completionTimeout,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        autoScalerConfig,
        lateMessageRejectionStartDateTime,
        new IdleConfig(null, null),
        stopTaskCount
    );

    this.consumerProperties = consumerProperties;
    Preconditions.checkNotNull(uri, "uri");
    this.uri = uri;

    this.pollTimeout = pollTimeout != null ? pollTimeout : DEFAULT_POLL_TIMEOUT_MILLIS;
  }

  @JsonProperty
  public String getUri()
  {
    return this.uri;
  }

  @JsonProperty
  public Map<String, Object> getConsumerProperties()
  {
    return consumerProperties;
  }

  @JsonProperty
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  @JsonProperty
  public boolean isUseEarliestOffset()
  {
    return isUseEarliestSequenceNumber();
  }

  @Override
  public String toString()
  {
    return "RabbitStreamSupervisorIOConfig{" +
        "stream='" + getStream() + '\'' +
        ", replicas=" + getReplicas() +
        ", uri=" + getUri() +
        ", taskCount=" + getTaskCount() +
        ", taskDuration=" + getTaskDuration() +
        ", autoScalerConfig=" + getAutoScalerConfig() +
        ", pollTimeout=" + pollTimeout +
        ", startDelay=" + getStartDelay() +
        ", period=" + getPeriod() +
        ", useEarliestOffset=" + isUseEarliestOffset() +
        ", completionTimeout=" + getCompletionTimeout() +
        ", earlyMessageRejectionPeriod=" + getEarlyMessageRejectionPeriod() +
        ", lateMessageRejectionPeriod=" + getLateMessageRejectionPeriod() +
        ", lateMessageRejectionStartDateTime=" + getLateMessageRejectionStartDateTime() +
        ", idleConfig=" + getIdleConfig() +
        '}';
  }

}
