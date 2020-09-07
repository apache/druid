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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.indexing.pubsub.supervisor.PubsubSupervisorIOConfig;
import org.apache.druid.segment.indexing.IOConfig;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

public class PubsubIndexTaskIOConfig implements IOConfig
{
  @Nullable
  private final String projectId;
  private final String subscription;
  private final Integer taskGroupId;
  private final long pollTimeout;
  private final DateTime minimumMessageTime;
  private final DateTime maximumMessageTime;
  private final InputFormat inputFormat;

  @JsonCreator
  public PubsubIndexTaskIOConfig(
      @JsonProperty("projectId") String projectId,
      @JsonProperty("subscription") String subscription,
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // can be null for backward compabitility
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
      @JsonProperty("inputFormat") @Nullable InputFormat inputFormat
  )
  {
    this.projectId = projectId;
    this.subscription = subscription;
    this.taskGroupId = taskGroupId;
    this.pollTimeout = pollTimeout != null ? pollTimeout : PubsubSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;
    this.minimumMessageTime = minimumMessageTime;
    this.maximumMessageTime = maximumMessageTime;
    this.inputFormat = inputFormat;
  }

  @JsonProperty
  public String getProjectId()
  {
    return projectId;
  }

  @JsonProperty
  public String getSubscription()
  {
    return subscription;
  }

  @Nullable
  @JsonProperty
  public Integer getTaskGroupId()
  {
    return taskGroupId;
  }

  @JsonProperty
  public Optional<DateTime> getMaximumMessageTime()
  {
    return Optional.of(maximumMessageTime);
  }

  @JsonProperty
  public Optional<DateTime> getMinimumMessageTime()
  {
    return Optional.of(minimumMessageTime);
  }

  @Nullable
  @JsonProperty("inputFormat")
  private InputFormat getGivenInputFormat()
  {
    return inputFormat;
  }

  @Nullable
  @JsonProperty("pollTimeout")
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  public InputFormat getInputFormat(ParseSpec parseSpec)
  {
    return inputFormat;
  }

  @Override
  public String toString()
  {
    return "PubsubIndexTaskIOConfig{" +
           "projectId=" + getProjectId() +
           ", subscription=" + getSubscription() +
           ", pollTimeout=" + getPollTimeout() +
           ", minimumMessageTime=" + getMinimumMessageTime() +
           ", maximumMessageTime=" + getMaximumMessageTime() +
           '}';
  }
}
