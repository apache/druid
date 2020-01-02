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

package org.apache.druid.indexing.pubsub.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.IAE;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;

public class PubsubSupervisorIOConfig
{
  public static final String SUBSCRIPTION = "subscription";
  public static final long DEFAULT_POLL_TIMEOUT_MILLIS = 100;

  private final long pollTimeout;

  private final String subscription;
  @Nullable
  private final InputFormat inputFormat; // nullable for backward compatibility
  private final Integer replicas;
  private final Integer taskCount;
  private final Duration taskDuration;
  private final Duration startDelay;
  private final Duration period;
  private final Duration completionTimeout;
  private final Optional<Duration> lateMessageRejectionPeriod;
  private final Optional<Duration> earlyMessageRejectionPeriod;
  private final Optional<DateTime> lateMessageRejectionStartDateTime;

  @JsonCreator
  public PubsubSupervisorIOConfig(
      @JsonProperty("subscription") String subscription,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("replicas") Integer replicas,
      @JsonProperty("taskCount") Integer taskCount,
      @JsonProperty("taskDuration") Period taskDuration,
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("startDelay") Period startDelay,
      @JsonProperty("period") Period period,
      @JsonProperty("completionTimeout") Period completionTimeout,
      @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
      @JsonProperty("earlyMessageRejectionPeriod") Period earlyMessageRejectionPeriod,
      @JsonProperty("lateMessageRejectionStartDateTime") DateTime lateMessageRejectionStartDateTime
  )
  {
    Preconditions.checkNotNull(subscription, "subscription");
    this.subscription = Preconditions.checkNotNull(subscription, "subscription cannot be null");
    this.inputFormat = inputFormat;
    this.replicas = replicas != null ? replicas : 1;
    this.taskCount = taskCount != null ? taskCount : 1;
    this.taskDuration = defaultDuration(taskDuration, "PT1H");
    this.startDelay = defaultDuration(startDelay, "PT5S");
    this.period = defaultDuration(period, "PT30S");
    this.completionTimeout = defaultDuration(completionTimeout, "PT30M");
    this.lateMessageRejectionPeriod = lateMessageRejectionPeriod == null
                                      ? Optional.absent()
                                      : Optional.of(lateMessageRejectionPeriod.toStandardDuration());
    this.lateMessageRejectionStartDateTime = lateMessageRejectionStartDateTime == null
                                             ? Optional.absent()
                                             : Optional.of(lateMessageRejectionStartDateTime);
    this.earlyMessageRejectionPeriod = earlyMessageRejectionPeriod == null
                                       ? Optional.absent()
                                       : Optional.of(earlyMessageRejectionPeriod.toStandardDuration());

    if (this.lateMessageRejectionPeriod.isPresent()
        && this.lateMessageRejectionStartDateTime.isPresent()) {
      throw new IAE("SeekableStreamSupervisorIOConfig does not support "
                    + "both properties lateMessageRejectionStartDateTime "
                    + "and lateMessageRejectionPeriod.");
    }

    this.pollTimeout = pollTimeout != null ? pollTimeout : DEFAULT_POLL_TIMEOUT_MILLIS;
  }


  private static Duration defaultDuration(final Period period, final String theDefault)
  {
    return (period == null ? new Period(theDefault) : period).toStandardDuration();
  }

  @Nullable
  @JsonProperty
  private InputFormat getGivenInputFormat()
  {
    return inputFormat;
  }

  @Nullable
  public InputFormat getInputFormat(@Nullable ParseSpec parseSpec)
  {
    if (inputFormat == null) {
      return Preconditions.checkNotNull(parseSpec, "parseSpec").toInputFormat();
    } else {
      return inputFormat;
    }
  }

  @JsonProperty
  public Integer getReplicas()
  {
    return replicas;
  }

  @JsonProperty
  public Integer getTaskCount()
  {
    return taskCount;
  }

  @JsonProperty
  public Duration getTaskDuration()
  {
    return taskDuration;
  }

  @JsonProperty
  public Duration getStartDelay()
  {
    return startDelay;
  }

  @JsonProperty
  public Duration getPeriod()
  {
    return period;
  }

  @JsonProperty
  public Duration getCompletionTimeout()
  {
    return completionTimeout;
  }

  @JsonProperty
  public Optional<Duration> getEarlyMessageRejectionPeriod()
  {
    return earlyMessageRejectionPeriod;
  }

  @JsonProperty
  public Optional<Duration> getLateMessageRejectionPeriod()
  {
    return lateMessageRejectionPeriod;
  }

  @JsonProperty
  public Optional<DateTime> getLateMessageRejectionStartDateTime()
  {
    return lateMessageRejectionStartDateTime;
  }

  @JsonProperty
  public String getSubscription()
  {
    return subscription;
  }

  @JsonProperty
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  @Override
  public String toString()
  {
    return "PubsubSupervisorIOConfig{" +
           "subscription='" + getSubscription() + '\'' +
           ", replicas=" + getReplicas() +
           ", taskCount=" + getTaskCount() +
           ", taskDuration=" + getTaskDuration() +
           ", pollTimeout=" + pollTimeout +
           ", startDelay=" + getStartDelay() +
           ", period=" + getPeriod() +
           ", completionTimeout=" + getCompletionTimeout() +
           ", earlyMessageRejectionPeriod=" + getEarlyMessageRejectionPeriod() +
           ", lateMessageRejectionPeriod=" + getLateMessageRejectionPeriod() +
           ", lateMessageRejectionStartDateTime=" + getLateMessageRejectionStartDateTime() +
           '}';
  }

}
