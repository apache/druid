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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.java.util.common.IAE;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;


public abstract class SeekableStreamSupervisorIOConfig
{
  private final String stream;
  @Nullable
  private final InputFormat inputFormat; // nullable for backward compatibility
  private final Integer replicas;
  private Integer taskCount;
  private final Duration taskDuration;
  private final Duration startDelay;
  private final Duration period;
  private final boolean useEarliestSequenceNumber;
  private final Duration completionTimeout;
  private final Optional<Duration> lateMessageRejectionPeriod;
  private final Optional<Duration> earlyMessageRejectionPeriod;
  private final Optional<DateTime> lateMessageRejectionStartDateTime;
  @Nullable private final AutoScalerConfig autoScalerConfig;
  @Nullable private final IdleConfig idleConfig;
  @Nullable private final Integer stopTaskCount;

  public SeekableStreamSupervisorIOConfig(
      String stream,
      @Nullable InputFormat inputFormat,
      Integer replicas,
      Integer taskCount,
      Period taskDuration,
      Period startDelay,
      Period period,
      Boolean useEarliestSequenceNumber,
      Period completionTimeout,
      Period lateMessageRejectionPeriod,
      Period earlyMessageRejectionPeriod,
      @Nullable AutoScalerConfig autoScalerConfig,
      DateTime lateMessageRejectionStartDateTime,
      @Nullable IdleConfig idleConfig,
      @Nullable Integer stopTaskCount
  )
  {
    this.stream = Preconditions.checkNotNull(stream, "stream cannot be null");
    this.inputFormat = inputFormat;
    this.replicas = replicas != null ? replicas : 1;
    // Could be null
    this.autoScalerConfig = autoScalerConfig;
    // if autoscaler is enable then taskcount will be ignored here. and init taskcount will be equal to taskCountMin
    if (autoScalerConfig != null && autoScalerConfig.getEnableTaskAutoScaler()) {
      this.taskCount = autoScalerConfig.getTaskCountMin();
    } else {
      this.taskCount = taskCount != null ? taskCount : 1;
    }
    Preconditions.checkArgument(stopTaskCount == null || stopTaskCount > 0,
                                "stopTaskCount must be greater than 0");
    this.stopTaskCount = stopTaskCount;
    this.taskDuration = defaultDuration(taskDuration, "PT1H");
    this.startDelay = defaultDuration(startDelay, "PT5S");
    this.period = defaultDuration(period, "PT30S");
    this.useEarliestSequenceNumber = useEarliestSequenceNumber != null ? useEarliestSequenceNumber : false;
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

    this.idleConfig = idleConfig;
  }

  private static Duration defaultDuration(final Period period, final String theDefault)
  {
    return (period == null ? new Period(theDefault) : period).toStandardDuration();
  }

  @JsonProperty
  public String getStream()
  {
    return stream;
  }

  @Nullable
  @JsonProperty()
  public InputFormat getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  public Integer getReplicas()
  {
    return replicas;
  }

  @Nullable
  @JsonProperty
  public AutoScalerConfig getAutoScalerConfig()
  {
    return autoScalerConfig;
  }

  @JsonProperty
  public Integer getTaskCount()
  {
    return taskCount;
  }

  public void setTaskCount(final int taskCount)
  {
    this.taskCount = taskCount;
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
  public boolean isUseEarliestSequenceNumber()
  {
    return useEarliestSequenceNumber;
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

  @Nullable
  @JsonProperty
  public IdleConfig getIdleConfig()
  {
    return idleConfig;
  }

  @Nullable
  @JsonProperty
  public Integer getStopTaskCount()
  {
    return stopTaskCount;
  }

  public int getMaxAllowedStops()
  {
    return stopTaskCount == null ? taskCount : stopTaskCount;
  }
}
