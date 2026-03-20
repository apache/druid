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

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.joda.time.DateTime;
import org.joda.time.Period;

/**
 * Builder for a {@link SeekableStreamSupervisorIOConfig}.
 *
 * @param <Self> Type of this builder itself
 * @param <C>    Type of the IOConfig created by this builder
 */
public abstract class SupervisorIOConfigBuilder<
    Self extends SupervisorIOConfigBuilder<Self, C>,
    C extends SeekableStreamSupervisorIOConfig>
{
  protected String stream;
  protected InputFormat inputFormat;
  protected Integer replicas;
  protected Integer taskCount;
  protected Period taskDuration;
  protected Period startDelay;
  protected Period period;
  protected Boolean useEarliestSequenceNumber;
  protected Period completionTimeout;
  protected Period lateMessageRejectionPeriod;
  protected Period earlyMessageRejectionPeriod;
  protected AutoScalerConfig autoScalerConfig;
  protected LagAggregator lagAggregator;
  protected DateTime lateMessageRejectionStartDateTime;
  protected IdleConfig idleConfig;
  protected Integer stopTaskCount;

  public Self withStream(String stream)
  {
    this.stream = stream;
    return self();
  }

  public Self withInputFormat(InputFormat inputFormat)
  {
    this.inputFormat = inputFormat;
    return self();
  }

  public Self withJsonInputFormat()
  {
    this.inputFormat = new JsonInputFormat(null, null, null, null, null);
    return self();
  }

  public Self withReplicas(Integer replicas)
  {
    this.replicas = replicas;
    return self();
  }

  public Self withTaskCount(Integer taskCount)
  {
    this.taskCount = taskCount;
    return self();
  }

  public Self withTaskDuration(Period taskDuration)
  {
    this.taskDuration = taskDuration;
    return self();
  }

  public Self withStartDelay(Period startDelay)
  {
    this.startDelay = startDelay;
    return self();
  }

  public Self withSupervisorRunPeriod(Period period)
  {
    this.period = period;
    return self();
  }

  public Self withUseEarliestSequenceNumber(Boolean useEarliestSequenceNumber)
  {
    this.useEarliestSequenceNumber = useEarliestSequenceNumber;
    return self();
  }

  public Self withCompletionTimeout(Period completionTimeout)
  {
    this.completionTimeout = completionTimeout;
    return self();
  }

  public Self withLateMessageRejectionPeriod(Period lateMessageRejectionPeriod)
  {
    this.lateMessageRejectionPeriod = lateMessageRejectionPeriod;
    return self();
  }

  public Self withEarlyMessageRejectionPeriod(Period earlyMessageRejectionPeriod)
  {
    this.earlyMessageRejectionPeriod = earlyMessageRejectionPeriod;
    return self();
  }

  public Self withAutoScalerConfig(AutoScalerConfig autoScalerConfig)
  {
    this.autoScalerConfig = autoScalerConfig;
    return self();
  }

  public Self withLagAggregator(LagAggregator lagAggregator)
  {
    this.lagAggregator = lagAggregator;
    return self();
  }

  public Self withLateMessageRejectionStartDateTime(DateTime lateMessageRejectionStartDateTime)
  {
    this.lateMessageRejectionStartDateTime = lateMessageRejectionStartDateTime;
    return self();
  }

  public Self withIdleConfig(IdleConfig idleConfig)
  {
    this.idleConfig = idleConfig;
    return self();
  }

  public Self withStopTaskCount(Integer stopTaskCount)
  {
    this.stopTaskCount = stopTaskCount;
    return self();
  }

  public abstract C build();

  @SuppressWarnings("unchecked")
  private Self self()
  {
    return (Self) this;
  }
}
