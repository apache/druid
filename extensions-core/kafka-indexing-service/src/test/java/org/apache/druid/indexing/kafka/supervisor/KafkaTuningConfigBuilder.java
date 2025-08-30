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

package org.apache.druid.indexing.kafka.supervisor;

import org.apache.druid.indexing.common.task.TuningConfigBuilder;
import org.joda.time.Period;

/**
 * Builder for {@link KafkaSupervisorTuningConfig} used in tests.
 */
public class KafkaTuningConfigBuilder extends TuningConfigBuilder<KafkaTuningConfigBuilder, KafkaSupervisorTuningConfig>
{
  private Period intermediatePersistPeriod;
  private Long handoffConditionTimeout;
  private Boolean resetOffsetAutomatically;
  private Integer workerThreads;
  private Period shutdownTimeout;
  private Period offsetFetchPeriod;
  private Period intermediateHandoffPeriod;

  public KafkaTuningConfigBuilder withIntermediatePersistPeriod(Period intermediatePersistPeriod)
  {
    this.intermediatePersistPeriod = intermediatePersistPeriod;
    return this;
  }

  public KafkaTuningConfigBuilder withHandoffConditionTimeout(Long handoffConditionTimeout)
  {
    this.handoffConditionTimeout = handoffConditionTimeout;
    return this;
  }

  public KafkaTuningConfigBuilder withResetOffsetAutomatically(Boolean resetOffsetAutomatically)
  {
    this.resetOffsetAutomatically = resetOffsetAutomatically;
    return this;
  }

  public KafkaTuningConfigBuilder withWorkerThreads(Integer workerThreads)
  {
    this.workerThreads = workerThreads;
    return this;
  }

  public KafkaTuningConfigBuilder withShutdownTimeout(Period shutdownTimeout)
  {
    this.shutdownTimeout = shutdownTimeout;
    return this;
  }

  public KafkaTuningConfigBuilder withOffsetFetchPeriod(Period offsetFetchPeriod)
  {
    this.offsetFetchPeriod = offsetFetchPeriod;
    return this;
  }

  public KafkaTuningConfigBuilder withIntermediateHandoffPeriod(Period intermediateHandoffPeriod)
  {
    this.intermediateHandoffPeriod = intermediateHandoffPeriod;
    return this;
  }

  @Override
  public KafkaSupervisorTuningConfig build()
  {
    return new KafkaSupervisorTuningConfig(
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        skipBytesInMemoryOverheadCheck,
        maxRowsPerSegment,
        maxTotalRows,
        intermediatePersistPeriod,
        maxPendingPersists,
        indexSpec,
        indexSpecForIntermediatePersists,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        segmentWriteOutMediumFactory,
        workerThreads,
        chatHandlerNumRetries == null ? null : (long) chatHandlerNumRetries,
        chatHandlerTimeout == null ? null : new Period(chatHandlerTimeout.getMillis()),
        shutdownTimeout,
        offsetFetchPeriod,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        numPersistThreads,
        maxColumnsToMerge
    );
  }
}