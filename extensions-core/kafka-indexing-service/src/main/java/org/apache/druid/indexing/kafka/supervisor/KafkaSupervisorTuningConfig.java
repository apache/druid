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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.kafka.KafkaTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.TuningConfigs;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;

public class KafkaSupervisorTuningConfig extends KafkaTuningConfig
{
  private final Integer workerThreads;
  private final Integer chatThreads;
  private final Long chatRetries;
  private final Duration httpTimeout;
  private final Duration shutdownTimeout;
  private final Duration offsetFetchPeriod;

  public KafkaSupervisorTuningConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") Long maxBytesInMemory,
      @JsonProperty("maxRowsPerSegment") Integer maxRowsPerSegment,
      @JsonProperty("maxTotalRows") Long maxTotalRows,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      // This parameter is left for compatibility when reading existing configs, to be removed in Druid 0.12.
      @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") Long handoffConditionTimeout,
      @JsonProperty("resetOffsetAutomatically") Boolean resetOffsetAutomatically,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("workerThreads") Integer workerThreads,
      @JsonProperty("chatThreads") Integer chatThreads,
      @JsonProperty("chatRetries") Long chatRetries,
      @JsonProperty("httpTimeout") Period httpTimeout,
      @JsonProperty("shutdownTimeout") Period shutdownTimeout,
      @JsonProperty("offsetFetchPeriod") Period offsetFetchPeriod,
      @JsonProperty("intermediateHandoffPeriod") Period intermediateHandoffPeriod,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions
  )
  {
    super(
        maxRowsInMemory,
        maxBytesInMemory,
        maxRowsPerSegment,
        maxTotalRows,
        intermediatePersistPeriod,
        basePersistDirectory,
        maxPendingPersists,
        indexSpec,
        true,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        segmentWriteOutMediumFactory,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
    );

    this.workerThreads = workerThreads;
    this.chatThreads = chatThreads;
    this.chatRetries = (chatRetries != null ? chatRetries : 8);
    this.httpTimeout = defaultDuration(httpTimeout, "PT10S");
    this.shutdownTimeout = defaultDuration(shutdownTimeout, "PT80S");
    this.offsetFetchPeriod = defaultDuration(offsetFetchPeriod, "PT30S");
  }

  @JsonProperty
  public Integer getWorkerThreads()
  {
    return workerThreads;
  }

  @JsonProperty
  public Integer getChatThreads()
  {
    return chatThreads;
  }

  @JsonProperty
  public Long getChatRetries()
  {
    return chatRetries;
  }

  @JsonProperty
  public Duration getHttpTimeout()
  {
    return httpTimeout;
  }

  @JsonProperty
  public Duration getShutdownTimeout()
  {
    return shutdownTimeout;
  }

  @JsonProperty
  public Duration getOffsetFetchPeriod()
  {
    return offsetFetchPeriod;
  }

  @Override
  public String toString()
  {
    return "KafkaSupervisorTuningConfig{" +
           "maxRowsInMemory=" + getMaxRowsInMemory() +
           ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
           ", maxTotalRows=" + getMaxTotalRows() +
           ", maxBytesInMemory=" + TuningConfigs.getMaxBytesInMemoryOrDefault(getMaxBytesInMemory()) +
           ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
           ", basePersistDirectory=" + getBasePersistDirectory() +
           ", maxPendingPersists=" + getMaxPendingPersists() +
           ", indexSpec=" + getIndexSpec() +
           ", reportParseExceptions=" + isReportParseExceptions() +
           ", handoffConditionTimeout=" + getHandoffConditionTimeout() +
           ", resetOffsetAutomatically=" + isResetOffsetAutomatically() +
           ", segmentWriteOutMediumFactory=" + getSegmentWriteOutMediumFactory() +
           ", workerThreads=" + workerThreads +
           ", chatThreads=" + chatThreads +
           ", chatRetries=" + chatRetries +
           ", httpTimeout=" + httpTimeout +
           ", shutdownTimeout=" + shutdownTimeout +
           ", offsetFetchPeriod=" + offsetFetchPeriod +
           ", intermediateHandoffPeriod=" + getIntermediateHandoffPeriod() +
           ", logParseExceptions=" + isLogParseExceptions() +
           ", maxParseExceptions=" + getMaxParseExceptions() +
           ", maxSavedParseExceptions=" + getMaxSavedParseExceptions() +
           '}';
  }

  private static Duration defaultDuration(final Period period, final String theDefault)
  {
    return (period == null ? new Period(theDefault) : period).toStandardDuration();
  }

  public static class Builder
  {
    private Integer maxRowsInMemory;
    private Long maxBytesInMemory;
    private Integer maxRowsPerSegment;
    private Long maxTotalRows;
    private Period intermediatePersistPeriod;
    private File basePersistDirectory;
    private int maxPendingPersists;
    private IndexSpec indexSpec;
    private Boolean reportParseExceptions;
    private Long handoffConditionTimeout;
    private Boolean resetOffsetAutomatically;
    private SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;
    private Period intermediateHandoffPeriod;
    private Boolean logParseExceptions;
    private Integer maxParseExceptions;
    private Integer maxSavedParseExceptions;
    private Integer workerThreads;
    private Integer chatThreads;
    private Long chatRetries;
    private Period httpTimeout;
    private Period shutdownTimeout;
    private Period offsetFetchPeriod;

    public Builder setMaxRowsInMemory(int maxRowsInMemory)
    {
      this.maxRowsInMemory = maxRowsInMemory;
      return this;
    }

    public Builder setMaxBytesInMemory(long maxBytesInMemory)
    {
      this.maxBytesInMemory = maxBytesInMemory;
      return this;
    }

    public Builder setMaxRowsPerSegment(int maxRowsPerSegment)
    {
      this.maxRowsPerSegment = maxRowsPerSegment;
      return this;
    }

    public Builder setMaxTotalRows(long maxTotalRows)
    {
      this.maxTotalRows = maxTotalRows;
      return this;
    }

    public Builder setIntermediatePersistePeriod(Period intermediatePersistePeriod)
    {
      this.intermediatePersistPeriod = intermediatePersistePeriod;
      return this;
    }

    public Builder setBasePersistDirectory(File basePersistDirectory)
    {
      this.basePersistDirectory = basePersistDirectory;
      return this;
    }

    @Deprecated
    public Builder setMaxPendingPersists(int maxPendingPersists)
    {
      this.maxPendingPersists = maxPendingPersists;
      return this;
    }

    public Builder setIndexSpec(IndexSpec indexSpec)
    {
      this.indexSpec = indexSpec;
      return this;
    }

    public Builder setReportParseExceptions(boolean reportParseExceptions)
    {
      this.reportParseExceptions = reportParseExceptions;
      return this;
    }

    public Builder setHandoffConditionTimeout(long handoffConditionTimeout)
    {
      this.handoffConditionTimeout = handoffConditionTimeout;
      return this;
    }

    public Builder setResetOffsetAutomatically(boolean resetOffsetAutomatically)
    {
      this.resetOffsetAutomatically = resetOffsetAutomatically;
      return this;
    }

    public Builder setSegmentWriteOutMediumFactory(SegmentWriteOutMediumFactory factory)
    {
      this.segmentWriteOutMediumFactory = factory;
      return this;
    }

    public Builder setIntermediateHandoffPeriod(Period intermediateHandoffPeriod)
    {
      this.intermediateHandoffPeriod = intermediateHandoffPeriod;
      return this;
    }

    public Builder setLogParseExceptions(boolean logParseExceptions)
    {
      this.logParseExceptions = logParseExceptions;
      return this;
    }

    public Builder setMaxParseExceptions(int maxParseExceptions)
    {
      this.maxParseExceptions = maxParseExceptions;
      return this;
    }

    public Builder setMaxSavedParseExceptions(int maxSavedParseExceptions)
    {
      this.maxSavedParseExceptions = maxSavedParseExceptions;
      return this;
    }

    public Builder setWorkerThreads(int workerThreads)
    {
      this.workerThreads = workerThreads;
      return this;
    }

    public Builder setChatThreads(int chatThreads)
    {
      this.chatThreads = chatThreads;
      return this;
    }

    public Builder setChatRetries(long chatRetries)
    {
      this.chatRetries = chatRetries;
      return this;
    }

    public Builder setHttpTimeout(Period httpTimeout)
    {
      this.httpTimeout = httpTimeout;
      return this;
    }

    public Builder setShutdownTimeout(Period shutdownTimeout)
    {
      this.shutdownTimeout = shutdownTimeout;
      return this;
    }

    public Builder setOffsetFetchPeriod(Period offsetFetchPeriod)
    {
      this.offsetFetchPeriod = offsetFetchPeriod;
      return this;
    }

    public KafkaSupervisorTuningConfig build()
    {
      return new KafkaSupervisorTuningConfig(
          maxRowsInMemory,
          maxBytesInMemory,
          maxRowsPerSegment,
          maxTotalRows,
          intermediatePersistPeriod,
          basePersistDirectory,
          maxPendingPersists,
          indexSpec,
          true,
          reportParseExceptions,
          handoffConditionTimeout,
          resetOffsetAutomatically,
          segmentWriteOutMediumFactory,
          workerThreads,
          chatThreads,
          chatRetries,
          httpTimeout,
          shutdownTimeout,
          offsetFetchPeriod,
          intermediateHandoffPeriod,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions
      );
    }
  }
}
