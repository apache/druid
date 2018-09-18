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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

public class KafkaTuningConfig implements TuningConfig, AppenderatorConfig
{
  private static final int DEFAULT_MAX_ROWS_PER_SEGMENT = 5_000_000;
  private static final boolean DEFAULT_RESET_OFFSET_AUTOMATICALLY = false;

  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
  private final int maxRowsPerSegment;
  @Nullable
  private final Long maxTotalRows;
  private final Period intermediatePersistPeriod;
  private final File basePersistDirectory;
  @Deprecated
  private final int maxPendingPersists;
  private final IndexSpec indexSpec;
  private final boolean reportParseExceptions;
  private final long handoffConditionTimeout;
  private final boolean resetOffsetAutomatically;
  @Nullable
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;
  private final Period intermediateHandoffPeriod;

  private final boolean logParseExceptions;
  private final int maxParseExceptions;
  private final int maxSavedParseExceptions;
  private final int numFilesPerMerge;

  @JsonCreator
  public KafkaTuningConfig(
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
      @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
      @JsonProperty("intermediatePersistPeriod") @Nullable Period intermediatePersistPeriod,
      @JsonProperty("basePersistDirectory") @Nullable File basePersistDirectory,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      // This parameter is left for compatibility when reading existing configs, to be removed in Druid 0.12.
      @JsonProperty("buildV9Directly") @Nullable Boolean buildV9Directly,
      @Deprecated @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") @Nullable Long handoffConditionTimeout,
      @JsonProperty("resetOffsetAutomatically") @Nullable Boolean resetOffsetAutomatically,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("intermediateHandoffPeriod") @Nullable Period intermediateHandoffPeriod,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
      @JsonProperty("numFilesPerMerge") @Nullable Integer numFilesPerMerge
  )
  {
    // Cannot be a static because default basePersistDirectory is unique per-instance
    final RealtimeTuningConfig defaults = RealtimeTuningConfig.makeDefaultTuningConfig(basePersistDirectory);

    this.maxRowsInMemory = maxRowsInMemory == null ? defaults.getMaxRowsInMemory() : maxRowsInMemory;
    this.maxRowsPerSegment = maxRowsPerSegment == null ? DEFAULT_MAX_ROWS_PER_SEGMENT : maxRowsPerSegment;
    // initializing this to 0, it will be lazily initialized to a value
    // @see server.src.main.java.org.apache.druid.segment.indexing.TuningConfigs#getMaxBytesInMemoryOrDefault(long)
    this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
    this.maxTotalRows = maxTotalRows;
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? defaults.getIntermediatePersistPeriod()
                                     : intermediatePersistPeriod;
    this.basePersistDirectory = defaults.getBasePersistDirectory();
    this.maxPendingPersists = 0;
    this.indexSpec = indexSpec == null ? defaults.getIndexSpec() : indexSpec;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? defaults.isReportParseExceptions()
                                 : reportParseExceptions;
    this.handoffConditionTimeout = handoffConditionTimeout == null
                                   ? defaults.getHandoffConditionTimeout()
                                   : handoffConditionTimeout;
    this.resetOffsetAutomatically = resetOffsetAutomatically == null
                                    ? DEFAULT_RESET_OFFSET_AUTOMATICALLY
                                    : resetOffsetAutomatically;
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
    this.intermediateHandoffPeriod = intermediateHandoffPeriod == null
                                     ? new Period().withDays(Integer.MAX_VALUE)
                                     : intermediateHandoffPeriod;

    if (this.reportParseExceptions) {
      this.maxParseExceptions = 0;
      this.maxSavedParseExceptions = maxSavedParseExceptions == null ? 0 : Math.min(1, maxSavedParseExceptions);
    } else {
      this.maxParseExceptions = maxParseExceptions == null
                                ? TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS
                                : maxParseExceptions;
      this.maxSavedParseExceptions = maxSavedParseExceptions == null
                                     ? TuningConfig.DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS
                                     : maxSavedParseExceptions;
    }
    this.logParseExceptions = logParseExceptions == null
                              ? TuningConfig.DEFAULT_LOG_PARSE_EXCEPTIONS
                              : logParseExceptions;
    this.numFilesPerMerge = TuningConfig.validateAndGetNumFilesPerMerge(numFilesPerMerge);
  }

  public static KafkaTuningConfig copyOf(KafkaTuningConfig config)
  {
    return new KafkaTuningConfig(
        config.maxRowsInMemory,
        config.maxBytesInMemory,
        config.maxRowsPerSegment,
        config.maxTotalRows,
        config.intermediatePersistPeriod,
        config.basePersistDirectory,
        config.maxPendingPersists,
        config.indexSpec,
        true,
        config.reportParseExceptions,
        config.handoffConditionTimeout,
        config.resetOffsetAutomatically,
        config.segmentWriteOutMediumFactory,
        config.intermediateHandoffPeriod,
        config.logParseExceptions,
        config.maxParseExceptions,
        config.maxSavedParseExceptions,
        config.numFilesPerMerge
    );
  }

  @Override
  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @Override
  @JsonProperty
  public long getMaxBytesInMemory()
  {
    return maxBytesInMemory;
  }

  @Override
  @JsonProperty
  public int getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
  }


  @JsonProperty
  @Override
  @Nullable
  public Long getMaxTotalRows()
  {
    return maxTotalRows;
  }

  @Override
  @JsonProperty
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }

  @Override
  @JsonProperty
  public File getBasePersistDirectory()
  {
    return basePersistDirectory;
  }

  @Override
  @JsonProperty
  @Deprecated
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  @Override
  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  /**
   * Always returns true, doesn't affect the version being built.
   */
  @Deprecated
  @JsonProperty
  public boolean getBuildV9Directly()
  {
    return true;
  }

  @Override
  @JsonProperty
  public boolean isReportParseExceptions()
  {
    return reportParseExceptions;
  }

  @JsonProperty
  public long getHandoffConditionTimeout()
  {
    return handoffConditionTimeout;
  }

  @JsonProperty
  public boolean isResetOffsetAutomatically()
  {
    return resetOffsetAutomatically;
  }

  @Override
  @JsonProperty
  @Nullable
  public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
  {
    return segmentWriteOutMediumFactory;
  }

  @Override
  public int getNumFilesPerMerge()
  {
    return numFilesPerMerge;
  }

  @JsonProperty
  public Period getIntermediateHandoffPeriod()
  {
    return intermediateHandoffPeriod;
  }

  @JsonProperty
  public boolean isLogParseExceptions()
  {
    return logParseExceptions;
  }

  @JsonProperty
  public int getMaxParseExceptions()
  {
    return maxParseExceptions;
  }

  @JsonProperty
  public int getMaxSavedParseExceptions()
  {
    return maxSavedParseExceptions;
  }

  public KafkaTuningConfig withBasePersistDirectory(File dir)
  {
    return new KafkaTuningConfig(
        maxRowsInMemory,
        maxBytesInMemory,
        maxRowsPerSegment,
        maxTotalRows,
        intermediatePersistPeriod,
        dir,
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
        maxSavedParseExceptions,
        numFilesPerMerge
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KafkaTuningConfig that = (KafkaTuningConfig) o;
    return maxRowsInMemory == that.maxRowsInMemory &&
           maxRowsPerSegment == that.maxRowsPerSegment &&
           maxBytesInMemory == that.maxBytesInMemory &&
           Objects.equals(maxTotalRows, that.maxTotalRows) &&
           maxPendingPersists == that.maxPendingPersists &&
           reportParseExceptions == that.reportParseExceptions &&
           handoffConditionTimeout == that.handoffConditionTimeout &&
           resetOffsetAutomatically == that.resetOffsetAutomatically &&
           Objects.equals(intermediatePersistPeriod, that.intermediatePersistPeriod) &&
           Objects.equals(basePersistDirectory, that.basePersistDirectory) &&
           Objects.equals(indexSpec, that.indexSpec) &&
           Objects.equals(segmentWriteOutMediumFactory, that.segmentWriteOutMediumFactory) &&
           Objects.equals(intermediateHandoffPeriod, that.intermediateHandoffPeriod) &&
           logParseExceptions == that.logParseExceptions &&
           maxParseExceptions == that.maxParseExceptions &&
           maxSavedParseExceptions == that.maxSavedParseExceptions &&
           numFilesPerMerge == that.numFilesPerMerge;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        maxRowsInMemory,
        maxRowsPerSegment,
        maxBytesInMemory,
        maxTotalRows,
        intermediatePersistPeriod,
        basePersistDirectory,
        maxPendingPersists,
        indexSpec,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        segmentWriteOutMediumFactory,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        numFilesPerMerge
    );
  }

  @Override
  public String toString()
  {
    return "KafkaTuningConfig{" +
           "maxRowsInMemory=" + maxRowsInMemory +
           ", maxRowsPerSegment=" + maxRowsPerSegment +
           ", maxTotalRows=" + maxTotalRows +
           ", maxBytesInMemory=" + maxBytesInMemory +
           ", intermediatePersistPeriod=" + intermediatePersistPeriod +
           ", basePersistDirectory=" + basePersistDirectory +
           ", maxPendingPersists=" + maxPendingPersists +
           ", indexSpec=" + indexSpec +
           ", reportParseExceptions=" + reportParseExceptions +
           ", handoffConditionTimeout=" + handoffConditionTimeout +
           ", resetOffsetAutomatically=" + resetOffsetAutomatically +
           ", segmentWriteOutMediumFactory=" + segmentWriteOutMediumFactory +
           ", intermediateHandoffPeriod=" + intermediateHandoffPeriod +
           ", logParseExceptions=" + logParseExceptions +
           ", maxParseExceptions=" + maxParseExceptions +
           ", maxSavedParseExceptions=" + maxSavedParseExceptions +
           ", numFilesPerMerge=" + numFilesPerMerge +
           '}';
  }

  public static class Builder
  {
    private Integer maxRowsInMemory;
    private Long maxBytesInMemory;
    private Integer maxRowsPerSegment;
    private Long maxTotalRows;
    private Period intermediatePersistPeriod;
    private File basePersistDirectory;
    private Integer maxPendingPersists;
    private IndexSpec indexSpec;
    private Boolean reportParseExceptions;
    private Long handoffConditionTimeout;
    private Boolean resetOffsetAutomatically;
    private SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;
    private Period intermediateHandoffPeriod;
    private Boolean logParseExceptions;
    private Integer maxParseExceptions;
    private Integer maxSavedParseExceptions;
    private Integer numFilesPerMerge;

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

    public Builder setMaxRowsPerSegment(@Nullable Integer maxRowsPerSegment)
    {
      if (maxRowsPerSegment != null) {
        this.maxRowsPerSegment = maxRowsPerSegment;
      }
      return this;
    }

    public Builder setMaxTotalRows(@Nullable Long maxTotalRows)
    {
      if (maxTotalRows != null) {
        this.maxTotalRows = maxTotalRows;
      }
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

    public Builder setIntermediateHandoffPeriod(@Nullable Period intermediateHandoffPeriod)
    {
      if (intermediateHandoffPeriod != null) {
        this.intermediateHandoffPeriod = intermediateHandoffPeriod;
      }
      return this;
    }

    public Builder setLogParseExceptions(boolean logParseExceptions)
    {
      this.logParseExceptions = logParseExceptions;
      return this;
    }

    public Builder setMaxParseExceptions(@Nullable Integer maxParseExceptions)
    {
      if (maxParseExceptions != null) {
        this.maxParseExceptions = maxParseExceptions;
      }
      return this;
    }

    public Builder setMaxSavedParseExceptions(@Nullable Integer maxSavedParseExceptions)
    {
      if (maxSavedParseExceptions != null) {
        this.maxSavedParseExceptions = maxSavedParseExceptions;
      }
      return this;
    }

    public Builder setNumFilesPerMerge(int numFilesPerMerge)
    {
      this.numFilesPerMerge = numFilesPerMerge;
      return this;
    }

    public KafkaTuningConfig build()
    {
      return new KafkaTuningConfig(
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
          maxSavedParseExceptions,
          numFilesPerMerge
      );
    }
  }
}
