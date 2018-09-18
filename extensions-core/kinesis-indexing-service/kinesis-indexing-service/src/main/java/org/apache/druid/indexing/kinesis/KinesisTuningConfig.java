/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

public class KinesisTuningConfig implements TuningConfig, AppenderatorConfig
{
  private static final int DEFAULT_MAX_ROWS_PER_SEGMENT = 5_000_000;
  private static final boolean DEFAULT_RESET_OFFSET_AUTOMATICALLY = false;
  private static final boolean DEFAULT_SKIP_SEQUENCE_NUMBER_AVAILABILITY_CHECK = false;
  private static final int DEFAULT_RECORD_BUFFER_SIZE = 10000;
  private static final int DEFAULT_RECORD_BUFFER_OFFER_TIMEOUT = 5000;
  private static final int DEFAULT_RECORD_BUFFER_FULL_WAIT = 5000;
  private static final int DEFAULT_FETCH_SEQUENCE_NUMBER_TIMEOUT = 60000;

  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
  private final int maxRowsPerSegment;
  private final Period intermediatePersistPeriod;
  private final File basePersistDirectory;
  private final int maxPendingPersists;
  private final IndexSpec indexSpec;
  private final boolean buildV9Directly;
  private final boolean reportParseExceptions;
  private final long handoffConditionTimeout;
  private final boolean resetOffsetAutomatically;
  private final boolean skipSequenceNumberAvailabilityCheck;
  private final int recordBufferSize;
  private final int recordBufferOfferTimeout;
  private final int recordBufferFullWait;
  private final int fetchSequenceNumberTimeout;
  private final Integer fetchThreads;
  @Nullable
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

  private final boolean logParseExceptions;
  private final int maxParseExceptions;
  private final int maxSavedParseExceptions;

  @JsonCreator
  public KinesisTuningConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") Long maxBytesInMemory,
      @JsonProperty("maxRowsPerSegment") Integer maxRowsPerSegment,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") Long handoffConditionTimeout,
      @JsonProperty("resetOffsetAutomatically") Boolean resetOffsetAutomatically,
      @JsonProperty("skipSequenceNumberAvailabilityCheck") Boolean skipSequenceNumberAvailabilityCheck,
      @JsonProperty("recordBufferSize") Integer recordBufferSize,
      @JsonProperty("recordBufferOfferTimeout") Integer recordBufferOfferTimeout,
      @JsonProperty("recordBufferFullWait") Integer recordBufferFullWait,
      @JsonProperty("fetchSequenceNumberTimeout") Integer fetchSequenceNumberTimeout,
      @JsonProperty("fetchThreads") Integer fetchThreads,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions
  )
  {
    // Cannot be a static because default basePersistDirectory is unique per-instance
    final RealtimeTuningConfig defaults = RealtimeTuningConfig.makeDefaultTuningConfig(basePersistDirectory);

    this.maxRowsInMemory = maxRowsInMemory == null ? defaults.getMaxRowsInMemory() : maxRowsInMemory;
    this.maxBytesInMemory = maxBytesInMemory == null ? defaults.getMaxBytesInMemory() : maxBytesInMemory;
    this.maxRowsPerSegment = maxRowsPerSegment == null ? DEFAULT_MAX_ROWS_PER_SEGMENT : maxRowsPerSegment;
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? defaults.getIntermediatePersistPeriod()
                                     : intermediatePersistPeriod;
    this.basePersistDirectory = defaults.getBasePersistDirectory();
    this.maxPendingPersists = maxPendingPersists == null ? defaults.getMaxPendingPersists() : maxPendingPersists;
    this.indexSpec = indexSpec == null ? defaults.getIndexSpec() : indexSpec;
    this.buildV9Directly = buildV9Directly == null ? defaults.getBuildV9Directly() : buildV9Directly;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? defaults.isReportParseExceptions()
                                 : reportParseExceptions;
    this.handoffConditionTimeout = handoffConditionTimeout == null
                                   ? defaults.getHandoffConditionTimeout()
                                   : handoffConditionTimeout;
    this.resetOffsetAutomatically = resetOffsetAutomatically == null
                                    ? DEFAULT_RESET_OFFSET_AUTOMATICALLY
                                    : resetOffsetAutomatically;
    this.skipSequenceNumberAvailabilityCheck = skipSequenceNumberAvailabilityCheck == null
                                               ? DEFAULT_SKIP_SEQUENCE_NUMBER_AVAILABILITY_CHECK
                                               : skipSequenceNumberAvailabilityCheck;
    this.recordBufferSize = recordBufferSize == null ? DEFAULT_RECORD_BUFFER_SIZE : recordBufferSize;
    this.recordBufferOfferTimeout = recordBufferOfferTimeout == null
                                    ? DEFAULT_RECORD_BUFFER_OFFER_TIMEOUT
                                    : recordBufferOfferTimeout;
    this.recordBufferFullWait = recordBufferFullWait == null ? DEFAULT_RECORD_BUFFER_FULL_WAIT : recordBufferFullWait;
    this.fetchSequenceNumberTimeout = fetchSequenceNumberTimeout
                                      == null ? DEFAULT_FETCH_SEQUENCE_NUMBER_TIMEOUT : fetchSequenceNumberTimeout;
    this.fetchThreads = fetchThreads; // we handle this being null later

    Preconditions.checkArgument(
        !this.resetOffsetAutomatically || !this.skipSequenceNumberAvailabilityCheck,
        "resetOffsetAutomatically cannot be used if skipSequenceNumberAvailabilityCheck=true"
    );

    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;

    if (this.reportParseExceptions) {
      this.maxParseExceptions = 0;
      this.maxSavedParseExceptions = maxSavedParseExceptions == null ? 0 : Math.min(1, maxSavedParseExceptions);
    } else {
      this.maxParseExceptions = maxParseExceptions == null ? TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS : maxParseExceptions;
      this.maxSavedParseExceptions = maxSavedParseExceptions == null
                                     ? TuningConfig.DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS
                                     : maxSavedParseExceptions;
    }
    this.logParseExceptions = logParseExceptions == null ? TuningConfig.DEFAULT_LOG_PARSE_EXCEPTIONS : logParseExceptions;
  }

  public static KinesisTuningConfig copyOf(KinesisTuningConfig config)
  {
    return new KinesisTuningConfig(
        config.maxRowsInMemory,
        config.maxBytesInMemory,
        config.maxRowsPerSegment,
        config.intermediatePersistPeriod,
        config.basePersistDirectory,
        config.maxPendingPersists,
        config.indexSpec,
        config.buildV9Directly,
        config.reportParseExceptions,
        config.handoffConditionTimeout,
        config.resetOffsetAutomatically,
        config.skipSequenceNumberAvailabilityCheck,
        config.recordBufferSize,
        config.recordBufferOfferTimeout,
        config.recordBufferFullWait,
        config.fetchSequenceNumberTimeout,
        config.fetchThreads,
        config.segmentWriteOutMediumFactory,
        config.logParseExceptions,
        config.maxParseExceptions,
        config.maxSavedParseExceptions
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

  @JsonProperty
  public int getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
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

  @Nullable
  @Override
  public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
  {
    return segmentWriteOutMediumFactory;
  }

  @Override
  @JsonProperty
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

  @JsonProperty
  public boolean getBuildV9Directly()
  {
    return buildV9Directly;
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

  @JsonProperty
  public boolean isSkipSequenceNumberAvailabilityCheck()
  {
    return skipSequenceNumberAvailabilityCheck;
  }

  @JsonProperty
  public int getRecordBufferSize()
  {
    return recordBufferSize;
  }

  @JsonProperty
  public int getRecordBufferOfferTimeout()
  {
    return recordBufferOfferTimeout;
  }

  @JsonProperty
  public int getRecordBufferFullWait()
  {
    return recordBufferFullWait;
  }

  @JsonProperty
  public int getFetchSequenceNumberTimeout()
  {
    return fetchSequenceNumberTimeout;
  }

  @JsonProperty
  public Integer getFetchThreads()
  {
    return fetchThreads;
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

  public KinesisTuningConfig withBasePersistDirectory(File dir)
  {
    return new KinesisTuningConfig(
        maxRowsInMemory,
        maxBytesInMemory,
        maxRowsPerSegment,
        intermediatePersistPeriod,
        dir,
        maxPendingPersists,
        indexSpec,
        buildV9Directly,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        skipSequenceNumberAvailabilityCheck,
        recordBufferSize,
        recordBufferOfferTimeout,
        recordBufferFullWait,
        fetchSequenceNumberTimeout,
        fetchThreads,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
    );
  }

  public KinesisTuningConfig withMaxRowsInMemory(int rows)
  {
    return new KinesisTuningConfig(
        rows,
        maxBytesInMemory,
        maxRowsPerSegment,
        intermediatePersistPeriod,
        basePersistDirectory,
        maxPendingPersists,
        indexSpec,
        buildV9Directly,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        skipSequenceNumberAvailabilityCheck,
        recordBufferSize,
        recordBufferOfferTimeout,
        recordBufferFullWait,
        fetchSequenceNumberTimeout,
        fetchThreads,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
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
    KinesisTuningConfig that = (KinesisTuningConfig) o;
    return getMaxRowsInMemory() == that.getMaxRowsInMemory() &&
           getMaxBytesInMemory() == that.getMaxBytesInMemory() &&
           getMaxRowsPerSegment() == that.getMaxRowsPerSegment() &&
           getMaxPendingPersists() == that.getMaxPendingPersists() &&
           getBuildV9Directly() == that.getBuildV9Directly() &&
           isReportParseExceptions() == that.isReportParseExceptions() &&
           getHandoffConditionTimeout() == that.getHandoffConditionTimeout() &&
           isResetOffsetAutomatically() == that.isResetOffsetAutomatically() &&
           isSkipSequenceNumberAvailabilityCheck() == that.isSkipSequenceNumberAvailabilityCheck() &&
           getRecordBufferSize() == that.getRecordBufferSize() &&
           getRecordBufferOfferTimeout() == that.getRecordBufferOfferTimeout() &&
           getRecordBufferFullWait() == that.getRecordBufferFullWait() &&
           getFetchSequenceNumberTimeout() == that.getFetchSequenceNumberTimeout() &&
           isLogParseExceptions() == that.isLogParseExceptions() &&
           getMaxParseExceptions() == that.getMaxParseExceptions() &&
           getMaxSavedParseExceptions() == that.getMaxSavedParseExceptions() &&
           Objects.equals(getIntermediatePersistPeriod(), that.getIntermediatePersistPeriod()) &&
           Objects.equals(getBasePersistDirectory(), that.getBasePersistDirectory()) &&
           Objects.equals(getIndexSpec(), that.getIndexSpec()) &&
           Objects.equals(getFetchThreads(), that.getFetchThreads()) &&
           Objects.equals(getSegmentWriteOutMediumFactory(), that.getSegmentWriteOutMediumFactory());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getMaxRowsInMemory(),
        getMaxBytesInMemory(),
        getMaxRowsPerSegment(),
        getIntermediatePersistPeriod(),
        getBasePersistDirectory(),
        getMaxPendingPersists(),
        getIndexSpec(),
        getBuildV9Directly(),
        isReportParseExceptions(),
        getHandoffConditionTimeout(),
        isResetOffsetAutomatically(),
        isSkipSequenceNumberAvailabilityCheck(),
        getRecordBufferSize(),
        getRecordBufferOfferTimeout(),
        getRecordBufferFullWait(),
        getFetchSequenceNumberTimeout(),
        getFetchThreads(),
        getSegmentWriteOutMediumFactory(),
        isLogParseExceptions(),
        getMaxParseExceptions(),
        getMaxSavedParseExceptions()
    );
  }

  @Override
  public String toString()
  {
    return "KinesisTuningConfig{" +
           "maxRowsInMemory=" + maxRowsInMemory +
           ", maxBytesInMemory=" + maxBytesInMemory +
           ", maxRowsPerSegment=" + maxRowsPerSegment +
           ", intermediatePersistPeriod=" + intermediatePersistPeriod +
           ", basePersistDirectory=" + basePersistDirectory +
           ", maxPendingPersists=" + maxPendingPersists +
           ", indexSpec=" + indexSpec +
           ", buildV9Directly=" + buildV9Directly +
           ", reportParseExceptions=" + reportParseExceptions +
           ", handoffConditionTimeout=" + handoffConditionTimeout +
           ", resetOffsetAutomatically=" + resetOffsetAutomatically +
           ", skipSequenceNumberAvailabilityCheck=" + skipSequenceNumberAvailabilityCheck +
           ", recordBufferSize=" + recordBufferSize +
           ", recordBufferOfferTimeout=" + recordBufferOfferTimeout +
           ", recordBufferFullWait=" + recordBufferFullWait +
           ", fetchSequenceNumberTimeout=" + fetchSequenceNumberTimeout +
           ", fetchThreads=" + fetchThreads +
           ", segmentWriteOutMediumFactory=" + segmentWriteOutMediumFactory +
           ", logParseExceptions=" + logParseExceptions +
           ", maxParseExceptions=" + maxParseExceptions +
           ", maxSavedParseExceptions=" + maxSavedParseExceptions +
           '}';
  }
}
