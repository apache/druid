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
package io.druid.indexing.common.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.TuningConfig;
import io.druid.segment.realtime.appenderator.AppenderatorConfig;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;

@JsonTypeName("realtime_appenderator")
public class RealtimeAppenderatorTuningConfig implements TuningConfig, AppenderatorConfig
{
  private static final int defaultMaxRowsInMemory = TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY;
  private static final int defaultMaxRowsPerSegment = 5_000_000;
  private static final Period defaultIntermediatePersistPeriod = new Period("PT10M");
  private static final int defaultMaxPendingPersists = 0;
  private static final ShardSpec defaultShardSpec = NoneShardSpec.instance();
  private static final IndexSpec defaultIndexSpec = new IndexSpec();
  private static final Boolean defaultReportParseExceptions = Boolean.FALSE;
  private static final long defaultPublishAndHandoffTimeout = 0;
  private static final long defaultAlertTimeout = 0;

  private static File createNewBasePersistDirectory()
  {
    return Files.createTempDir();
  }



  private final int maxRowsInMemory;
  private final int maxRowsPerSegment;
  private final long maxBytesInMemory;
  private final Period intermediatePersistPeriod;
  private final File basePersistDirectory;
  private final int maxPendingPersists;
  private final ShardSpec shardSpec;
  private final IndexSpec indexSpec;
  private final boolean reportParseExceptions;
  private final long publishAndHandoffTimeout;
  private final long alertTimeout;
  @Nullable
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

  private final boolean logParseExceptions;
  private final int maxParseExceptions;
  private final int maxSavedParseExceptions;

  @JsonCreator
  public RealtimeAppenderatorTuningConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
      @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("shardSpec") ShardSpec shardSpec,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("publishAndHandoffTimeout") Long publishAndHandoffTimeout,
      @JsonProperty("alertTimeout") Long alertTimeout,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions
  )
  {
    this.maxRowsInMemory = maxRowsInMemory == null ? defaultMaxRowsInMemory : maxRowsInMemory;
    this.maxRowsPerSegment = maxRowsPerSegment == null ? defaultMaxRowsPerSegment : maxRowsPerSegment;
    // initializing this to 0, it will be lazily intialized to a value
    // @see server.src.main.java.io.druid.segment.indexing.TuningConfigs#getMaxBytesInMemoryOrDefault(long)
    this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? defaultIntermediatePersistPeriod
                                     : intermediatePersistPeriod;
    this.basePersistDirectory = basePersistDirectory == null ? createNewBasePersistDirectory() : basePersistDirectory;
    this.maxPendingPersists = maxPendingPersists == null ? defaultMaxPendingPersists : maxPendingPersists;
    this.shardSpec = shardSpec == null ? defaultShardSpec : shardSpec;
    this.indexSpec = indexSpec == null ? defaultIndexSpec : indexSpec;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? defaultReportParseExceptions
                                 : reportParseExceptions;
    this.publishAndHandoffTimeout = publishAndHandoffTimeout == null
                                   ? defaultPublishAndHandoffTimeout
                                   : publishAndHandoffTimeout;
    Preconditions.checkArgument(this.publishAndHandoffTimeout >= 0, "publishAndHandoffTimeout must be >= 0");

    this.alertTimeout = alertTimeout == null ? defaultAlertTimeout : alertTimeout;
    Preconditions.checkArgument(this.alertTimeout >= 0, "alertTimeout must be >= 0");
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

  @Override
  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @Override
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

  @Override
  @JsonProperty
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  @JsonProperty
  public ShardSpec getShardSpec()
  {
    return shardSpec;
  }

  @Override
  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @Override
  @JsonProperty
  public boolean isReportParseExceptions()
  {
    return reportParseExceptions;
  }

  @JsonProperty
  public long getPublishAndHandoffTimeout()
  {
    return publishAndHandoffTimeout;
  }

  @JsonProperty
  public long getAlertTimeout()
  {
    return alertTimeout;
  }

  @Override
  @JsonProperty
  @Nullable
  public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
  {
    return segmentWriteOutMediumFactory;
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

  public RealtimeAppenderatorTuningConfig withBasePersistDirectory(File dir)
  {
    return new RealtimeAppenderatorTuningConfig(
        maxRowsInMemory,
        maxRowsPerSegment,
        maxBytesInMemory,
        intermediatePersistPeriod,
        dir,
        maxPendingPersists,
        shardSpec,
        indexSpec,
        reportParseExceptions,
        publishAndHandoffTimeout,
        alertTimeout,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
    );
  }
}
