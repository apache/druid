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

package io.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.TuningConfig;
import io.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.joda.time.Period;

import java.io.File;

public class KafkaTuningConfig implements TuningConfig, AppenderatorConfig
{
  private static final int DEFAULT_MAX_ROWS_PER_SEGMENT = 5_000_000;

  private final int maxRowsInMemory;
  private final int maxRowsPerSegment;
  private final Period intermediatePersistPeriod;
  private final File basePersistDirectory;
  private final int maxPendingPersists;
  private final IndexSpec indexSpec;
  private final boolean buildV9Directly;
  private final boolean reportParseExceptions;
  private final long handoffConditionTimeout;

  @JsonCreator
  public KafkaTuningConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxRowsPerSegment") Integer maxRowsPerSegment,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") Long handoffConditionTimeout
  )
  {
    // Cannot be a static because default basePersistDirectory is unique per-instance
    final RealtimeTuningConfig defaults = RealtimeTuningConfig.makeDefaultTuningConfig(basePersistDirectory);

    this.maxRowsInMemory = maxRowsInMemory == null ? defaults.getMaxRowsInMemory() : maxRowsInMemory;
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
  }

  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @JsonProperty
  public int getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
  }

  @JsonProperty
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }

  @JsonProperty
  public File getBasePersistDirectory()
  {
    return basePersistDirectory;
  }

  @JsonProperty
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

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

  public KafkaTuningConfig withBasePersistDirectory(File dir)
  {
    return new KafkaTuningConfig(
        maxRowsInMemory,
        maxRowsPerSegment,
        intermediatePersistPeriod,
        dir,
        maxPendingPersists,
        indexSpec,
        buildV9Directly,
        reportParseExceptions,
        handoffConditionTimeout
    );
  }
}
