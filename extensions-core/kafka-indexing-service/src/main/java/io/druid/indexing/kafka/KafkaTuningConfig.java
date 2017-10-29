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
  private static final boolean DEFAULT_RESET_OFFSET_AUTOMATICALLY = false;

  private final int maxRowsInMemory;
  private final int maxRowsPerSegment;
  private final Period intermediatePersistPeriod;
  private final File basePersistDirectory;
  private final int maxPendingPersists;
  private final IndexSpec indexSpec;
  private final boolean reportParseExceptions;
  @Deprecated
  private final long handoffConditionTimeout;
  private final boolean resetOffsetAutomatically;

  @JsonCreator
  public KafkaTuningConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxRowsPerSegment") Integer maxRowsPerSegment,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      // This parameter is left for compatibility when reading existing configs, to be removed in Druid 0.12.
      @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") Long handoffConditionTimeout,
      @JsonProperty("resetOffsetAutomatically") Boolean resetOffsetAutomatically
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
    this.reportParseExceptions = reportParseExceptions == null
                                 ? defaults.isReportParseExceptions()
                                 : reportParseExceptions;
    this.handoffConditionTimeout = handoffConditionTimeout == null
                                   ? defaults.getHandoffConditionTimeout()
                                   : handoffConditionTimeout;
    this.resetOffsetAutomatically = resetOffsetAutomatically == null
                                    ? DEFAULT_RESET_OFFSET_AUTOMATICALLY
                                    : resetOffsetAutomatically;
  }

  public static KafkaTuningConfig copyOf(KafkaTuningConfig config)
  {
    return new KafkaTuningConfig(
        config.maxRowsInMemory,
        config.maxRowsPerSegment,
        config.intermediatePersistPeriod,
        config.basePersistDirectory,
        config.maxPendingPersists,
        config.indexSpec,
        true,
        config.reportParseExceptions,
        config.handoffConditionTimeout,
        config.resetOffsetAutomatically
    );
  }

  @Override
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

  @Deprecated
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

  public KafkaTuningConfig withBasePersistDirectory(File dir)
  {
    return new KafkaTuningConfig(
        maxRowsInMemory,
        maxRowsPerSegment,
        intermediatePersistPeriod,
        dir,
        maxPendingPersists,
        indexSpec,
        true,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically
    );
  }

  public KafkaTuningConfig withMaxRowsInMemory(int rows)
  {
    return new KafkaTuningConfig(
        rows,
        maxRowsPerSegment,
        intermediatePersistPeriod,
        basePersistDirectory,
        maxPendingPersists,
        indexSpec,
        true,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically
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

    if (maxRowsInMemory != that.maxRowsInMemory) {
      return false;
    }
    if (maxRowsPerSegment != that.maxRowsPerSegment) {
      return false;
    }
    if (maxPendingPersists != that.maxPendingPersists) {
      return false;
    }
    if (reportParseExceptions != that.reportParseExceptions) {
      return false;
    }
    if (handoffConditionTimeout != that.handoffConditionTimeout) {
      return false;
    }
    if (resetOffsetAutomatically != that.resetOffsetAutomatically) {
      return false;
    }
    if (intermediatePersistPeriod != null
        ? !intermediatePersistPeriod.equals(that.intermediatePersistPeriod)
        : that.intermediatePersistPeriod != null) {
      return false;
    }
    if (basePersistDirectory != null
        ? !basePersistDirectory.equals(that.basePersistDirectory)
        : that.basePersistDirectory != null) {
      return false;
    }
    return indexSpec != null ? indexSpec.equals(that.indexSpec) : that.indexSpec == null;

  }

  @Override
  public int hashCode()
  {
    int result = maxRowsInMemory;
    result = 31 * result + maxRowsPerSegment;
    result = 31 * result + (intermediatePersistPeriod != null ? intermediatePersistPeriod.hashCode() : 0);
    result = 31 * result + (basePersistDirectory != null ? basePersistDirectory.hashCode() : 0);
    result = 31 * result + maxPendingPersists;
    result = 31 * result + (indexSpec != null ? indexSpec.hashCode() : 0);
    result = 31 * result + (reportParseExceptions ? 1 : 0);
    result = 31 * result + (int) (handoffConditionTimeout ^ (handoffConditionTimeout >>> 32));
    result = 31 * result + (resetOffsetAutomatically ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "KafkaTuningConfig{" +
           "maxRowsInMemory=" + maxRowsInMemory +
           ", maxRowsPerSegment=" + maxRowsPerSegment +
           ", intermediatePersistPeriod=" + intermediatePersistPeriod +
           ", basePersistDirectory=" + basePersistDirectory +
           ", maxPendingPersists=" + maxPendingPersists +
           ", indexSpec=" + indexSpec +
           ", reportParseExceptions=" + reportParseExceptions +
           ", handoffConditionTimeout=" + handoffConditionTimeout +
           ", resetOffsetAutomatically=" + resetOffsetAutomatically +
           '}';
  }
}
