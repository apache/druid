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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * This class is just used to pass the strategy type via the "type" parameter for deserilization to appropriate
 * {@link org.apache.druid.indexing.common.task.CompactionRunner} subtype at the overlod.
 */
public class ClientCompactionRunnerInfo
{
  private final CompactionEngine type;

  @JsonCreator
  public ClientCompactionRunnerInfo(@JsonProperty("type") CompactionEngine type)
  {
    this.type = type;
  }

  @JsonProperty
  public CompactionEngine getType()
  {
    return type;
  }

  @Override
  public String toString()
  {
    return "ClientCompactionRunnerInfo{" +
           "type=" + type +
           '}';
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
    ClientCompactionRunnerInfo that = (ClientCompactionRunnerInfo) o;
    return type == that.type;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type);
  }

  public static CompactionConfigValidationResult validateCompactionConfig(
      DataSourceCompactionConfig newConfig,
      CompactionEngine defaultCompactionEngine
  )
  {
    CompactionEngine compactionEngine = newConfig.getEngine() == null ? defaultCompactionEngine : newConfig.getEngine();
    if (compactionEngine == CompactionEngine.NATIVE) {
      return new CompactionConfigValidationResult(true, null);
    } else {
      return msqEngineSupportsCompactionConfig(newConfig);
    }
  }

  /**
   * Checks if the provided compaction config is supported by MSQ. The following configs aren't supported:
   * <ul>
   * <li>partitionsSpec of type HashedParititionsSpec.</li>
   * <li>maxTotalRows in DynamicPartitionsSpec.</li>
   * <li>rollup set to false in granularitySpec when metricsSpec is specified. Null is treated as true.</li>
   * <li>queryGranularity set to ALL in granularitySpec.</li>
   * </ul>
   */
  private static CompactionConfigValidationResult msqEngineSupportsCompactionConfig(DataSourceCompactionConfig newConfig)
  {
    List<CompactionConfigValidationResult> validationResults = new ArrayList<>();
    if (newConfig.getTuningConfig() != null) {
      validationResults.add(validatePartitionsSpecForMsq(newConfig.getTuningConfig().getPartitionsSpec()));
    }
    if (newConfig.getGranularitySpec() != null) {
      validationResults.add(validateRollupForMsq(
          newConfig.getMetricsSpec(),
          newConfig.getGranularitySpec().isRollup()
      ));
      validationResults.add(validateQueryGranularityForMsq(newConfig.getGranularitySpec().getQueryGranularity()));
    }
    return validationResults.stream()
                            .filter(result -> !result.isValid())
                            .findFirst()
                            .orElse(new CompactionConfigValidationResult(true, null));
  }

  /**
   * Validate that partitionSpec is either 'dynamic` or 'range', and if 'dynamic', ensure 'maxTotalRows' is null.
   */
  public static CompactionConfigValidationResult validatePartitionsSpecForMsq(PartitionsSpec partitionsSpec)
  {
    if (!(partitionsSpec instanceof DimensionRangePartitionsSpec
          || partitionsSpec instanceof DynamicPartitionsSpec)) {
      return new CompactionConfigValidationResult(
          false,
          "Invalid partitionsSpec type[%s] for MSQ engine. Type must be either 'dynamic' or 'range'.",
          partitionsSpec.getClass().getSimpleName()

      );
    }
    if (partitionsSpec instanceof DynamicPartitionsSpec
        && ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows() != null) {
      return new CompactionConfigValidationResult(
          false,
          "maxTotalRows[%d] in DynamicPartitionsSpec not supported for MSQ engine.",
          ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows()
      );
    }
    return new CompactionConfigValidationResult(true, null);
  }

  /**
   * Validate rollup is set to false in granularitySpec when metricsSpec is specified.
   */
  public static CompactionConfigValidationResult validateRollupForMsq(
      AggregatorFactory[] metricsSpec,
      @Nullable Boolean isRollup
  )
  {
    if (metricsSpec != null && isRollup != null && !isRollup) {
      return new CompactionConfigValidationResult(
          false,
          "rollup in granularitySpec must be set to True if metricsSpec is specifed for MSQ engine."
      );
    }
    return new CompactionConfigValidationResult(true, null);
  }

  /**
   * Validate query granularity is not set to ALL.
   */
  public static CompactionConfigValidationResult validateQueryGranularityForMsq(Granularity queryGranularity)
  {
    if (queryGranularity != null && queryGranularity.equals(Granularities.ALL)) {
      return new CompactionConfigValidationResult(
          false,
          "queryGranularity[ALL] in granularitySpec not supported for MSQ engine"
      );
    }
    return new CompactionConfigValidationResult(true, null);
  }
}
