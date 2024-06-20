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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.Map;
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

  public static class ValidationResult{
    private final boolean valid;
    private final String reason;

    public ValidationResult(boolean valid, String reason)
    {
      this.valid = valid;
      this.reason = reason;
    }

    public boolean isValid()
    {
      return valid;
    }

    public String getReason()
    {
      return reason;
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type);
  }

  public static ValidationResult validateCompactionConfig(
      DataSourceCompactionConfig newConfig,
      CompactionEngine defaultCompactionEngine
  )
  {
    CompactionEngine compactionEngine = newConfig.getEngine() == null ? defaultCompactionEngine : newConfig.getEngine();
    if (compactionEngine == CompactionEngine.NATIVE) {
      return new ValidationResult(true, null);
    } else {
      return msqEngineSupportsCompactionConfig(newConfig);
    }
  }

  /**
   * Checks if the provided compaction config is supported by MSQ. The following configs aren't supported:
   * <ul>
   * <li>finalizeAggregations set to false in context.</li>
   *
   * <li>partitionsSpec of type HashedParititionsSpec.</li>
   *
   * <li>maxTotalRows in DynamicPartitionsSpec.</li>
   *
   * <li>rollup set to false in granularitySpec when metricsSpec is specified.</li>
   * </ul>
   *
   * @param newConfig The updated compaction config
   * @return ValidationResult. The reason string is null if isValid() is True.
   */
  private static ValidationResult msqEngineSupportsCompactionConfig(DataSourceCompactionConfig newConfig)
  {
    if (newConfig.getTuningConfig() != null) {
      ValidationResult partitionSpecValidationResult =
          validatePartitionsSpec(newConfig.getTuningConfig().getPartitionsSpec());
      if (!partitionSpecValidationResult.isValid()) {
        return partitionSpecValidationResult;
      }
    }
    if (newConfig.getGranularitySpec() != null) {
      ValidationResult rollupValidationResult = validateRollup(
          newConfig.getMetricsSpec(),
          newConfig.getGranularitySpec().isRollup()
      );
      if (!rollupValidationResult.isValid()) {
        return rollupValidationResult;
      }
    }
    return new ValidationResult(true, null);
  }

  /**
   * Validte that partitionSpec is either 'dynamic` or 'range', and if 'dynamic', ensure maxTotalRows is null.
   */
  public static ValidationResult validatePartitionsSpec(PartitionsSpec partitionsSpec)
  {
    if (!(partitionsSpec instanceof DimensionRangePartitionsSpec
          || partitionsSpec instanceof DynamicPartitionsSpec)) {
      return new ValidationResult(false, StringUtils.format(
          "Invalid partition spec type[%s] for MSQ engine."
          + " Type must be either DynamicPartitionsSpec or DynamicRangePartitionsSpec.",
          partitionsSpec.getClass()
      )
      );
    }
    if (partitionsSpec instanceof DynamicPartitionsSpec
        && ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows() != null) {
      return new ValidationResult(false, StringUtils.format(
          "maxTotalRows[%d] in DynamicPartitionsSpec not supported for MSQ engine.",
          ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows()
      ));
    }
    return new ValidationResult(true, null);
  }

  /**
   * Validate rollup is set to false in granularitySpec when metricsSpec is specified.
   */
  public static ValidationResult validateRollup(AggregatorFactory[] metricsSpec, boolean isRollup) {
    if (metricsSpec != null && !isRollup) {
      return new ValidationResult(false, StringUtils.format(
          "rollup in granularitySpec must be set to True if metricsSpec is specifed "
          + "for MSQ engine."));
    }
    return new ValidationResult(true, null);
  }

  /**
   * This class copies over MSQ context parameters from the MSQ extension. This is required to validate the submitted
   * compaction config at the coordinator. The values used here should be kept in sync with those in
   * {@link org.apache.druid.msq.util.MultiStageQueryContext}
   */
  public static class MSQContext
  {
    public static final String CTX_FINALIZE_AGGREGATIONS = "finalizeAggregations";
  }
}
