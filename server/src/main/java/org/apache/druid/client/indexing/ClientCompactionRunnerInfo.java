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
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
      return CompactionConfigValidationResult.success();
    } else {
      return compactionConfigSupportedByMSQEngine(newConfig);
    }
  }

  /**
   * Checks if the provided compaction config is supported by MSQ. The following configs aren't supported:
   * <ul>
   * <li>partitionsSpec of type HashedParititionsSpec.</li>
   * <li>maxTotalRows in DynamicPartitionsSpec.</li>
   * <li>rollup in granularitySpec set to false when metricsSpec is specified or true when it's empty.</li>
   * <li>any metric is non-idempotent, i.e. it defines some aggregatorFactory 'A' s.t. 'A != A.combiningFactory()'.</li>
   * </ul>
   */
  private static CompactionConfigValidationResult compactionConfigSupportedByMSQEngine(DataSourceCompactionConfig newConfig)
  {
    List<CompactionConfigValidationResult> validationResults = new ArrayList<>();
    if (newConfig.getTuningConfig() != null) {
      validationResults.add(validatePartitionsSpecForMSQ(newConfig.getTuningConfig().getPartitionsSpec()));
    }
    if (newConfig.getGranularitySpec() != null) {
      validationResults.add(validateRollupForMSQ(
          newConfig.getMetricsSpec(),
          newConfig.getGranularitySpec().isRollup()
      ));
    }
    validationResults.add(validateMaxNumTasksForMSQ(newConfig.getTaskContext()));
    validationResults.add(validateMetricsSpecForMSQ(newConfig.getMetricsSpec()));
    return validationResults.stream()
                            .filter(result -> !result.isValid())
                            .findFirst()
                            .orElse(CompactionConfigValidationResult.success());
  }

  /**
   * Validate that partitionSpec is either 'dynamic` or 'range', and if 'dynamic', ensure 'maxTotalRows' is null.
   */
  public static CompactionConfigValidationResult validatePartitionsSpecForMSQ(PartitionsSpec partitionsSpec)
  {
    if (!(partitionsSpec instanceof DimensionRangePartitionsSpec
          || partitionsSpec instanceof DynamicPartitionsSpec)) {
      return CompactionConfigValidationResult.failure(
          "MSQ: Invalid partitioning type[%s]. Must be either 'dynamic' or 'range'",
          partitionsSpec.getClass().getSimpleName()

      );
    }
    if (partitionsSpec instanceof DynamicPartitionsSpec
        && ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows() != null) {
      return CompactionConfigValidationResult.failure(
          "MSQ: 'maxTotalRows' not supported with 'dynamic' partitioning"
      );
    }
    return CompactionConfigValidationResult.success();
  }

  /**
   * Validate rollup in granularitySpec is set to true when metricsSpec is specified and false if it's null.
   * If rollup set to null, all existing segments are analyzed, and it's set to true iff all segments have rollup
   * set to true.
   */
  public static CompactionConfigValidationResult validateRollupForMSQ(
      AggregatorFactory[] metricsSpec,
      @Nullable Boolean isRollup
  )
  {
    if (metricsSpec != null && metricsSpec.length != 0 && isRollup != null && !isRollup) {
      return CompactionConfigValidationResult.failure(
          "MSQ: 'granularitySpec.rollup' must be true if 'metricsSpec' is specified"
      );
    } else if ((metricsSpec == null || metricsSpec.length == 0) && isRollup != null && isRollup) {
      return CompactionConfigValidationResult.failure(
          "MSQ: 'granularitySpec.rollup' must be false if 'metricsSpec' is null"
      );
    }
    return CompactionConfigValidationResult.success();
  }

  /**
   * Validate maxNumTasks >= 2 in context.
   */
  public static CompactionConfigValidationResult validateMaxNumTasksForMSQ(Map<String, Object> context)
  {
    if (context != null) {
      int maxNumTasks = QueryContext.of(context)
                                    .getInt(ClientMSQContext.CTX_MAX_NUM_TASKS, ClientMSQContext.DEFAULT_MAX_NUM_TASKS);
      if (maxNumTasks < 2) {
        return CompactionConfigValidationResult.failure(
            "MSQ: Context maxNumTasks[%,d] must be at least 2 (1 controller + 1 worker)",
            maxNumTasks
        );
      }
    }
    return CompactionConfigValidationResult.success();
  }

  /**
   * Validate each metric is idempotent, i.e. it defines some aggregatorFactory 'A' s.t. 'A = A.combiningFactory()'.
   */
  public static CompactionConfigValidationResult validateMetricsSpecForMSQ(AggregatorFactory[] metricsSpec)
  {
    if (metricsSpec == null) {
      return CompactionConfigValidationResult.success();
    }
    return Arrays.stream(metricsSpec)
                 .filter(aggregatorFactory -> !aggregatorFactory.equals(aggregatorFactory.getCombiningFactory()))
                 .findFirst()
                 .map(aggregatorFactory ->
                          CompactionConfigValidationResult.failure(
                              "MSQ: Non-idempotent aggregator[%s] not supported in 'metricsSpec'.",
                              aggregatorFactory.getName()
                          )
                 ).orElse(CompactionConfigValidationResult.success());
  }
}
