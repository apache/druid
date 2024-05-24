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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.query.QueryContext;

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

//  @JsonProperty("type")
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

  public static void supportsCompactionConfig(DataSourceCompactionConfig newConfig, String engineSource)
  {
    CompactionEngine compactionEngine = newConfig.getEngine();
    if (compactionEngine == CompactionEngine.MSQ) {
      if (newConfig.getTuningConfig() != null) {
        PartitionsSpec partitionsSpec = newConfig.getTuningConfig().getPartitionsSpec();

        if (partitionsSpec != null && !(partitionsSpec instanceof DimensionRangePartitionsSpec
                                        || partitionsSpec instanceof DynamicPartitionsSpec)) {
          throw InvalidInput.exception(
              "Invalid partition spec type[%s] for MSQ compaction engine[%s]."
              + " Type must be either DynamicPartitionsSpec or DynamicRangePartitionsSpec.",
              partitionsSpec.getClass(),
              engineSource
          );
        }
        if (partitionsSpec instanceof DynamicPartitionsSpec
            && ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows() != null) {
          throw InvalidInput.exception(
              "maxTotalRows[%d] in DynamicPartitionsSpec not supported for MSQ compaction engine[%s].",
              ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows(), engineSource
          );
        }
      }
      if (newConfig.getMetricsSpec() != null
          && newConfig.getGranularitySpec() != null
          && !newConfig.getGranularitySpec()
                       .isRollup()) {
        throw InvalidInput.exception(
            "rollup in granularitySpec must be set to True if metricsSpec is specifed "
            + "for MSQ compaction engine[%s].");
      }

      QueryContext queryContext = QueryContext.of(newConfig.getTaskContext());
      if (!queryContext.getBoolean(MSQContext.CTX_FINALIZE_AGGREGATIONS, true)) {
        throw InvalidInput.exception(
            "Config[%s] cannot be set to false for auto-compaction with MSQ engine.",
            MSQContext.CTX_FINALIZE_AGGREGATIONS
        );

      }
      if (queryContext.getString(MSQContext.CTX_TASK_ASSIGNMENT_STRATEGY, MSQContext.TASK_ASSIGNMENT_STRATEGY_MAX)
                      .equals(MSQContext.TASK_ASSIGNMENT_STRATEGY_AUTO)) {
        throw InvalidInput.exception(
            "Config[%s] cannot be set to value[%s] for auto-compaction with MSQ engine.",
            MSQContext.CTX_TASK_ASSIGNMENT_STRATEGY,
            MSQContext.TASK_ASSIGNMENT_STRATEGY_AUTO
        );
      }
    }
  }

  /**
   * This class copies over MSQ context parameters from the MSQ extension. This is required to validate the submitted
   * compaction config at the coordinator. The values used here should be kept in sync with those in
   * {@link org.apache.druid.msq.util.MultiStageQueryContext}
   */

  private static class MSQContext {
    public static final String CTX_FINALIZE_AGGREGATIONS = "finalizeAggregations";
    public static final String CTX_TASK_ASSIGNMENT_STRATEGY = "taskAssignment";
    public static final String TASK_ASSIGNMENT_STRATEGY_MAX = "MAX";
    public static final String TASK_ASSIGNMENT_STRATEGY_AUTO = "AUTO";
  }
}
