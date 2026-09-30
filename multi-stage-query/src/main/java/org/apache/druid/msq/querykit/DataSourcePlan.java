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

package org.apache.druid.msq.querykit;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

/**
 * Plan for getting data from a {@link DataSource}. Used by {@link QueryKit} implementations.
 */
public class DataSourcePlan
{
  private final DataSource newDataSource;
  private final List<InputSpec> inputSpecs;
  private final IntSet broadcastInputs;

  @Nullable
  private final QueryDefinitionBuilder subQueryDefBuilder;

  public DataSourcePlan(
      final DataSource newDataSource,
      final List<InputSpec> inputSpecs,
      final IntSet broadcastInputs,
      @Nullable final QueryDefinitionBuilder subQueryDefBuilder
  )
  {
    this.newDataSource = Preconditions.checkNotNull(newDataSource, "newDataSource");
    this.inputSpecs = Preconditions.checkNotNull(inputSpecs, "inputSpecs");
    this.broadcastInputs = Preconditions.checkNotNull(broadcastInputs, "broadcastInputs");
    this.subQueryDefBuilder = subQueryDefBuilder;

    for (int broadcastInput : broadcastInputs) {
      if (broadcastInput < 0 || broadcastInput >= inputSpecs.size()) {
        throw new IAE("Broadcast input number [%d] out of range [0, %d)", broadcastInput, inputSpecs.size());
      }
    }
  }

  public DataSourcePlan withDataSource(DataSource newDataSource)
  {
    return new DataSourcePlan(newDataSource, inputSpecs, broadcastInputs, subQueryDefBuilder);
  }

  /**
   * Build a plan.
   *
   * @param queryKitSpec     reference for recursive planning
   * @param queryContext     query context
   * @param dataSource       datasource to plan
   * @param querySegmentSpec intervals for mandatory pruning. Must be {@link MultipleIntervalSegmentSpec}. The returned
   *                         plan is guaranteed to be filtered to this interval.
   * @param minStageNumber   starting stage number for subqueries
   * @param broadcast        whether the plan should broadcast data for this datasource
   */
  public static DataSourcePlan forDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final DataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    //noinspection rawtypes
    final DataSourcePlanner planner = queryKitSpec.getDataSourcePlanners().getPlanner(dataSource.getClass());
    if (planner == null) {
      throw new UOE("Cannot handle dataSource [%s]", dataSource);
    }

    //noinspection unchecked
    return planner.planDataSource(
        queryKitSpec,
        queryContext,
        dataSource,
        querySegmentSpec,
        minStageNumber,
        broadcast
    );
  }

  /**
   * Possibly remapped datasource that should be used when processing. Will be either the original datasource, or the
   * original datasource with itself or some children replaced by {@link InputNumberDataSource}. Any added
   * {@link InputNumberDataSource} refer to {@link StageInputSpec} in {@link #getInputSpecs()}.
   */
  public DataSource getNewDataSource()
  {
    return newDataSource;
  }

  /**
   * Input specs that should be used when processing.
   */
  public List<InputSpec> getInputSpecs()
  {
    return inputSpecs;
  }

  /**
   * Which input specs from {@link #getInputSpecs()} are broadcast.
   */
  public IntSet getBroadcastInputs()
  {
    return broadcastInputs;
  }

  /**
   * Figure for {@link StageDefinition#getMaxWorkerCount()} that should be used when processing.
   */
  public int getMaxWorkerCount()
  {
    if (isSingleWorker()) {
      return 1;
    } else {
      // Use MAX_WORKERS as a high upper bound; capped at runtime by QueryDefinition.withRuntimeBounds.
      return Limits.MAX_WORKERS;
    }
  }

  /**
   * Returns a {@link QueryDefinitionBuilder} that includes any {@link StageInputSpec} from {@link #getInputSpecs()}.
   * Absent if this plan does not involve reading from prior stages.
   */
  public Optional<QueryDefinitionBuilder> getSubQueryDefBuilder()
  {
    return Optional.ofNullable(subQueryDefBuilder);
  }

  /**
   * Whether this datasource must be processed by a single worker. True if, and only if, all inputs are broadcast.
   */
  public boolean isSingleWorker()
  {
    return broadcastInputs.size() == inputSpecs.size();
  }
}
