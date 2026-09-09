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

package org.apache.druid.msq.querykit.datasource;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.DataSourcePlanner;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.spec.QuerySegmentSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner for {@link UnionDataSource}. Plans each child, then concatenates their inputs.
 */
public class UnionDataSourcePlanner implements DataSourcePlanner<UnionDataSource>
{
  @Override
  public DataSourcePlan planDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final UnionDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    // This is done to prevent loss of generality since MSQ can plan any type of DataSource.
    final List<DataSource> children = dataSource.getChildren();

    final QueryDefinitionBuilder subqueryDefBuilder = QueryDefinition.builder(queryKitSpec.getQueryId());
    final List<DataSource> newChildren = new ArrayList<>();
    final List<InputSpec> inputSpecs = new ArrayList<>();
    final IntSet broadcastInputs = new IntOpenHashSet();

    for (final DataSource child : children) {
      final DataSourcePlan childDataSourcePlan = DataSourcePlan.forDataSource(
          queryKitSpec,
          queryContext,
          child,
          querySegmentSpec,
          Math.max(minStageNumber, subqueryDefBuilder.getNextStageNumber()),
          broadcast
      );

      final int shift = inputSpecs.size();

      newChildren.add(DataSourcePlannerUtils.shiftInputNumbers(childDataSourcePlan.getNewDataSource(), shift));
      inputSpecs.addAll(childDataSourcePlan.getInputSpecs());
      childDataSourcePlan.getSubQueryDefBuilder().ifPresent(subqueryDefBuilder::addAll);
      childDataSourcePlan.getBroadcastInputs().forEach(inp -> broadcastInputs.add(inp + shift));
    }

    return new DataSourcePlan(
        new UnionDataSource(newChildren),
        inputSpecs,
        broadcastInputs,
        subqueryDefBuilder
    );
  }
}
