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
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.DataSourcePlanner;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.msq.querykit.ShuffleSpecFactories;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Planner for {@link QueryDataSource}. Plans the subquery as a separate set of stages.
 */
public class QueryDataSourcePlanner implements DataSourcePlanner<QueryDataSource>
{
  /**
   * A map with {@link DruidSqlInsert#SQL_INSERT_SEGMENT_GRANULARITY} set to null, so we can clear it from the context
   * of subqueries.
   */
  private static final Map<String, Object> CONTEXT_MAP_NO_SEGMENT_GRANULARITY = new HashMap<>();

  static {
    CONTEXT_MAP_NO_SEGMENT_GRANULARITY.put(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY, null);
  }

  @Override
  public DataSourcePlan planDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final QueryDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    DataSourcePlannerUtils.checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);

    final QueryDefinition subQueryDef = queryKitSpec.getQueryKit().makeQueryDefinition(
        queryKitSpec,
        // Subqueries ignore SQL_INSERT_SEGMENT_GRANULARITY, even if set in the context. It's only used for the
        // outermost query, and setting it for the subquery makes us erroneously add bucketing where it doesn't belong.
        dataSource.getQuery().withOverriddenContext(CONTEXT_MAP_NO_SEGMENT_GRANULARITY),
        ShuffleSpecFactories.globalSortWithTargetPartitions(),
        minStageNumber
    );

    final int stageNumber = subQueryDef.getFinalStageDefinition().getStageNumber();

    return new DataSourcePlan(
        new InputNumberDataSource(0),
        Collections.singletonList(new StageInputSpec(stageNumber)),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        QueryDefinition.builder(subQueryDef)
    );
  }
}
