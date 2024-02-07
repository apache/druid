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

package org.apache.druid.sql.calcite.run;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.planner.JoinAlgorithm;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.destination.IngestDestination;

import java.util.Map;
import java.util.Set;

@LazySingleton
public class NativeSqlEngine implements SqlEngine
{
  public static final Set<String> SYSTEM_CONTEXT_PARAMETERS = ImmutableSet.of(
      TimeBoundaryQuery.MAX_TIME_ARRAY_OUTPUT_NAME,
      TimeBoundaryQuery.MIN_TIME_ARRAY_OUTPUT_NAME,
      GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD,
      GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_GRANULARITY,
      GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_INDEX,
      DruidQuery.CTX_SCAN_SIGNATURE,
      DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
      DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS
  );

  private static final String NAME = "native";

  private final QueryLifecycleFactory queryLifecycleFactory;
  private final ObjectMapper jsonMapper;

  @Inject
  public NativeSqlEngine(
      final QueryLifecycleFactory queryLifecycleFactory,
      final ObjectMapper jsonMapper
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public String name()
  {
    return NAME;
  }

  @Override
  public void validateContext(Map<String, Object> queryContext)
  {
    SqlEngines.validateNoSpecialContextKeys(queryContext, SYSTEM_CONTEXT_PARAMETERS);
    validateJoinAlgorithm(queryContext);
  }

  @Override
  public RelDataType resultTypeForSelect(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
  {
    return validatedRowType;
  }

  @Override
  public RelDataType resultTypeForInsert(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean featureAvailable(EngineFeature feature, PlannerContext plannerContext)
  {
    switch (feature) {
      case CAN_SELECT:
      case ALLOW_BINDABLE_PLAN:
      case TIMESERIES_QUERY:
      case TOPN_QUERY:
      case GROUPING_SETS:
      case WINDOW_FUNCTIONS:
      case UNNEST:
      case ALLOW_BROADCAST_RIGHTY_JOIN:
      case ALLOW_TOP_LEVEL_UNION_ALL:
        return true;
      case TIME_BOUNDARY_QUERY:
        return plannerContext.queryContext().isTimeBoundaryPlanningEnabled();
      case CAN_INSERT:
      case CAN_REPLACE:
      case READ_EXTERNAL_DATA:
      case WRITE_EXTERNAL_DATA:
      case SCAN_ORDER_BY_NON_TIME:
      case SCAN_NEEDS_SIGNATURE:
        return false;
      default:
        throw SqlEngines.generateUnrecognizedFeatureException(NativeSqlEngine.class.getSimpleName(), feature);
    }
  }

  @Override
  public QueryMaker buildQueryMakerForSelect(final RelRoot relRoot, final PlannerContext plannerContext)
  {
    return new NativeQueryMaker(
        queryLifecycleFactory,
        plannerContext,
        jsonMapper,
        relRoot.fields
    );
  }

  @Override
  public QueryMaker buildQueryMakerForInsert(
      final IngestDestination destination,
      final RelRoot relRoot,
      final PlannerContext plannerContext
  )
  {
    throw new UnsupportedOperationException();
  }

  /**
   * Validates that {@link PlannerContext#CTX_SQL_JOIN_ALGORITHM} is {@link JoinAlgorithm#BROADCAST}. This is the
   * only join algorithm supported by native queries.
   */
  private static void validateJoinAlgorithm(final Map<String, Object> queryContext)
  {
    final JoinAlgorithm joinAlgorithm = PlannerContext.getJoinAlgorithm(queryContext);

    if (joinAlgorithm != JoinAlgorithm.BROADCAST) {
      throw InvalidSqlInput.exception("Join algorithm [%s] is not supported by engine [%s]", joinAlgorithm, NAME);
    }
  }
}
