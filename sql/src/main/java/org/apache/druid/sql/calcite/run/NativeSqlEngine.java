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
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQuery;

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
  public boolean isSystemContextParameter(String contextParameterName)
  {
    return SYSTEM_CONTEXT_PARAMETERS.contains(contextParameterName);
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
  public boolean feature(EngineFeature feature, PlannerContext plannerContext)
  {
    switch (feature) {
      case ALLOW_BINDABLE_PLAN:
      case CAN_RUN_TIMESERIES:
      case CAN_RUN_TOPN:
        return true;
      case CAN_INSERT:
      case CAN_READ_EXTERNAL_DATA:
      case SCAN_CAN_ORDER_BY_NON_TIME:
      case SCAN_NEEDS_SIGNATURE:
        return false;
      case CAN_RUN_TIME_BOUNDARY:
        return QueryContexts.isTimeBoundaryPlanningEnabled(plannerContext.getQueryContext().getMergedParams());
      default:
        throw new IAE("Unrecognized feature: %s", feature);
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
      final String targetDataSource,
      final RelRoot relRoot,
      final PlannerContext plannerContext
  )
  {
    throw new UnsupportedOperationException();
  }
}
