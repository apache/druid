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

package org.apache.druid.msq.dart.controller.sql;

import org.apache.druid.msq.exec.QueryKitBasedMSQPlanner;
import org.apache.druid.msq.exec.ResultsContext;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.QueryDefMSQSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.logical.DruidLogicalToQueryDefinitionTranslator;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;
import org.apache.druid.sql.calcite.run.QueryMaker;

import java.util.List;
import java.util.Map.Entry;

/**
 * Executes Dart queries with up-front planned {@link QueryDefinition}.
 *
 * Normal execution flow utilizes {@link QueryKit} to prdocue the plan;
 * meanwhile it also supports planning the {@link QueryDefinition} directly from
 * the {@link DruidLogicalNode}.
 */
class PrePlannedDartQueryMaker implements QueryMaker, QueryMaker.FromDruidLogical
{
  private PlannerContext plannerContext;
  private DartQueryMaker dartQueryMaker;

  public PrePlannedDartQueryMaker(PlannerContext plannerContext, QueryMaker queryMaker)
  {
    this.plannerContext = plannerContext;
    this.dartQueryMaker = (DartQueryMaker) queryMaker;
  }

  @Override
  public QueryResponse<Object[]> runQuery(DruidLogicalNode rootRel)
  {
    if (!plannerContext.getAuthorizationResult().allowAccessWithNoRestriction()) {
      throw new ForbiddenException(plannerContext.getAuthorizationResult().getErrorMessage());
    }
    DruidLogicalToQueryDefinitionTranslator qdt = new DruidLogicalToQueryDefinitionTranslator(plannerContext);
    QueryDefinition queryDef = qdt.translate(rootRel);
    QueryContext context = plannerContext.queryContext();
    QueryDefMSQSpec querySpec = MSQTaskQueryMaker.makeQueryDefMSQSpec(
        null,
        context,
        dartQueryMaker.fieldMapping,
        plannerContext,
        null, // Only used for DML, which this isn't
        queryDef
    );

    ResultsContext resultsContext = MSQTaskQueryMaker.makeSimpleResultContext(
        querySpec.getQueryDef(), rootRel.getRowType(), dartQueryMaker.fieldMapping, plannerContext
    );
    QueryResponse<Object[]> response = dartQueryMaker.runQueryDefMSQSpec(querySpec, context, resultsContext);
    return response;
  }

  @Override
  public QueryResponse<Object[]> runQuery(DruidQuery druidQuery)
  {
    QueryContext queryContext = druidQuery.getQuery().context();
    ResultsContext resultsContext = DartQueryMaker.makeResultsContext(druidQuery, dartQueryMaker.fieldMapping, plannerContext);
    QueryDefMSQSpec msqSpec = buildMSQSpec(druidQuery, dartQueryMaker.fieldMapping, queryContext, resultsContext);
    QueryResponse<Object[]> response = dartQueryMaker.runQueryDefMSQSpec(msqSpec, queryContext, resultsContext);
    return response;
  }

  private QueryDefMSQSpec buildMSQSpec(
      DruidQuery druidQuery,
      List<Entry<Integer, String>> fieldMapping,
      QueryContext queryContext,
      ResultsContext resultsContext)
  {
    LegacyMSQSpec querySpec = MSQTaskQueryMaker.makeLegacyMSQSpec(
        null,
        druidQuery,
        druidQuery.getQuery().context(),
        fieldMapping,
        plannerContext,
        null
    );

    String dartQueryId = queryContext.getString(DartSqlEngine.CTX_DART_QUERY_ID);

    QueryDefinition queryDef = new QueryKitBasedMSQPlanner(
        querySpec,
        resultsContext,
        querySpec.getQuery(),
        plannerContext.getJsonMapper(),
        dartQueryMaker.queryKitSpecFactory.makeQueryKitSpec(
            QueryKitBasedMSQPlanner
                .makeQueryControllerToolKit(querySpec.getContext(), plannerContext.getJsonMapper()),
            dartQueryId,
            querySpec.getTuningConfig(),
            querySpec.getContext()
        )
    ).makeQueryDefinition();

    return MSQTaskQueryMaker.makeQueryDefMSQSpec(
        null,
        druidQuery.getQuery().context(),
        fieldMapping,
        plannerContext,
        null,
        queryDef
    );
  }
}
