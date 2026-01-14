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

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.error.DruidException;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.destination.IngestDestination;
import org.apache.druid.sql.http.GetQueriesResponse;
import org.apache.druid.sql.http.GetQueryReportResponse;
import org.apache.druid.sql.http.QueryInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Engine for running SQL queries.
 */
public interface SqlEngine
{
  /**
   * Name of this engine. Appears in user-visible messages.
   */
  String name();

  /**
   * Whether a feature applies to this engine or not. Most callers should use
   * {@link PlannerContext#featureAvailable(EngineFeature)} instead, which also checks feature flags in context
   * parameters.
   */
  boolean featureAvailable(EngineFeature feature);

  /**
   * Validates a provided query context. Returns quietly if the context is OK; throws {@link ValidationException}
   * if the context has a problem.
   */
  void validateContext(Map<String, Object> queryContext);

  /**
   * SQL row type that would be emitted by the {@link QueryMaker} from {@link #buildQueryMakerForSelect}.
   * Called for SELECT. Not called for EXPLAIN, which is handled by the planner itself.
   *
   * @param typeFactory      type factory
   * @param validatedRowType row type from Calcite's validator
   * @param queryContext     query context, in case that affects the result type
   */
  RelDataType resultTypeForSelect(
      RelDataTypeFactory typeFactory,
      RelDataType validatedRowType,
      Map<String, Object> queryContext
  );

  /**
   * SQL row type that would be emitted by the {@link QueryMaker} from {@link #buildQueryMakerForInsert}.
   * Called for INSERT and REPLACE. Not called for EXPLAIN, which is handled by the planner itself.
   *
   * @param typeFactory      type factory
   * @param validatedRowType row type from Calcite's validator
   * @param queryContext     query context, in case that affects the result type
   */
  RelDataType resultTypeForInsert(
      RelDataTypeFactory typeFactory,
      RelDataType validatedRowType,
      Map<String, Object> queryContext
  );

  /**
   * Create a {@link QueryMaker} for a SELECT query.
   *
   * @param relRoot        planned and validated rel
   * @param plannerContext context for this query
   *
   * @return an executor for the provided query
   *
   * @throws ValidationException if this engine cannot build an executor for the provided query
   */
  @SuppressWarnings("RedundantThrows")
  QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext) throws ValidationException;

  /**
   * Create a {@link QueryMaker} for an INSERT ... SELECT query.
   *
   * @param destination    destination for the INSERT portion of the query
   * @param relRoot        planned and validated rel for the SELECT portion of the query
   * @param plannerContext context for this query
   *
   * @return an executor for the provided query
   *
   * @throws ValidationException if this engine cannot build an executor for the provided query
   */
  @SuppressWarnings("RedundantThrows")
  QueryMaker buildQueryMakerForInsert(
      IngestDestination destination,
      RelRoot relRoot,
      PlannerContext plannerContext
  ) throws ValidationException;

  /**
   * Enables the engine to make changes to the Context.
   */
  default void initContextMap(Map<String, Object> contextMap)
  {
  }

  /**
   * Returns a {@link SqlStatementFactory} which uses this engine to create statements.
   */
  SqlStatementFactory getSqlStatementFactory();

  /**
   * Returns a list of {@link QueryInfo} containing the currently running queries using this engine. Returns an empty
   * list if the operation is not supported.
   *
   * @param selfOnly               whether to only include queries running on this server. If false, this server should
   *                               contact all other servers to build a full list of all running queries.
   * @param authenticationResult   implementations should use this for filtering the list of visible queries
   * @param stateReadAuthorization authorization for the STATE READ resource. If this is authorized, implementations
   *                               should allow all queries to be visible
   */
  default GetQueriesResponse getRunningQueries(
      boolean selfOnly,
      AuthenticationResult authenticationResult,
      AuthorizationResult stateReadAuthorization
  )
  {
    return new GetQueriesResponse(List.of());
  }

  /**
   * Retrieves the report for a query, if available.
   *
   * @param sqlQueryId             SQL query ID to retrieve the report for
   * @param selfOnly               whether to only include queries running on this server. If false, this server should
   *                               contact all other servers to find this query, if necessary.
   * @param authenticationResult   implementations should use this to determine if a query should be visible to a user
   * @param stateReadAuthorization authorization for the STATE READ resource. If this is authorized, implementations
   *                               should allow all queries to be visible
   */
  @Nullable
  default GetQueryReportResponse getQueryReport(
      String sqlQueryId,
      boolean selfOnly,
      AuthenticationResult authenticationResult,
      AuthorizationResult stateReadAuthorization
  )
  {
    return null;
  }

  /**
   * Cancels a currently running query given the {@link PlannerContext} for the query.
   */
  default void cancelQuery(PlannerContext plannerContext, QueryScheduler queryScheduler)
  {
    throw DruidException.forPersona(DruidException.Persona.USER)
                        .ofCategory(DruidException.Category.UNSUPPORTED)
                        .build("Engine[%s] does not support canceling queries", name());
  }
}
