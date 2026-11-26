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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.RunningQueryRegistry;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.destination.IngestDestination;
import org.apache.druid.sql.http.GetQueriesResponse;
import org.apache.druid.sql.http.NativeQueryInfo;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@LazySingleton
public class NativeSqlEngine implements SqlEngine
{
  private static final Logger LOG = new Logger(NativeSqlEngine.class);

  public static final Set<String> SYSTEM_CONTEXT_PARAMETERS = ImmutableSet.of(
      QueryContextParameters.MAX_TIME_ARRAY_OUTPUT_NAME.getName(),
      QueryContextParameters.MIN_TIME_ARRAY_OUTPUT_NAME.getName(),
      QueryContextParameters.TIMESTAMP_RESULT_FIELD.getName(),
      QueryContextParameters.TIMESTAMP_RESULT_FIELD_GRANULARITY.getName(),
      QueryContextParameters.TIMESTAMP_RESULT_FIELD_IN_ORIGINAL_DIMENSIONS.getName(),
      QueryContextParameters.SQL_INSERT_SEGMENT_GRANULARITY.getName(),
      QueryContextParameters.SQL_REPLACE_TIME_CHUNKS.getName()
  );

  public static final String NAME = "native";

  private final QueryLifecycleFactory queryLifecycleFactory;
  private final ObjectMapper jsonMapper;
  private final SqlStatementFactory sqlStatementFactory;
  private final RunningQueryRegistry runningQueryRegistry;

  @Inject
  public NativeSqlEngine(
      final QueryLifecycleFactory queryLifecycleFactory,
      final ObjectMapper jsonMapper,
      final SqlToolbox toolbox,
      final RunningQueryRegistry runningQueryRegistry
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.jsonMapper = jsonMapper;
    this.sqlStatementFactory = new SqlStatementFactory(toolbox.withEngine(this));
    this.runningQueryRegistry = runningQueryRegistry;
  }

  @VisibleForTesting
  public NativeSqlEngine(
      final QueryLifecycleFactory queryLifecycleFactory,
      final ObjectMapper jsonMapper,
      final SqlStatementFactory sqlStatementFactory
  )
  {
    this(queryLifecycleFactory, jsonMapper, sqlStatementFactory, new RunningQueryRegistry());
  }

  @VisibleForTesting
  public NativeSqlEngine(
      final QueryLifecycleFactory queryLifecycleFactory,
      final ObjectMapper jsonMapper,
      final SqlStatementFactory sqlStatementFactory,
      final RunningQueryRegistry runningQueryRegistry
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.jsonMapper = jsonMapper;
    this.sqlStatementFactory = sqlStatementFactory;
    this.runningQueryRegistry = runningQueryRegistry;
  }

  @Override
  public String name()
  {
    return NAME;
  }

  /**
   * Returns the queries that are currently executing on this server. Completed queries are not retained by the native
   * engine, so the {@code includeComplete} parameter has no effect. This engine has no way to collect queries from
   * other servers, so the list is always local to this server, regardless of the {@code selfOnly} parameter.
   */
  @Override
  public GetQueriesResponse getQueries(
      final boolean selfOnly,
      final boolean includeComplete,
      final AuthenticationResult authenticationResult,
      final AuthorizationResult stateReadAuthorization
  )
  {
    final List<NativeQueryInfo> queries = new ArrayList<>();

    for (final RunningQueryRegistry.Entry entry : runningQueryRegistry.getAll()) {
      queries.add(NativeQueryInfo.fromEntry(entry, serializeQuery(entry.getNativeQuery())));
    }

    if (stateReadAuthorization.allowAccessWithNoRestriction()) {
      // User can READ STATE, so they can see all running queries, as well as authentication details.
      return new GetQueriesResponse(List.copyOf(queries));
    } else {
      // User cannot READ STATE, so they can see only their own queries, without authentication details.
      return new GetQueriesResponse(
          queries.stream()
                 .filter(queryInfo -> isOwnQuery(authenticationResult, queryInfo))
                 .map(NativeQueryInfo::withoutAuthenticationResult)
                 .collect(Collectors.toList())
      );
    }
  }

  /**
   * Serializes the given native query, for the running queries API. Returns null when there is no query, or when the
   * query cannot be serialized.
   */
  @Nullable
  private String serializeQuery(@Nullable final Query<?> query)
  {
    if (query == null) {
      return null;
    }

    try {
      return jsonMapper.writeValueAsString(query);
    }
    catch (Exception e) {
      LOG.warn(e, "Could not serialize query[%s] for the running queries API", query.getId());
      return null;
    }
  }

  /**
   * Returns whether the given query belongs to the authenticated user, following the same rules as
   * {@code DartSqlEngine#isOwnQuery}.
   */
  private static boolean isOwnQuery(
      final AuthenticationResult authenticationResult,
      final NativeQueryInfo queryInfo
  )
  {
    return authenticationResult.getAuthenticatedBy() != null
           && authenticationResult.getIdentity() != null
           && Objects.equals(authenticationResult.getAuthenticatedBy(), queryInfo.getAuthenticator())
           && Objects.equals(authenticationResult.getIdentity(), queryInfo.getIdentity());
  }

  @Override
  public void validateContext(QueryContext queryContext)
  {
    SqlEngines.validateNoSpecialContextKeys(queryContext, SYSTEM_CONTEXT_PARAMETERS);
    validateJoinAlgorithm(queryContext);
  }

  @Override
  public RelDataType resultTypeForSelect(
      RelDataTypeFactory typeFactory,
      RelDataType validatedRowType,
      QueryContext queryContext
  )
  {
    return validatedRowType;
  }

  @Override
  public RelDataType resultTypeForInsert(
      RelDataTypeFactory typeFactory,
      RelDataType validatedRowType,
      QueryContext queryContext
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean featureAvailable(EngineFeature feature)
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
      case TIME_BOUNDARY_QUERY:
      case GROUPBY_IMPLICITLY_SORTS:
      case CAN_DDL:
        return true;
      case CAN_INSERT:
      case CAN_REPLACE:
      case READ_EXTERNAL_DATA:
      case WRITE_EXTERNAL_DATA:
      case SCAN_ORDER_BY_NON_TIME:
      case WINDOW_LEAF_OPERATOR:
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
   * Validates that {@link QueryContextParameters#SQL_JOIN_ALGORITHM} is {@link JoinAlgorithm#BROADCAST}. This is the
   * only join algorithm supported by native queries.
   */
  private static void validateJoinAlgorithm(final QueryContext queryContext)
  {
    final JoinAlgorithm joinAlgorithm = queryContext.getOrDefault(QueryContextParameters.SQL_JOIN_ALGORITHM);

    if (joinAlgorithm != JoinAlgorithm.BROADCAST) {
      throw InvalidSqlInput.exception("Join algorithm [%s] is not supported by engine [%s]", joinAlgorithm, NAME);
    }
  }

  @Override
  public SqlStatementFactory getSqlStatementFactory()
  {
    return sqlStatementFactory;
  }

  @Override
  public void cancelQuery(PlannerContext plannerContext, QueryScheduler queryScheduler)
  {
    final CopyOnWriteArrayList<String> nativeQueryIds = plannerContext.getNativeQueryIds();

    for (String nativeQueryId : nativeQueryIds) {
      LOG.debug("Canceling native query [%s]", nativeQueryId);
      queryScheduler.cancelQuery(nativeQueryId);
    }
  }
}
