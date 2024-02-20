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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.RegisteredLookupExtractionFn;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rule.AggregatePullUpLookupRule;
import org.apache.druid.sql.calcite.rule.ReverseLookupRule;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Like {@link PlannerConfig}, but that has static configuration and this class
 * contains dynamic, per-query configuration. Additional Druid-specific static
 * configuration resides in the {@link PlannerToolbox} class.
 */
public class PlannerContext
{
  // Query context keys
  public static final String CTX_SQL_CURRENT_TIMESTAMP = "sqlCurrentTimestamp";
  public static final String CTX_SQL_TIME_ZONE = "sqlTimeZone";
  public static final String CTX_SQL_JOIN_ALGORITHM = "sqlJoinAlgorithm";
  private static final JoinAlgorithm DEFAULT_SQL_JOIN_ALGORITHM = JoinAlgorithm.BROADCAST;

  /**
   * Undocumented context key, used internally, to allow the web console to
   * apply a limit without having to rewrite the SQL query.
   */
  public static final String CTX_SQL_OUTER_LIMIT = "sqlOuterLimit";

  /**
   * Key to enable window functions.
   */
  public static final String CTX_ENABLE_WINDOW_FNS = "enableWindowing";

  /**
   * Context key for {@link PlannerContext#isUseBoundsAndSelectors()}.
   */
  public static final String CTX_SQL_USE_BOUNDS_AND_SELECTORS = "sqlUseBoundAndSelectors";
  public static final boolean DEFAULT_SQL_USE_BOUNDS_AND_SELECTORS = NullHandling.replaceWithDefault();

  /**
   * Context key for {@link PlannerContext#isPullUpLookup()}.
   */
  public static final String CTX_SQL_PULL_UP_LOOKUP = "sqlPullUpLookup";
  public static final boolean DEFAULT_SQL_PULL_UP_LOOKUP = true;

  /**
   * Context key for {@link PlannerContext#isReverseLookup()}.
   */
  public static final String CTX_SQL_REVERSE_LOOKUP = "sqlReverseLookup";
  public static final boolean DEFAULT_SQL_REVERSE_LOOKUP = true;

  // DataContext keys
  public static final String DATA_CTX_AUTHENTICATION_RESULT = "authenticationResult";

  private final PlannerToolbox plannerToolbox;
  private final ExpressionParser expressionParser;
  private final String sql;
  private final PlannerConfig plannerConfig;
  private final DateTime localNow;
  private final SqlEngine engine;
  private final Map<String, Object> queryContext;
  private final String sqlQueryId;
  private final boolean stringifyArrays;
  private final boolean useBoundsAndSelectors;
  private final boolean pullUpLookup;
  private final boolean reverseLookup;
  private final CopyOnWriteArrayList<String> nativeQueryIds = new CopyOnWriteArrayList<>();
  private final PlannerHook hook;
  // bindings for dynamic parameters to bind during planning
  private List<TypedValue> parameters = Collections.emptyList();
  // result of authentication, providing identity to authorize set of resources produced by validation
  private AuthenticationResult authenticationResult;
  // set of datasources and views which must be authorized, initialized to null so we can detect if it has been set.
  private Set<ResourceAction> resourceActions;
  // result of authorizing set of resources against authentication identity
  private Access authorizationResult;
  // error messages encountered while planning the query
  @Nullable
  private String planningError;
  private QueryMaker queryMaker;
  private VirtualColumnRegistry joinExpressionVirtualColumnRegistry;
  // set of attributes for a SQL statement used in the EXPLAIN PLAN output
  private ExplainAttributes explainAttributes;
  private PlannerLookupCache lookupCache;

  private PlannerContext(
      final PlannerToolbox plannerToolbox,
      final String sql,
      final PlannerConfig plannerConfig,
      final DateTime localNow,
      final boolean stringifyArrays,
      final boolean useBoundsAndSelectors,
      final boolean pullUpLookup,
      final boolean reverseLookup,
      final SqlEngine engine,
      final Map<String, Object> queryContext,
      final PlannerHook hook
  )
  {
    this.plannerToolbox = plannerToolbox;
    this.expressionParser = new ExpressionParserImpl(plannerToolbox.exprMacroTable());
    this.sql = sql;
    this.plannerConfig = Preconditions.checkNotNull(plannerConfig, "plannerConfig");
    this.engine = engine;
    this.queryContext = queryContext;
    this.localNow = Preconditions.checkNotNull(localNow, "localNow");
    this.stringifyArrays = stringifyArrays;
    this.useBoundsAndSelectors = useBoundsAndSelectors;
    this.pullUpLookup = pullUpLookup;
    this.reverseLookup = reverseLookup;
    this.hook = hook == null ? NoOpPlannerHook.INSTANCE : hook;

    String sqlQueryId = (String) this.queryContext.get(QueryContexts.CTX_SQL_QUERY_ID);
    // special handling for DruidViewMacro, normal client will allocate sqlid in SqlLifecyle
    if (Strings.isNullOrEmpty(sqlQueryId)) {
      sqlQueryId = UUID.randomUUID().toString();
    }
    this.sqlQueryId = sqlQueryId;
  }

  public static PlannerContext create(
      final PlannerToolbox plannerToolbox,
      final String sql,
      final SqlEngine engine,
      final Map<String, Object> queryContext,
      final PlannerHook hook
  )
  {
    final DateTime utcNow;
    final DateTimeZone timeZone;
    final boolean stringifyArrays;
    final boolean useBoundsAndSelectors;
    final boolean pullUpLookup;
    final boolean reverseLookup;

    final Object stringifyParam = queryContext.get(QueryContexts.CTX_SQL_STRINGIFY_ARRAYS);
    final Object tsParam = queryContext.get(CTX_SQL_CURRENT_TIMESTAMP);
    final Object tzParam = queryContext.get(CTX_SQL_TIME_ZONE);
    final Object useBoundsAndSelectorsParam = queryContext.get(CTX_SQL_USE_BOUNDS_AND_SELECTORS);
    final Object pullUpLookupParam = queryContext.get(CTX_SQL_PULL_UP_LOOKUP);
    final Object reverseLookupParam = queryContext.get(CTX_SQL_REVERSE_LOOKUP);

    if (tsParam != null) {
      utcNow = new DateTime(tsParam, DateTimeZone.UTC);
    } else {
      utcNow = new DateTime(DateTimeZone.UTC);
    }

    if (tzParam != null) {
      timeZone = DateTimes.inferTzFromString(String.valueOf(tzParam));
    } else {
      timeZone = plannerToolbox.plannerConfig().getSqlTimeZone();
    }

    if (stringifyParam != null) {
      stringifyArrays = Numbers.parseBoolean(stringifyParam);
    } else {
      stringifyArrays = true;
    }

    if (useBoundsAndSelectorsParam != null) {
      useBoundsAndSelectors = Numbers.parseBoolean(useBoundsAndSelectorsParam);
    } else {
      useBoundsAndSelectors = DEFAULT_SQL_USE_BOUNDS_AND_SELECTORS;
    }

    if (pullUpLookupParam != null) {
      pullUpLookup = Numbers.parseBoolean(pullUpLookupParam);
    } else {
      pullUpLookup = DEFAULT_SQL_PULL_UP_LOOKUP;
    }

    if (reverseLookupParam != null) {
      reverseLookup = Numbers.parseBoolean(reverseLookupParam);
    } else {
      reverseLookup = DEFAULT_SQL_REVERSE_LOOKUP;
    }

    return new PlannerContext(
        plannerToolbox,
        sql,
        plannerToolbox.plannerConfig().withOverrides(queryContext),
        utcNow.withZone(timeZone),
        stringifyArrays,
        useBoundsAndSelectors,
        pullUpLookup,
        reverseLookup,
        engine,
        queryContext,
        hook
    );
  }

  /**
   * Returns the join algorithm specified in a query context.
   */
  public static JoinAlgorithm getJoinAlgorithm(QueryContext queryContext)
  {
    return getJoinAlgorithmFromContextValue(queryContext.get(CTX_SQL_JOIN_ALGORITHM));
  }

  /**
   * Returns the join algorithm specified in a query context.
   */
  public static JoinAlgorithm getJoinAlgorithm(Map<String, Object> queryContext)
  {
    return getJoinAlgorithmFromContextValue(queryContext.get(CTX_SQL_JOIN_ALGORITHM));
  }

  private static JoinAlgorithm getJoinAlgorithmFromContextValue(final Object object)
  {
    final String s = QueryContexts.getAsString(
        CTX_SQL_JOIN_ALGORITHM,
        object,
        DEFAULT_SQL_JOIN_ALGORITHM.toString()
    );

    try {
      return JoinAlgorithm.fromString(s);
    }
    catch (IllegalArgumentException e) {
      throw QueryContexts.badValueException(
          CTX_SQL_JOIN_ALGORITHM,
          StringUtils.format("one of %s", Arrays.toString(JoinAlgorithm.values())),
          object
      );
    }
  }

  public PlannerToolbox getPlannerToolbox()
  {
    return plannerToolbox;
  }

  public ExprMacroTable getExprMacroTable()
  {
    return plannerToolbox.exprMacroTable();
  }

  public ExpressionParser getExpressionParser()
  {
    return expressionParser;
  }


  /**
   * Equivalent to {@link ExpressionParser#parse(String)} on {@link #getExpressionParser()}.
   */
  public Expr parseExpression(final String expr)
  {
    return expressionParser.parse(expr);
  }

  // Deprecated: prefer using the toolbox
  public ObjectMapper getJsonMapper()
  {
    return plannerToolbox.jsonMapper();
  }

  public PlannerConfig getPlannerConfig()
  {
    return plannerConfig;
  }

  public DateTime getLocalNow()
  {
    return localNow;
  }

  public DateTimeZone getTimeZone()
  {
    return localNow.getZone();
  }

  public JoinableFactoryWrapper getJoinableFactoryWrapper()
  {
    return plannerToolbox.joinableFactoryWrapper();
  }

  @Nullable
  public String getSchemaResourceType(String schema, String resourceName)
  {
    return plannerToolbox.rootSchema().getResourceType(schema, resourceName);
  }

  /**
   * Return the query context as a mutable map. Use this form when
   * modifying the context during planning.
   */
  public Map<String, Object> queryContextMap()
  {
    return queryContext;
  }

  /**
   * Return the query context as an immutable object. Use this form
   * when querying the context as it provides type-safe accessors.
   */
  public QueryContext queryContext()
  {
    return QueryContext.of(queryContext);
  }

  public boolean isStringifyArrays()
  {
    return stringifyArrays;
  }

  /**
   * Whether we should use {@link org.apache.druid.query.filter.BoundDimFilter} and
   * {@link org.apache.druid.query.filter.SelectorDimFilter} (true) or {@link org.apache.druid.query.filter.RangeFilter},
   * {@link org.apache.druid.query.filter.EqualityFilter}, and {@link org.apache.druid.query.filter.NullFilter} (false).
   *
   * Typically true when {@link NullHandling#replaceWithDefault()} and false when {@link NullHandling#sqlCompatible()}.
   * Can be overriden by the undocumented context parameter {@link #CTX_SQL_USE_BOUNDS_AND_SELECTORS}.
   */
  public boolean isUseBoundsAndSelectors()
  {
    return useBoundsAndSelectors;
  }

  /**
   * Whether we should use {@link AggregatePullUpLookupRule} to pull LOOKUP functions on injective lookups up above
   * a GROUP BY.
   */
  public boolean isPullUpLookup()
  {
    return pullUpLookup;
  }

  /**
   * Whether we should use {@link ReverseLookupRule} to reduce the LOOKUP function, and whether we should set the
   * "optimize" flag on {@link RegisteredLookupExtractionFn}.
   */
  public boolean isReverseLookup()
  {
    return reverseLookup;
  }

  public List<TypedValue> getParameters()
  {
    return parameters;
  }

  public AuthenticationResult getAuthenticationResult()
  {
    return Preconditions.checkNotNull(authenticationResult, "Authentication result not available");
  }

  public JoinAlgorithm getJoinAlgorithm()
  {
    return getJoinAlgorithm(queryContext);
  }

  public String getSql()
  {
    return sql;
  }

  public PlannerHook getPlannerHook()
  {
    return hook;
  }

  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

  public CopyOnWriteArrayList<String> getNativeQueryIds()
  {
    return nativeQueryIds;
  }

  public void addNativeQueryId(String queryId)
  {
    this.nativeQueryIds.add(queryId);
  }

  @Nullable
  public String getPlanningError()
  {
    return planningError;
  }

  /**
   * Sets the planning error in the context that will be shown to the user if the SQL query cannot be translated
   * to a native query. This error is often a hint and thus should be phrased as such. Also, the final plan can
   * be very different from SQL that user has written. So again, the error should be phrased to indicate this gap
   * clearly.
   */
  public void setPlanningError(String formatText, Object... arguments)
  {
    planningError = StringUtils.nonStrictFormat(formatText, arguments);
  }

  public DataContext createDataContext(final JavaTypeFactory typeFactory, List<TypedValue> parameters)
  {
    class DruidDataContext implements DataContext
    {
      private final Map<String, Object> base_context = ImmutableMap.of(
          DataContext.Variable.UTC_TIMESTAMP.camelName, localNow.getMillis(),
          DataContext.Variable.CURRENT_TIMESTAMP.camelName, localNow.getMillis(),
          DataContext.Variable.LOCAL_TIMESTAMP.camelName, new Interval(
              new DateTime("1970-01-01T00:00:00.000", localNow.getZone()),
              localNow
          ).toDurationMillis(),
          DataContext.Variable.TIME_ZONE.camelName, localNow.getZone().toTimeZone().clone()
      );
      private final Map<String, Object> context;

      DruidDataContext()
      {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.putAll(base_context);
        int i = 0;
        for (TypedValue parameter : parameters) {
          builder.put("?" + i, parameter.value);
          i++;
        }
        if (authenticationResult != null) {
          builder.put(DATA_CTX_AUTHENTICATION_RESULT, authenticationResult);
        }
        context = builder.build();
      }

      @Override
      public SchemaPlus getRootSchema()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public JavaTypeFactory getTypeFactory()
      {
        return typeFactory;
      }

      @Override
      public QueryProvider getQueryProvider()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object get(final String name)
      {
        return context.get(name);
      }
    }

    return new DruidDataContext();
  }


  public Access getAuthorizationResult()
  {
    return Preconditions.checkNotNull(authorizationResult, "Authorization result not available");
  }

  public void setParameters(List<TypedValue> parameters)
  {
    this.parameters = Preconditions.checkNotNull(parameters, "parameters");
  }

  public void setAuthenticationResult(AuthenticationResult authenticationResult)
  {
    if (this.authenticationResult != null) {
      // It's a bug if this happens, because setAuthenticationResult should be called exactly once.
      throw new ISE("Authentication result has already been set");
    }

    this.authenticationResult = Preconditions.checkNotNull(authenticationResult, "authenticationResult");
  }

  public void setAuthorizationResult(Access access)
  {
    if (this.authorizationResult != null) {
      // It's a bug if this happens, because setAuthorizationResult should be called exactly once.
      throw new ISE("Authorization result has already been set");
    }

    this.authorizationResult = Preconditions.checkNotNull(access, "authorizationResult");
  }

  public Set<ResourceAction> getResourceActions()
  {
    return Preconditions.checkNotNull(resourceActions, "Resources not available");
  }

  public void setResourceActions(Set<ResourceAction> resourceActions)
  {
    if (this.resourceActions != null) {
      // It's a bug if this happens, because setResourceActions should be called exactly once.
      throw new ISE("Resources have already been set");
    }

    this.resourceActions = Preconditions.checkNotNull(resourceActions, "resourceActions");
  }

  public void setQueryMaker(QueryMaker queryMaker)
  {
    if (this.queryMaker != null) {
      // It's a bug if this happens, because setQueryMaker should be called exactly once.
      throw new ISE("QueryMaker has already been set");
    }

    this.queryMaker = Preconditions.checkNotNull(queryMaker, "queryMaker");
  }

  public SqlEngine getEngine()
  {
    return engine;
  }

  /**
   * Checks if the current {@link SqlEngine} supports a particular feature.
   *
   * When executing a specific query, use this method instead of
   * {@link SqlEngine#featureAvailable(EngineFeature, PlannerContext)}, because it also verifies feature flags such as
   * {@link #CTX_ENABLE_WINDOW_FNS}.
   */
  public boolean featureAvailable(final EngineFeature feature)
  {
    if (feature == EngineFeature.WINDOW_FUNCTIONS &&
        !QueryContexts.getAsBoolean(CTX_ENABLE_WINDOW_FNS, queryContext.get(CTX_ENABLE_WINDOW_FNS), false)) {
      // Short-circuit: feature requires context flag.
      return false;
    }
    return engine.featureAvailable(feature, this);
  }

  public QueryMaker getQueryMaker()
  {
    return Preconditions.checkNotNull(queryMaker, "QueryMaker not available");
  }

  public VirtualColumnRegistry getJoinExpressionVirtualColumnRegistry()
  {
    return joinExpressionVirtualColumnRegistry;
  }

  public void setJoinExpressionVirtualColumnRegistry(VirtualColumnRegistry joinExpressionVirtualColumnRegistry)
  {
    this.joinExpressionVirtualColumnRegistry = joinExpressionVirtualColumnRegistry;
  }

  public ExplainAttributes getExplainAttributes()
  {
    return this.explainAttributes;
  }

  public void setExplainAttributes(ExplainAttributes explainAttributes)
  {
    if (this.explainAttributes != null) {
      throw new ISE("ExplainAttributes has already been set");
    }
    this.explainAttributes = explainAttributes;
  }

  /**
   * Retrieve a named {@link LookupExtractor}.
   */
  public LookupExtractor getLookup(final String lookupName)
  {
    if (lookupCache == null) {
      final SqlOperatorConversion lookupOperatorConversion =
          plannerToolbox.operatorTable().lookupOperatorConversion(QueryLookupOperatorConversion.SQL_FUNCTION);

      if (lookupOperatorConversion != null) {
        final LookupExtractorFactoryContainerProvider lookupProvider =
            ((QueryLookupOperatorConversion) lookupOperatorConversion).getLookupExtractorFactoryContainerProvider();
        lookupCache = new PlannerLookupCache(lookupProvider);
      } else {
        lookupCache = new PlannerLookupCache(null);
      }
    }

    return lookupCache.getLookup(lookupName);
  }
}
