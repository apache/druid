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
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.CancelFlag;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameter;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.explain.ExplainAttributes;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.RegisteredLookupExtractionFn;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rule.AggregatePullUpLookupRule;
import org.apache.druid.sql.calcite.rule.ReverseLookupRule;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalogProvider;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.sql.hook.DruidHook.HookKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Like {@link PlannerConfig}, but that has static configuration and this class
 * contains dynamic, per-query configuration. Additional Druid-specific static
 * configuration resides in the {@link PlannerToolbox} class.
 */
public class PlannerContext
{
  /**
   * @deprecated Use {@link QueryContextParameters#SQL_CURRENT_TIMESTAMP} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SQL_CURRENT_TIMESTAMP = "sqlCurrentTimestamp";

  /**
   * @deprecated Use {@link QueryContextParameters#SQL_TIME_ZONE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SQL_TIME_ZONE = "sqlTimeZone";

  /**
   * @deprecated Use {@link QueryContextParameters#SQL_JOIN_ALGORITHM} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SQL_JOIN_ALGORITHM = "sqlJoinAlgorithm";

  /**
   * @deprecated Use {@link QueryContextParameters#SQL_OUTER_LIMIT} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SQL_OUTER_LIMIT = "sqlOuterLimit";

  /**
   * @deprecated Use {@link QueryContextParameters#ENABLE_RAC_OVER_WIRE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_ENABLE_RAC_TRANSFER_OVER_WIRE = "enableRACOverWire";

  /**
   * @deprecated Use {@link QueryContextParameters#SQL_USE_BOUND_AND_SELECTORS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SQL_USE_BOUNDS_AND_SELECTORS = "sqlUseBoundAndSelectors";

  /**
   * @deprecated Use the declared default of {@link QueryContextParameters#SQL_USE_BOUND_AND_SELECTORS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_SQL_USE_BOUNDS_AND_SELECTORS = false;

  /**
   * @deprecated Use {@link QueryContextParameters#SQL_USE_EXTRACTION_FNS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SQL_USE_EXTRACTION_FNS = "sqlUseExtractionFns";

  /**
   * @deprecated Use the declared default of {@link QueryContextParameters#SQL_USE_EXTRACTION_FNS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_SQL_USE_EXTRACTION_FNS = false;

  /**
   * @deprecated Use {@link QueryContextParameters#SQL_PULL_UP_LOOKUP} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SQL_PULL_UP_LOOKUP = "sqlPullUpLookup";

  /**
   * @deprecated Use the declared default of {@link QueryContextParameters#SQL_PULL_UP_LOOKUP} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_SQL_PULL_UP_LOOKUP = true;

  /**
   * @deprecated Use {@link QueryContextParameters#SQL_REVERSE_LOOKUP} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SQL_REVERSE_LOOKUP = "sqlReverseLookup";

  /**
   * @deprecated Use the declared default of {@link QueryContextParameters#SQL_REVERSE_LOOKUP} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_SQL_REVERSE_LOOKUP = true;

  /**
   * @deprecated Use {@link QueryContextParameters#SQL_USE_GRANULARITY} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SQL_USE_GRANULARITY = "sqlUseGranularity";

  /**
   * @deprecated Use the declared default of {@link QueryContextParameters#SQL_USE_GRANULARITY} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_SQL_USE_GRANULARITY = true;

  /**
   * Returns the join algorithm specified in a query context.
   *
   * @deprecated Use {@link QueryContext#getOrDefault(QueryContextParameter)} with
   * {@link QueryContextParameters#SQL_JOIN_ALGORITHM} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static JoinAlgorithm getJoinAlgorithm(QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.SQL_JOIN_ALGORITHM);
  }

  /**
   * Returns the join algorithm specified in a query context.
   *
   * @deprecated Use {@link QueryContext#getOrDefault(QueryContextParameter)} with
   * {@link QueryContextParameters#SQL_JOIN_ALGORITHM} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static JoinAlgorithm getJoinAlgorithm(Map<String, Object> queryContext)
  {
    return getJoinAlgorithm(QueryContext.of(queryContext));
  }

  // DataContext keys
  public static final String DATA_CTX_AUTHENTICATION_RESULT = "authenticationResult";

  private final PlannerToolbox plannerToolbox;
  private final ExpressionParser expressionParser;
  private final String sql;
  private final SqlNode sqlNode;
  private final SqlEngine engine;
  private final DruidSchemaCatalog rootSchema;
  private final AuthenticationResult authenticationResult;
  private final Set<String> authContextKeys;
  private final Map<String, Object> queryContext;
  private final CopyOnWriteArrayList<String> nativeQueryIds = new CopyOnWriteArrayList<>();
  private final PlannerHook hook;
  private final Set<String> lookupsToLoad = new HashSet<>();

  /**
   * Cancel flag inherited by any {@link PlannerContext} created on this thread while set, so nested view-expansion
   * planners share the top-level query's {@link #cancelFlag}. Managed by {@link #withInheritedCancelFlag}.
   */
  private static final ThreadLocal<CancelFlag> INHERITED_CANCEL_FLAG = new ThreadLocal<>();

  /**
   * Calcite cancellation flag, wired into the planner via the framework {@link org.apache.calcite.plan.Context} in
   * {@link PlannerFactory}; tripping it enforces the planning timeout ({@link PlannerConfig#getMaxPlanningTimeMs()}).
   */
  private final CancelFlag cancelFlag = inheritedOrNewCancelFlag();

  private PlannerConfig plannerConfig;
  /**
   * Root schema created with {@link DruidSchemaCatalogProvider#createEscalatedRootSchema()}, for use in
   * view expansion. See {@link ViewManager} for details on the security model.
   */
  private DruidSchemaCatalog escalatedRootSchema;
  private String sqlQueryId;
  private boolean stringifyArrays;
  private boolean useBoundsAndSelectors;
  private boolean useExtractionFns;
  private boolean pullUpLookup;
  private boolean reverseLookup;
  private boolean useGranularity;
  @Nullable
  private List<Interval> timeCondition;
  private DateTime localNow;

  // bindings for dynamic parameters to bind during planning
  private List<TypedValue> parameters = Collections.emptyList();
  // set of datasources and views which must be authorized, initialized to null so we can detect if it has been set.
  private Set<ResourceAction> resourceActions;
  // result of authorizing set of resources against authentication identity
  private AuthorizationResult authorizationResult;
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
      final SqlNode sqlNode,
      final SqlEngine engine,
      final DruidSchemaCatalog rootSchema,
      final AuthenticationResult authenticationResult,
      final Set<String> authContextKeys,
      final Map<String, Object> queryContext,
      final PlannerHook hook
  )
  {
    this.plannerToolbox = plannerToolbox;
    this.expressionParser = new ExpressionParserImpl(plannerToolbox.exprMacroTable());
    this.sql = sql;
    this.sqlNode = sqlNode;
    this.engine = engine;
    this.rootSchema = rootSchema;
    this.authenticationResult = authenticationResult;
    this.authContextKeys = authContextKeys;
    this.queryContext = new LinkedHashMap<>(queryContext);
    this.hook = hook == null ? NoOpPlannerHook.INSTANCE : hook;
    initializeContextFieldsAndPlannerConfig();
  }

  public static PlannerContext create(
      final PlannerToolbox plannerToolbox,
      final String sql,
      final SqlNode sqlNode,
      final SqlEngine engine,
      final AuthenticationResult authenticationResult,
      final Set<String> authContextKeys,
      final Map<String, Object> queryContext,
      final PlannerHook hook
  )
  {
    return new PlannerContext(
        plannerToolbox,
        sql,
        sqlNode,
        engine,
        plannerToolbox.rootSchemaProvider.createRootSchema(authenticationResult),
        authenticationResult,
        authContextKeys,
        queryContext,
        hook
    );
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

  public CancelFlag getCancelFlag()
  {
    return cancelFlag;
  }

  private static CancelFlag inheritedOrNewCancelFlag()
  {
    final CancelFlag inherited = INHERITED_CANCEL_FLAG.get();
    return inherited != null ? inherited : new CancelFlag(new AtomicBoolean(false));
  }

  /**
   * Runs {@code action} with {@code cancelFlag} installed as the flag that {@link PlannerContext}s created on this
   * thread inherit, so one planning timeout governs nested view-expansion planners. Restores the prior value after.
   */
  public static <T> T withInheritedCancelFlag(final CancelFlag cancelFlag, final Supplier<T> action)
  {
    final CancelFlag previous = INHERITED_CANCEL_FLAG.get();
    INHERITED_CANCEL_FLAG.set(cancelFlag);
    try {
      return action.get();
    }
    finally {
      if (previous != null) {
        INHERITED_CANCEL_FLAG.set(previous);
      } else {
        INHERITED_CANCEL_FLAG.remove();
      }
    }
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

  /**
   * Returns the {@link Table} object corresponding to a Druid table/datasource, or null if none exists.
   */
  @Nullable
  public Table getDruidTable(String tableName)
  {
    return rootSchema.getSubSchema(plannerToolbox.druidSchemaName()).tables().get(tableName);
  }

  /**
   * Returns the root schema created with {@link DruidSchemaCatalogProvider#createRootSchema(AuthenticationResult)},
   * scoped for {@link #getAuthenticationResult()}.
   */
  public DruidSchemaCatalog getRootSchema()
  {
    return rootSchema;
  }

  /**
   * Returns the root schema created with {@link DruidSchemaCatalogProvider#createEscalatedRootSchema()},
   * for use in view expansion. See {@link ViewManager} for details on the security model.
   */
  public DruidSchemaCatalog getEscalatedRootSchema()
  {
    if (escalatedRootSchema == null) {
      escalatedRootSchema = plannerToolbox.rootSchemaProvider.createEscalatedRootSchema();
    }
    return escalatedRootSchema;
  }

  /**
   * Returns the {@link Resource} to authorize against for a particular resource (e.g. table, view) located in a
   * particular schema. If null, there is no authorization associated with the named resource (all authenticated
   * users may access it).
   */
  @Nullable
  public Resource getSchemaResource(String schema, String resourceName)
  {
    return rootSchema.getResource(schema, resourceName);
  }

  /**
   * Adds the given lookup name to the lookup loading spec.
   */
  public void addLookupToLoad(String lookupName)
  {
    lookupsToLoad.add(lookupName);
  }

  /**
   * Lookup loading spec used if this context corresponds to an MSQ task.
   */
  public LookupLoadingSpec getLookupLoadingSpec()
  {
    return lookupsToLoad.isEmpty() ? LookupLoadingSpec.NONE : LookupLoadingSpec.loadOnly(lookupsToLoad);
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
   * Return an immutable set of context keys need to be authorization checked, this usually comes from user-provided query context, including SET parameters.
   */
  public Set<String> authContextKeys()
  {
    return authContextKeys;
  }

  /**
   * Return the query context as an immutable object. Use this form
   * when querying the context as it provides type-safe accessors.
   */
  public QueryContext queryContext()
  {
    return QueryContext.of(queryContext);
  }

  public boolean useDruidEngineForSystemTables()
  {
    return queryContext().getOrDefault(QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES);
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
   * Can be overridden by the context parameter {@link QueryContextParameters#SQL_USE_BOUND_AND_SELECTORS}.
   */
  public boolean isUseBoundsAndSelectors()
  {
    return useBoundsAndSelectors;
  }

  /**
   * Whether we should use {@link ExtractionFn} in planning.
   */
  public boolean isUseExtractionFns()
  {
    return useExtractionFns;
  }

  /**
   * Whether we should use {@link InDimFilter} (true) or {@link TypedInFilter} (false).
   */
  public boolean isUseLegacyInFilter()
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

  /**
   * Whether we should use granularities other than {@link Granularities#ALL} when planning queries. This is provided
   * mainly so it can be set to false when running SQL queries on non-time-ordered tables, which do not support
   * any other granularities.
   */
  public boolean isUseGranularity()
  {
    return useGranularity;
  }

  /**
   * Returns intervals that should be applied (via AND) to table reads. The returned list is sorted and
   * non-overlapping.
   */
  @Nullable
  public List<Interval> getTimeCondition()
  {
    return timeCondition;
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
    return QueryContext.of(queryContext).getOrDefault(QueryContextParameters.SQL_JOIN_ALGORITHM);
  }

  public String getSql()
  {
    return sql;
  }

  public SqlNode getSqlNode()
  {
    return sqlNode;
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
    if (queryContext().isDecoupledMode()) {
      throw InvalidSqlInput.exception(formatText, arguments);
    }
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


  public AuthorizationResult getAuthorizationResult()
  {
    return Preconditions.checkNotNull(authorizationResult, "Authorization result not available");
  }

  public void setParameters(List<TypedValue> parameters)
  {
    this.parameters = Preconditions.checkNotNull(parameters, "parameters");
  }

  public void setAuthorizationResult(AuthorizationResult access)
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

  /**
   * Add additional query context parameters, overriding any existing values.
   */
  public void addAllToQueryContext(Map<String, Object> toAdd)
  {
    this.queryContext.putAll(toAdd);
    initializeContextFieldsAndPlannerConfig();
  }

  public SqlEngine getEngine()
  {
    return engine;
  }

  /**
   * Checks if the current {@link SqlEngine} supports a particular feature.
   * <p>
   * When executing a specific query, use this method instead of {@link SqlEngine#featureAvailable(EngineFeature)}
   * because it also verifies feature flags.
   */
  public boolean featureAvailable(final EngineFeature feature)
  {
    if (feature == EngineFeature.TIME_BOUNDARY_QUERY && !queryContext().isTimeBoundaryPlanningEnabled()) {
      // Short-circuit: feature requires context flag.
      return false;
    }
    return engine.featureAvailable(feature);
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

  public <T> void dispatchHook(HookKey<T> key, T object)
  {
    plannerToolbox.getHookDispatcher().dispatch(key, object);
  }



  private void initializeContextFieldsAndPlannerConfig()
  {
    final QueryContext context = QueryContext.of(queryContext);
    final String tsParam = context.get(QueryContextParameters.SQL_CURRENT_TIMESTAMP);
    final DateTime utcNow = tsParam != null ? DateTimes.of(tsParam) : DateTimes.nowUtc();

    final String tzParam = context.get(QueryContextParameters.SQL_TIME_ZONE);
    final DateTimeZone timeZone;
    if (tzParam != null) {
      timeZone = DateTimes.inferTzFromString(tzParam);
    } else {
      timeZone = plannerToolbox.plannerConfig().getSqlTimeZone();
    }
    localNow = utcNow.withZone(timeZone);

    stringifyArrays = context.getOrDefault(QueryContextParameters.SQL_STRINGIFY_ARRAYS, true);
    useBoundsAndSelectors = context.getOrDefault(QueryContextParameters.SQL_USE_BOUND_AND_SELECTORS);
    useExtractionFns = context.getOrDefault(QueryContextParameters.SQL_USE_EXTRACTION_FNS);
    pullUpLookup = context.getOrDefault(QueryContextParameters.SQL_PULL_UP_LOOKUP);
    reverseLookup = context.getOrDefault(QueryContextParameters.SQL_REVERSE_LOOKUP);

    useGranularity = context.getOrDefault(QueryContextParameters.SQL_USE_GRANULARITY);

    timeCondition = parseTimeCondition(context.get(QueryContextParameters.SQL_TIME_CONDITION));

    sqlQueryId = context.get(QueryContextParameters.SQL_QUERY_ID);
    // special handling for DruidViewMacro, normal client will allocate sqlid in SqlLifecyle
    if (Strings.isNullOrEmpty(sqlQueryId)) {
      sqlQueryId = UUID.randomUUID().toString();
      QueryContextParameters.SQL_QUERY_ID.set(this.queryContext, sqlQueryId);
    }

    if (plannerConfig != null) {
      plannerConfig = plannerConfig.withOverrides(queryContext);
    } else {
      plannerConfig = getPlannerToolbox().plannerConfig.withOverrides(queryContext);
    }
  }

  /**
   * Parses the value of {@link QueryContextParameters#SQL_TIME_CONDITION}, a comma-separated string of ISO8601
   * intervals. Returns null if the value is null. Throws an error if the string is empty or if there are empty items
   * after splitting on commas. The returned intervals are condensed and sorted.
   */
  @Nullable
  static List<Interval> parseTimeCondition(@Nullable final String value)
  {
    if (value == null) {
      return null;
    }

    final List<Interval> intervals = new ArrayList<>();
    for (final String intervalString : value.split(",", -1)) {
      try {
        intervals.add(Intervals.of(intervalString.trim()));
      }
      catch (DruidException e) {
        throw InvalidInput.exception(
            e,
            "Query context parameter [%s] must be a comma-separated string of ISO8601 intervals, but got [%s]",
            QueryContextParameters.SQL_TIME_CONDITION,
            value
        );
      }
    }

    return JodaUtils.condenseIntervals(intervals);
  }
}
