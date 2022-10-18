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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.druid.annotations.UsedByJUnitParamsRunner;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.hll.VersionOneHyperLogLogCollector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.CascadeExtractionFn;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.PreparedStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.sql.http.SqlParameter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A base class for SQL query testing. It sets up query execution environment, provides useful helper methods,
 * and populates data using {@link CalciteTests#createMockWalker}.
 */
public class BaseCalciteQueryTest extends CalciteTestBase implements QueryComponentSupplier
{
  public static String NULL_STRING;
  public static Float NULL_FLOAT;
  public static Long NULL_LONG;
  public static final String HLLC_STRING = VersionOneHyperLogLogCollector.class.getName();

  @BeforeClass
  public static void setupNullValues()
  {
    NULL_STRING = NullHandling.defaultStringValue();
    NULL_FLOAT = NullHandling.defaultFloatValue();
    NULL_LONG = NullHandling.defaultLongValue();
  }

  public static final Logger log = new Logger(BaseCalciteQueryTest.class);

  public static final PlannerConfig PLANNER_CONFIG_DEFAULT = new PlannerConfig();
  public static final PlannerConfig PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE =
      PlannerConfig.builder().serializeComplexValues(false).build();

  public static final PlannerConfig PLANNER_CONFIG_REQUIRE_TIME_CONDITION =
      PlannerConfig.builder().requireTimeCondition(true).build();

  public static final PlannerConfig PLANNER_CONFIG_NO_TOPN =
      PlannerConfig.builder().maxTopNLimit(0).build();

  public static final PlannerConfig PLANNER_CONFIG_NO_HLL =
      PlannerConfig.builder().useApproximateCountDistinct(false).build();

  public static final String LOS_ANGELES = "America/Los_Angeles";
  public static final PlannerConfig PLANNER_CONFIG_LOS_ANGELES =
      PlannerConfig
          .builder()
          .sqlTimeZone(DateTimes.inferTzFromString(LOS_ANGELES))
          .build();

  public static final PlannerConfig PLANNER_CONFIG_AUTHORIZE_SYS_TABLES =
      PlannerConfig.builder().authorizeSystemTablesDirectly(true).build();

  public static final PlannerConfig PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN =
      PlannerConfig.builder().useNativeQueryExplain(true).build();

  public static final int MAX_NUM_IN_FILTERS = 100;
  public static final PlannerConfig PLANNER_CONFIG_MAX_NUMERIC_IN_FILTER =
      PlannerConfig.builder().maxNumericInFilters(MAX_NUM_IN_FILTERS).build();

  public static final String DUMMY_SQL_ID = "dummy";

  public static final String PRETEND_CURRENT_TIME = "2000-01-01T00:00:00Z";
  private static final ImmutableMap.Builder<String, Object> DEFAULT_QUERY_CONTEXT_BUILDER =
      ImmutableMap.<String, Object>builder()
                  .put(QueryContexts.CTX_SQL_QUERY_ID, DUMMY_SQL_ID)
                  .put(PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z")
                  .put(QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS)
                  .put(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE);
  public static final Map<String, Object> QUERY_CONTEXT_DEFAULT = DEFAULT_QUERY_CONTEXT_BUILDER.build();

  public static final Map<String, Object> QUERY_CONTEXT_NO_STRINGIFY_ARRAY =
      DEFAULT_QUERY_CONTEXT_BUILDER.put(QueryContexts.CTX_SQL_STRINGIFY_ARRAYS, false)
                                   .build();

  public static final Map<String, Object> QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS = ImmutableMap.of(
      QueryContexts.CTX_SQL_QUERY_ID, DUMMY_SQL_ID,
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, PRETEND_CURRENT_TIME,
      TimeseriesQuery.SKIP_EMPTY_BUCKETS, false,
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE
  );

  public static final Map<String, Object> QUERY_CONTEXT_DO_SKIP_EMPTY_BUCKETS = ImmutableMap.of(
      QueryContexts.CTX_SQL_QUERY_ID, DUMMY_SQL_ID,
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, PRETEND_CURRENT_TIME,
      TimeseriesQuery.SKIP_EMPTY_BUCKETS, true,
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE
  );

  public static final Map<String, Object> QUERY_CONTEXT_NO_TOPN = ImmutableMap.of(
      QueryContexts.CTX_SQL_QUERY_ID, DUMMY_SQL_ID,
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, PRETEND_CURRENT_TIME,
      PlannerConfig.CTX_KEY_USE_APPROXIMATE_TOPN, "false",
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE
  );

  public static final Map<String, Object> QUERY_CONTEXT_LOS_ANGELES = ImmutableMap.of(
      QueryContexts.CTX_SQL_QUERY_ID, DUMMY_SQL_ID,
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, PRETEND_CURRENT_TIME,
      PlannerContext.CTX_SQL_TIME_ZONE, LOS_ANGELES,
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE
  );

  // Matches QUERY_CONTEXT_DEFAULT
  public static final Map<String, Object> TIMESERIES_CONTEXT_BY_GRAN = ImmutableMap.of(
      QueryContexts.CTX_SQL_QUERY_ID, DUMMY_SQL_ID,
      PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, PRETEND_CURRENT_TIME,
      TimeseriesQuery.SKIP_EMPTY_BUCKETS, true,
      QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS,
      QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE
  );

  // Add additional context to the given context map for when the
  // timeseries query has timestamp_floor expression on the timestamp dimension
  public static Map<String, Object> getTimeseriesContextWithFloorTime(
      Map<String, Object> context,
      String timestampResultField
  )
  {
    return ImmutableMap.<String, Object>builder()
                       .putAll(context)
                       .put(TimeseriesQuery.CTX_TIMESTAMP_RESULT_FIELD, timestampResultField)
                       .build();
  }

  // Matches QUERY_CONTEXT_LOS_ANGELES
  public static final Map<String, Object> TIMESERIES_CONTEXT_LOS_ANGELES = new HashMap<>();

  public static final Map<String, Object> OUTER_LIMIT_CONTEXT = new HashMap<>(QUERY_CONTEXT_DEFAULT);

  public static int minTopNThreshold = TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD;

  @Nullable
  public final SqlEngine engine0;
  private static SqlTestFramework queryFramework;
  final boolean useDefault = NullHandling.replaceWithDefault();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  public boolean cannotVectorize = false;
  public boolean skipVectorize = false;

  public QueryLogHook queryLogHook;

  public QueryComponentSupplier baseComponentSupplier;

  public BaseCalciteQueryTest()
  {
    this(null);
  }

  public BaseCalciteQueryTest(@Nullable final SqlEngine engine)
  {
    this.engine0 = engine;
  }

  static {
    TIMESERIES_CONTEXT_LOS_ANGELES.put(QueryContexts.CTX_SQL_QUERY_ID, DUMMY_SQL_ID);
    TIMESERIES_CONTEXT_LOS_ANGELES.put(PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z");
    TIMESERIES_CONTEXT_LOS_ANGELES.put(PlannerContext.CTX_SQL_TIME_ZONE, LOS_ANGELES);
    TIMESERIES_CONTEXT_LOS_ANGELES.put(TimeseriesQuery.SKIP_EMPTY_BUCKETS, true);
    TIMESERIES_CONTEXT_LOS_ANGELES.put(QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS);
    TIMESERIES_CONTEXT_LOS_ANGELES.put(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE);

    OUTER_LIMIT_CONTEXT.put(PlannerContext.CTX_SQL_OUTER_LIMIT, 2);
  }

  // Generate timestamps for expected results
  public static long timestamp(final String timeString)
  {
    return Calcites.jodaToCalciteTimestamp(DateTimes.of(timeString), DateTimeZone.UTC);
  }

  // Generate timestamps for expected results
  public static long timestamp(final String timeString, final String timeZoneString)
  {
    final DateTimeZone timeZone = DateTimes.inferTzFromString(timeZoneString);
    return Calcites.jodaToCalciteTimestamp(new DateTime(timeString, timeZone), timeZone);
  }

  // Generate day numbers for expected results
  public static int day(final String dayString)
  {
    return (int) (Intervals.utc(timestamp("1970"), timestamp(dayString)).toDurationMillis() / (86400L * 1000L));
  }

  public static QuerySegmentSpec querySegmentSpec(final Interval... intervals)
  {
    return new MultipleIntervalSegmentSpec(Arrays.asList(intervals));
  }

  public static AndDimFilter and(DimFilter... filters)
  {
    return new AndDimFilter(Arrays.asList(filters));
  }

  public static OrDimFilter or(DimFilter... filters)
  {
    return new OrDimFilter(Arrays.asList(filters));
  }

  public static NotDimFilter not(DimFilter filter)
  {
    return new NotDimFilter(filter);
  }

  public static InDimFilter in(String dimension, List<String> values, ExtractionFn extractionFn)
  {
    return new InDimFilter(dimension, values, extractionFn);
  }

  public static SelectorDimFilter selector(final String fieldName, final String value, final ExtractionFn extractionFn)
  {
    return new SelectorDimFilter(fieldName, value, extractionFn);
  }

  public static ExpressionDimFilter expressionFilter(final String expression)
  {
    return new ExpressionDimFilter(expression, CalciteTests.createExprMacroTable());
  }

  public static DimFilter numericSelector(
      final String fieldName,
      final String value,
      final ExtractionFn extractionFn
  )
  {
    // We use Bound filters for numeric equality to achieve "10.0" = "10"
    return bound(fieldName, value, value, false, false, extractionFn, StringComparators.NUMERIC);
  }

  public static BoundDimFilter bound(
      final String fieldName,
      final String lower,
      final String upper,
      final boolean lowerStrict,
      final boolean upperStrict,
      final ExtractionFn extractionFn,
      final StringComparator comparator
  )
  {
    return new BoundDimFilter(fieldName, lower, upper, lowerStrict, upperStrict, null, extractionFn, comparator);
  }

  public static BoundDimFilter timeBound(final Object intervalObj)
  {
    final Interval interval = new Interval(intervalObj, ISOChronology.getInstanceUTC());
    return new BoundDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        String.valueOf(interval.getStartMillis()),
        String.valueOf(interval.getEndMillis()),
        false,
        true,
        null,
        null,
        StringComparators.NUMERIC
    );
  }

  public static CascadeExtractionFn cascade(final ExtractionFn... fns)
  {
    return new CascadeExtractionFn(fns);
  }

  public static List<DimensionSpec> dimensions(final DimensionSpec... dimensionSpecs)
  {
    return Arrays.asList(dimensionSpecs);
  }

  public static List<AggregatorFactory> aggregators(final AggregatorFactory... aggregators)
  {
    return Arrays.asList(aggregators);
  }

  public static DimFilterHavingSpec having(final DimFilter filter)
  {
    return new DimFilterHavingSpec(filter, true);
  }

  public static ExpressionVirtualColumn expressionVirtualColumn(
      final String name,
      final String expression,
      final ColumnType outputType
  )
  {
    return new ExpressionVirtualColumn(name, expression, outputType, CalciteTests.createExprMacroTable());
  }

  public static JoinDataSource join(
      DataSource left,
      DataSource right,
      String rightPrefix,
      String condition,
      JoinType joinType,
      DimFilter filter
  )
  {
    return JoinDataSource.create(
        left,
        right,
        rightPrefix,
        condition,
        joinType,
        filter,
        CalciteTests.createExprMacroTable()
    );
  }

  public static JoinDataSource join(
      DataSource left,
      DataSource right,
      String rightPrefix,
      String condition,
      JoinType joinType
  )
  {
    return join(left, right, rightPrefix, condition, joinType, null);
  }

  public static String equalsCondition(DruidExpression left, DruidExpression right)
  {
    return StringUtils.format("(%s == %s)", left.getExpression(), right.getExpression());
  }

  public static ExpressionPostAggregator expressionPostAgg(final String name, final String expression)
  {
    return new ExpressionPostAggregator(name, expression, null, CalciteTests.createExprMacroTable());
  }

  public static Druids.ScanQueryBuilder newScanQueryBuilder()
  {
    return new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .legacy(false);
  }

  @BeforeClass
  public static void setUpClass()
  {
    resetFramework();
  }

  @AfterClass
  public static void tearDownClass()
  {
    resetFramework();
  }

  protected static void resetFramework()
  {
    if (queryFramework != null) {
      queryFramework.close();
    }
    queryFramework = null;
  }

  @Rule
  public QueryLogHook getQueryLogHook()
  {
    return queryLogHook = QueryLogHook.create(queryFramework().queryJsonMapper());
  }

  public SqlTestFramework queryFramework()
  {
    if (queryFramework == null) {
      createFramework(0);
    }
    return queryFramework;
  }

  /**
   * Creates the query planning/execution framework. The logic is somewhat
   * round-about: the builder creates the structure, but delegates back to
   * this class for the parts that the Calcite tests customize. This class,
   * in turn, delegates back to a standard class to create components. However,
   * subclasses do override each method to customize components for specific
   * tests.
   */
  private void createFramework(int mergeBufferCount)
  {
    resetFramework();
    try {
      baseComponentSupplier = new StandardComponentSupplier(
          CalciteTests.INJECTOR,
          temporaryFolder.newFolder());
    }
    catch (IOException e) {
      throw new RE(e);
    }
    queryFramework = new SqlTestFramework.Builder(this)
        .minTopNThreshold(minTopNThreshold)
        .mergeBufferCount(mergeBufferCount)
        .build();
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate
  ) throws IOException
  {
    return baseComponentSupplier.createQuerySegmentWalker(conglomerate);
  }

  @Override
  public SqlEngine createEngine(
      QueryLifecycleFactory qlf,
      ObjectMapper queryJsonMapper
  )
  {
    if (engine0 == null) {
      return baseComponentSupplier.createEngine(qlf, queryJsonMapper);
    } else {
      return engine0;
    }
  }

  @Override
  public void configureJsonMapper(ObjectMapper mapper)
  {
    baseComponentSupplier.configureJsonMapper(mapper);
  }

  @Override
  public DruidOperatorTable createOperatorTable()
  {
    return baseComponentSupplier.createOperatorTable();
  }

  @Override
  public ExprMacroTable createMacroTable()
  {
    return baseComponentSupplier.createMacroTable();
  }

  @Override
  public Map<String, Object> getJacksonInjectables()
  {
    return baseComponentSupplier.getJacksonInjectables();
  }

  @Override
  public Iterable<? extends Module> getJacksonModules()
  {
    return baseComponentSupplier.getJacksonModules();
  }

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
  }

  public void assertQueryIsUnplannable(final String sql, String expectedError)
  {
    assertQueryIsUnplannable(PLANNER_CONFIG_DEFAULT, sql, expectedError);
  }

  public void assertQueryIsUnplannable(final PlannerConfig plannerConfig, final String sql, String expectedError)
  {
    Exception e = null;
    try {
      testQuery(plannerConfig, sql, CalciteTests.REGULAR_USER_AUTH_RESULT, ImmutableList.of(), ImmutableList.of());
    }
    catch (Exception e1) {
      e = e1;
    }

    if (!(e instanceof RelOptPlanner.CannotPlanException)) {
      log.error(e, "Expected CannotPlanException for query: %s", sql);
      Assert.fail(sql);
    }
    Assert.assertEquals(
        sql,
        StringUtils.format("Query not supported. %s SQL was: %s", expectedError, sql),
        e.getMessage()
    );
  }

  /**
   * Provided for tests that wish to check multiple queries instead of relying on ExpectedException.
   */
  public void assertQueryIsForbidden(final String sql, final AuthenticationResult authenticationResult)
  {
    assertQueryIsForbidden(PLANNER_CONFIG_DEFAULT, sql, authenticationResult);
  }

  public void assertQueryIsForbidden(
      final PlannerConfig plannerConfig,
      final String sql,
      final AuthenticationResult authenticationResult
  )
  {
    Exception e = null;
    try {
      testQuery(plannerConfig, sql, authenticationResult, ImmutableList.of(), ImmutableList.of());
    }
    catch (Exception e1) {
      e = e1;
    }

    if (!(e instanceof ForbiddenException)) {
      log.error(e, "Expected ForbiddenException for query: %s with authResult: %s", sql, authenticationResult);
      Assert.fail(sql);
    }
  }

  public void testQuery(
      final String sql,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults
  )
  {
    testBuilder()
        .sql(sql)
        .expectedQueries(expectedQueries)
        .expectedResults(expectedResults)
        .run();
  }

  public void testQuery(
      final String sql,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults,
      final RowSignature expectedResultSignature
  )
  {
    testBuilder()
        .sql(sql)
        .expectedQueries(expectedQueries)
        .expectedResults(expectedResults)
        .expectedSignature(expectedResultSignature)
        .run();
  }

  public void testQuery(
      final String sql,
      final Map<String, Object> queryContext,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults
  )
  {
    testBuilder()
        .queryContext(queryContext)
        .sql(sql)
        .expectedQueries(expectedQueries)
        .expectedResults(expectedResults)
        .run();
  }

  public void testQuery(
      final String sql,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults,
      final List<SqlParameter> parameters
  )
  {
    testBuilder()
        .sql(sql)
        .parameters(parameters)
        .expectedQueries(expectedQueries)
        .expectedResults(expectedResults)
        .run();
  }

  public void testQuery(
      final PlannerConfig plannerConfig,
      final String sql,
      final AuthenticationResult authenticationResult,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults
  )
  {
    testBuilder()
        .plannerConfig(plannerConfig)
        .sql(sql)
        .authResult(authenticationResult)
        .expectedQueries(expectedQueries)
        .expectedResults(expectedResults)
        .run();
  }

  public void testQuery(
      final String sql,
      final Map<String, Object> queryContext,
      final List<Query<?>> expectedQueries,
      final ResultsVerifier expectedResultsVerifier
  )
  {
    testBuilder()
        .sql(sql)
        .queryContext(queryContext)
        .expectedQueries(expectedQueries)
        .expectedResults(expectedResultsVerifier)
        .run();
  }

  public void testQuery(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final String sql,
      final AuthenticationResult authenticationResult,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults
  )
  {
    testBuilder()
        .plannerConfig(plannerConfig)
        .queryContext(queryContext)
        .sql(sql)
        .authResult(authenticationResult)
        .expectedQueries(expectedQueries)
        .expectedResults(expectedResults)
        .run();
  }

  public void testQuery(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final List<SqlParameter> parameters,
      final String sql,
      final AuthenticationResult authenticationResult,
      final List<Query<?>> expectedQueries,
      final List<Object[]> expectedResults
  )
  {
    testBuilder()
        .plannerConfig(plannerConfig)
        .queryContext(queryContext)
        .parameters(parameters)
        .sql(sql)
        .authResult(authenticationResult)
        .expectedQueries(expectedQueries)
        .expectedResults(expectedResults)
        .run();
  }

  public void testQuery(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final List<SqlParameter> parameters,
      final String sql,
      final AuthenticationResult authenticationResult,
      final List<Query<?>> expectedQueries,
      final ResultsVerifier expectedResultsVerifier,
      @Nullable final Consumer<ExpectedException> expectedExceptionInitializer
  )
  {
    testBuilder()
        .plannerConfig(plannerConfig)
        .queryContext(queryContext)
        .parameters(parameters)
        .sql(sql)
        .authResult(authenticationResult)
        .expectedQueries(expectedQueries)
        .expectedResults(expectedResultsVerifier)
        .expectedException(expectedExceptionInitializer)
        .run();
  }

  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig())
        .cannotVectorize(cannotVectorize)
        .skipVectorize(skipVectorize);
  }

  public class CalciteTestConfig implements QueryTestBuilder.QueryTestConfig
  {
    @Override
    public QueryTestRunner analyze(QueryTestBuilder builder)
    {
      if (builder.expectedResultsVerifier == null && builder.expectedResults != null) {
        builder.expectedResultsVerifier = defaultResultsVerifier(
            builder.expectedResults,
            builder.expectedResultSignature
        );
      }
      final List<QueryTestRunner.QueryRunStep> runSteps = new ArrayList<>();
      final List<QueryTestRunner.QueryVerifyStep> verifySteps = new ArrayList<>();

      // Historically, a test either prepares the query (to check resources), or
      // runs the query (to check the native query and results.) In the future we
      // may want to do both in a single test; but we have no such tests today.
      if (builder.expectedResources != null) {
        Preconditions.checkArgument(
            builder.expectedResultsVerifier == null,
            "Cannot check both results and resources"
        );
        QueryTestRunner.PrepareQuery execStep = new QueryTestRunner.PrepareQuery(builder);
        runSteps.add(execStep);
        verifySteps.add(new QueryTestRunner.VerifyResources(execStep));
      } else {
        QueryTestRunner.ExecuteQuery execStep = new QueryTestRunner.ExecuteQuery(builder);
        runSteps.add(execStep);

        // Verify native queries before results. (Note: change from prior pattern
        // that reversed the steps.
        if (builder.expectedQueries != null) {
          verifySteps.add(new QueryTestRunner.VerifyNativeQueries(execStep));
        }
        if (builder.expectedResultsVerifier != null) {
          verifySteps.add(new QueryTestRunner.VerifyResults(execStep));
        }

        // The exception is always verified: either there should be no exception
        // (the other steps ran), or there should be the defined exception.
        verifySteps.add(new QueryTestRunner.VerifyExpectedException(execStep));
      }
      return new QueryTestRunner(runSteps, verifySteps);
    }

    @Override
    public QueryLogHook queryLogHook()
    {
      return queryLogHook;
    }

    @Override
    public ExpectedException expectedException()
    {
      return expectedException;
    }

    @Override
    public SqlStatementFactory statementFactory(PlannerConfig plannerConfig, AuthConfig authConfig)
    {
      return getSqlStatementFactory(plannerConfig, authConfig);
    }

    @Override
    public ObjectMapper jsonMapper()
    {
      return queryFramework().queryJsonMapper();
    }
  }

  public Set<ResourceAction> analyzeResources(
      final SqlStatementFactory sqlStatementFactory,
      final SqlQueryPlus query
  )
  {
    PreparedStatement stmt = sqlStatementFactory.preparedStatement(query);
    stmt.prepare();
    return stmt.allResources();
  }

  public void assertResultsEquals(String sql, List<Object[]> expectedResults, List<Object[]> results)
  {
    for (int i = 0; i < results.size(); i++) {
      Assert.assertArrayEquals(
          StringUtils.format("result #%d: %s", i + 1, sql),
          expectedResults.get(i),
          results.get(i)
      );
    }
  }

  public void testQueryThrows(final String sql, Consumer<ExpectedException> expectedExceptionInitializer)
  {
    testBuilder()
        .sql(sql)
        .expectedException(expectedExceptionInitializer)
        .build()
        .run();
  }

  public void testQueryThrows(
      final String sql,
      final Map<String, Object> queryContext,
      final List<Query<?>> expectedQueries,
      final Consumer<ExpectedException> expectedExceptionInitializer
  )
  {
    testBuilder()
        .sql(sql)
        .queryContext(queryContext)
        .expectedQueries(expectedQueries)
        .expectedException(expectedExceptionInitializer)
        .build()
        .run();
  }

  public void analyzeResources(
      String sql,
      List<ResourceAction> expectedActions
  )
  {
    testBuilder()
        .sql(sql)
        .expectedResources(expectedActions)
        .run();
  }

  public void analyzeResources(
      PlannerConfig plannerConfig,
      String sql,
      AuthenticationResult authenticationResult,
      List<ResourceAction> expectedActions
  )
  {
    testBuilder()
        .plannerConfig(plannerConfig)
        .sql(sql)
        .authResult(authenticationResult)
        .expectedResources(expectedActions)
        .run();
  }

  public void analyzeResources(
      PlannerConfig plannerConfig,
      AuthConfig authConfig,
      String sql,
      Map<String, Object> contexts,
      AuthenticationResult authenticationResult,
      List<ResourceAction> expectedActions
  )
  {
    testBuilder()
        .plannerConfig(plannerConfig)
        .authConfig(authConfig)
        .sql(sql)
        .queryContext(contexts)
        .authResult(authenticationResult)
        .expectedResources(expectedActions)
        .run();
  }

  public SqlStatementFactory getSqlStatementFactory(
      PlannerConfig plannerConfig
  )
  {
    return getSqlStatementFactory(
        plannerConfig,
        new AuthConfig()
     );
  }

  /**
   * Build the statement factory, which also builds all the infrastructure
   * behind the factory by calling methods on this test class. As a result, each
   * factory is specific to one test and one planner config. This method can be
   * overridden to control the objects passed to the factory.
   */
  private SqlStatementFactory getSqlStatementFactory(
      PlannerConfig plannerConfig,
      AuthConfig authConfig
  )
  {
    return queryFramework().statementFactory(plannerConfig, authConfig);
  }

  protected void cannotVectorize()
  {
    cannotVectorize = true;
  }

  protected void skipVectorize()
  {
    skipVectorize = true;
  }

  protected static boolean isRewriteJoinToFilter(final Map<String, Object> queryContext)
  {
    return (boolean) queryContext.getOrDefault(
        QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY,
        QueryContexts.DEFAULT_ENABLE_REWRITE_JOIN_TO_FILTER
    );
  }

  /**
   * Override not just the outer query context, but also the contexts of all subqueries.
   */
  public static <T> Query<T> recursivelyOverrideContext(final Query<T> query, final Map<String, Object> context)
  {
    return query.withDataSource(recursivelyOverrideContext(query.getDataSource(), context))
                .withOverriddenContext(context);
  }

  /**
   * Override the contexts of all subqueries of a particular datasource.
   */
  private static DataSource recursivelyOverrideContext(final DataSource dataSource, final Map<String, Object> context)
  {
    if (dataSource instanceof QueryDataSource) {
      final Query<?> subquery = ((QueryDataSource) dataSource).getQuery();
      return new QueryDataSource(recursivelyOverrideContext(subquery, context));
    } else {
      return dataSource.withChildren(
          dataSource.getChildren()
                    .stream()
                    .map(ds -> recursivelyOverrideContext(ds, context))
                    .collect(Collectors.toList())
      );
    }
  }

  /**
   * This is a provider of query contexts that should be used by join tests.
   * It tests various configs that can be passed to join queries. All the configs provided by this provider should
   * have the join query engine return the same results.
   */
  public static class QueryContextForJoinProvider
  {
    @UsedByJUnitParamsRunner
    public static Object[] provideQueryContexts()
    {
      return new Object[]{
          // default behavior
          QUERY_CONTEXT_DEFAULT,
          // all rewrites enabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, true)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, true)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, true)
              .build(),
          // filter-on-value-column rewrites disabled, everything else enabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, false)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, true)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, true)
              .build(),
          // filter rewrites fully disabled, join-to-filter enabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, false)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, false)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, true)
              .build(),
          // filter rewrites disabled, but value column filters still set to true (it should be ignored and this should
          // behave the same as the previous context)
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, true)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, false)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, true)
              .build(),
          // filter rewrites fully enabled, join-to-filter disabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, true)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, true)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, false)
              .build(),
          // all rewrites disabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, false)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, false)
              .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, false)
              .build(),
          };
    }
  }

  protected Map<String, Object> withLeftDirectAccessEnabled(Map<String, Object> context)
  {
    // since context is usually immutable in tests, make a copy
    HashMap<String, Object> newContext = new HashMap<>(context);
    newContext.put(QueryContexts.SQL_JOIN_LEFT_SCAN_DIRECT, true);
    return newContext;
  }

  /**
   * Reset the conglomerate, walker, and engine with required number of merge buffers. Default value is 2.
   */
  protected void requireMergeBuffers(int numMergeBuffers)
  {
    createFramework(numMergeBuffers);
  }

  protected Map<String, Object> withTimestampResultContext(
      Map<String, Object> input,
      String timestampResultField,
      int timestampResultFieldIndex,
      Granularity granularity
  )
  {
    Map<String, Object> output = new HashMap<>(input);
    output.put(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD, timestampResultField);

    try {
      output.put(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_GRANULARITY, queryFramework().queryJsonMapper().writeValueAsString(granularity));
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    output.put(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_INDEX, timestampResultFieldIndex);
    return output;
  }

  @FunctionalInterface
  public interface ResultsVerifier
  {
    default void verifyRowSignature(RowSignature rowSignature)
    {
      // do nothing
    }

    void verify(String sql, List<Object[]> results);
  }

  private ResultsVerifier defaultResultsVerifier(
      final List<Object[]> expectedResults,
      final RowSignature expectedSignature
  )
  {
    return new DefaultResultsVerifier(expectedResults, expectedSignature);
  }

  public class DefaultResultsVerifier implements ResultsVerifier
  {
    protected final List<Object[]> expectedResults;
    @Nullable
    protected final RowSignature expectedResultRowSignature;

    public DefaultResultsVerifier(List<Object[]> expectedResults, RowSignature expectedSignature)
    {
      this.expectedResults = expectedResults;
      this.expectedResultRowSignature = expectedSignature;
    }

    @Override
    public void verifyRowSignature(RowSignature rowSignature)
    {
      if (expectedResultRowSignature != null) {
        Assert.assertEquals(expectedResultRowSignature, rowSignature);
      }
    }

    @Override
    public void verify(String sql, List<Object[]> results)
    {
      Assert.assertEquals(StringUtils.format("result count: %s", sql), expectedResults.size(), results.size());
      assertResultsEquals(sql, expectedResults, results);
    }
  }
}
