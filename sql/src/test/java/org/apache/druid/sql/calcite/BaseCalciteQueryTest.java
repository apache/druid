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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.druid.annotations.UsedByJUnitParamsRunner;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.hll.VersionOneHyperLogLogCollector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.Evals;
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
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.IsTrueDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.QueryTestRunner.QueryResults;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.ExpressionTestHelper;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.rule.ExtensionCalciteRuleProvider;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaManager;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.Builder;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerFixture;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardPlannerComponentSupplier;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.sql.http.SqlParameter;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * A base class for SQL query testing. It sets up query execution environment, provides useful helper methods,
 * and populates data using {@link CalciteTests#createMockWalker}.
 */
public class BaseCalciteQueryTest extends CalciteTestBase
    implements QueryComponentSupplier, PlannerComponentSupplier
{
  public static final double ASSERTION_EPSILON = 1e-5;
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

  public static final PlannerConfig PLANNER_CONFIG_LEGACY_QUERY_EXPLAIN =
      PlannerConfig.builder().useNativeQueryExplain(false).build();

  public static final PlannerConfig PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN =
      PlannerConfig.builder().useNativeQueryExplain(true).build();

  public static final int MAX_NUM_IN_FILTERS = 100;
  public static final PlannerConfig PLANNER_CONFIG_MAX_NUMERIC_IN_FILTER =
      PlannerConfig.builder().maxNumericInFilters(MAX_NUM_IN_FILTERS).build();

  public static final String DUMMY_SQL_ID = "dummy";

  public static final String PRETEND_CURRENT_TIME = "2000-01-01T00:00:00Z";

  public static final Map<String, Object> QUERY_CONTEXT_DEFAULT =
      ImmutableMap.<String, Object>builder()
                  .put(QueryContexts.CTX_SQL_QUERY_ID, DUMMY_SQL_ID)
                  .put(PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00Z")
                  .put(QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS)
                  .put(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE)
                  .build();

  public static final Map<String, Object> QUERY_CONTEXT_NO_STRINGIFY_ARRAY =
      ImmutableMap.<String, Object>builder()
                  .putAll(QUERY_CONTEXT_DEFAULT)
                  .put(QueryContexts.CTX_SQL_STRINGIFY_ARRAYS, false)
                  .build();

  public static final Map<String, Object> QUERY_CONTEXT_NO_STRINGIFY_ARRAY_USE_EQUALITY =
      ImmutableMap.<String, Object>builder()
                  .putAll(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                  .put(PlannerContext.CTX_SQL_USE_BOUNDS_AND_SELECTORS, false)
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

  public static final Map<String, Object> QUERY_CONTEXT_WITH_SUBQUERY_MEMORY_LIMIT =
      ImmutableMap.<String, Object>builder()
                  .putAll(QUERY_CONTEXT_DEFAULT)
                  .put(QueryContexts.MAX_SUBQUERY_BYTES_KEY, "100000")
                  .build();

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

  @Nullable
  public final SqlEngine engine0;
  final boolean useDefault = NullHandling.replaceWithDefault();

  @Rule(order = 1)
  public ExpectedException expectedException = ExpectedException.none();

  @Rule(order = 2)
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public boolean cannotVectorize = false;
  public boolean skipVectorize = false;
  public boolean msqCompatible = true;

  public QueryLogHook queryLogHook;

  private QueryComponentSupplier baseComponentSupplier;
  public PlannerComponentSupplier basePlannerComponentSupplier = new StandardPlannerComponentSupplier();

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

  public static IsTrueDimFilter istrue(DimFilter filter)
  {
    return new IsTrueDimFilter(filter);
  }

  public static InDimFilter in(String dimension, Collection<String> values, ExtractionFn extractionFn)
  {
    return new InDimFilter(dimension, values, extractionFn);
  }

  public static DimFilter isNull(final String fieldName)
  {
    return isNull(fieldName, null);
  }

  public static DimFilter isNull(final String fieldName, final ExtractionFn extractionFn)
  {
    if (NullHandling.sqlCompatible()) {
      return new NullFilter(fieldName, null);
    }
    return selector(fieldName, NullHandling.defaultStringValue(), extractionFn);
  }

  public static DimFilter notNull(final String fieldName)
  {
    return not(isNull(fieldName));
  }

  public static DimFilter equality(final String fieldName, final Object matchValue, final ColumnType matchValueType)
  {
    if (NullHandling.sqlCompatible()) {
      return new EqualityFilter(fieldName, matchValueType, matchValue, null);
    }
    return selector(fieldName, Evals.asString(matchValue), null);
  }

  /**
   * Callers should use {@link #equality(String, Object, ColumnType)} instead of this method, since they will correctly
   * use either a {@link EqualityFilter} or {@link SelectorDimFilter} depending on the value of
   * {@link NullHandling#sqlCompatible()}, which determines the default of
   * {@link PlannerContext#CTX_SQL_USE_BOUNDS_AND_SELECTORS}
   */
  public static SelectorDimFilter selector(final String fieldName, final String value)
  {
    return selector(fieldName, value, null);
  }

  /**
   * Callers should use {@link #equality(String, Object, ColumnType)} instead of this method, since they will correctly
   * use either a {@link EqualityFilter} or {@link SelectorDimFilter} depending on the value of
   * {@link NullHandling#sqlCompatible()}, which determines the default of
   * {@link PlannerContext#CTX_SQL_USE_BOUNDS_AND_SELECTORS}
   */
  public static SelectorDimFilter selector(final String fieldName, final String value, final ExtractionFn extractionFn)
  {
    return new SelectorDimFilter(fieldName, value, extractionFn);
  }

  public static ExpressionDimFilter expressionFilter(final String expression)
  {
    return new ExpressionDimFilter(expression, CalciteTests.createExprMacroTable());
  }

  /**
   * This method should be used instead of {@link #equality(String, Object, ColumnType)} when the match value type
   * does not match the column type. If {@link NullHandling#sqlCompatible()} is true, this method is equivalent to
   * {@link #equality(String, Object, ColumnType)}. When false, this method uses
   * {@link #numericSelector(String, String)} so that the equality comparison uses a bound filter to correctly match
   * numerical types.
   */
  public static DimFilter numericEquality(
      final String fieldName,
      final Object value,
      final ColumnType matchValueType
  )
  {
    if (NullHandling.sqlCompatible()) {
      return equality(fieldName, value, matchValueType);
    }
    return numericSelector(fieldName, String.valueOf(value));
  }

  public static DimFilter numericSelector(
      final String fieldName,
      final String value
  )
  {
    // We use Bound filters for numeric equality to achieve "10.0" = "10"
    return bound(fieldName, value, value, false, false, null, StringComparators.NUMERIC);
  }

  /**
   * Callers should use {@link #range(String, ColumnType, Object, Object, boolean, boolean)} instead of this method,
   * since they will correctly use either a {@link RangeFilter} or {@link BoundDimFilter} depending on the value of
   * {@link NullHandling#sqlCompatible()}, which determines the default of
   * {@link PlannerContext#CTX_SQL_USE_BOUNDS_AND_SELECTORS}
   */
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

  /**
   * Callers should use {@link #timeRange(Object)} instead of this method, since it will correctly use either a
   * {@link RangeFilter} or {@link BoundDimFilter} depending on the value of {@link NullHandling#sqlCompatible()},
   * which determines the default of {@link PlannerContext#CTX_SQL_USE_BOUNDS_AND_SELECTORS}
   */
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

  public static DimFilter range(
      final String fieldName,
      final ColumnType matchValueType,
      final Object lower,
      final Object upper,
      final boolean lowerStrict,
      final boolean upperStrict
  )
  {
    if (NullHandling.sqlCompatible()) {
      return new RangeFilter(fieldName, matchValueType, lower, upper, lowerStrict, upperStrict, null);
    }
    return new BoundDimFilter(
        fieldName,
        Evals.asString(lower),
        Evals.asString(upper),
        lowerStrict,
        upperStrict,
        false,
        null,
        matchValueType.isNumeric() ? StringComparators.NUMERIC : StringComparators.LEXICOGRAPHIC
    );
  }

  public static DimFilter timeRange(final Object intervalObj)
  {
    final Interval interval = new Interval(intervalObj, ISOChronology.getInstanceUTC());
    if (NullHandling.sqlCompatible()) {
      return range(
          ColumnHolder.TIME_COLUMN_NAME,
          ColumnType.LONG,
          interval.getStartMillis(),
          interval.getEndMillis(),
          false,
          true
      );
    }
    return timeBound(intervalObj);
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
        CalciteTests.createExprMacroTable(),
        CalciteTests.createJoinableFactoryWrapper()
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

  public static ExpressionPostAggregator expressionPostAgg(final String name, final String expression, ColumnType outputType)
  {
    return new ExpressionPostAggregator(name, expression, null, outputType, CalciteTests.createExprMacroTable());
  }

  public static Druids.ScanQueryBuilder newScanQueryBuilder()
  {
    return new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .legacy(false);
  }

  protected static DruidExceptionMatcher invalidSqlIs(String s)
  {
    return DruidExceptionMatcher.invalidSqlInput().expectMessageIs(s);
  }

  protected static DruidExceptionMatcher invalidSqlContains(String s)
  {
    return DruidExceptionMatcher.invalidSqlInput().expectMessageContains(s);
  }

  @Rule
  public QueryLogHook getQueryLogHook()
  {
    // Indirection for the JSON mapper. Otherwise, this rule method is called
    // before Setup is called, causing the query framework to be built before
    // tests have done their setup. The indirection means we access the query
    // framework only when we log the first query. By then, the query framework
    // will have been created via the normal path.
    return queryLogHook = new QueryLogHook(() -> queryFramework().queryJsonMapper());
  }

  @ClassRule
  public static SqlTestFrameworkConfig.ClassRule queryFrameworkClassRule = new SqlTestFrameworkConfig.ClassRule();

  @Rule(order = 3)
  public SqlTestFrameworkConfig.MethodRule queryFrameworkRule = queryFrameworkClassRule.methodRule(this);

  public SqlTestFramework queryFramework()
  {
    return queryFrameworkRule.get();
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final JoinableFactoryWrapper joinableFactory,
      final Injector injector
  ) throws IOException
  {
    return baseComponentSupplier.createQuerySegmentWalker(conglomerate, joinableFactory, injector);
  }

  @Override
  public SqlEngine createEngine(
      final QueryLifecycleFactory qlf,
      final ObjectMapper queryJsonMapper,
      Injector injector
  )
  {
    if (engine0 == null) {
      return baseComponentSupplier.createEngine(qlf, queryJsonMapper, injector);
    } else {
      return engine0;
    }
  }

  @Override
  public void gatherProperties(Properties properties)
  {
    try {
      baseComponentSupplier = new StandardComponentSupplier(
          temporaryFolder.newFolder()
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    baseComponentSupplier.gatherProperties(properties);
  }

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    baseComponentSupplier.configureGuice(builder);
  }

  @Override
  public QueryRunnerFactoryConglomerate createCongolmerate(Builder builder, Closer closer)
  {
    return baseComponentSupplier.createCongolmerate(builder, closer);
  }

  @Override
  public void configureJsonMapper(ObjectMapper mapper)
  {
    baseComponentSupplier.configureJsonMapper(mapper);
  }

  @Override
  public JoinableFactoryWrapper createJoinableFactoryWrapper(LookupExtractorFactoryContainerProvider lookupProvider)
  {
    return baseComponentSupplier.createJoinableFactoryWrapper(lookupProvider);
  }

  @Override
  public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
  {
    baseComponentSupplier.finalizeTestFramework(sqlTestFramework);
  }

  @Override
  public Set<ExtensionCalciteRuleProvider> extensionCalciteRules()
  {
    return basePlannerComponentSupplier.extensionCalciteRules();
  }

  @Override
  public ViewManager createViewManager()
  {
    return basePlannerComponentSupplier.createViewManager();
  }

  @Override
  public void populateViews(ViewManager viewManager, PlannerFactory plannerFactory)
  {
    basePlannerComponentSupplier.populateViews(viewManager, plannerFactory);
  }

  @Override
  public DruidSchemaManager createSchemaManager()
  {
    return basePlannerComponentSupplier.createSchemaManager();
  }

  @Override
  public void finalizePlanner(PlannerFixture plannerFixture)
  {
    basePlannerComponentSupplier.finalizePlanner(plannerFixture);
  }

  public void assumeFeatureAvailable(EngineFeature feature)
  {
    boolean featureAvailable = queryFramework().engine()
        .featureAvailable(feature, ExpressionTestHelper.PLANNER_CONTEXT);
    assumeTrue(StringUtils.format("test disabled; feature [%s] is not available!", feature), featureAvailable);
  }

  public void assertQueryIsUnplannable(final String sql, String expectedError)
  {
    assertQueryIsUnplannable(PLANNER_CONFIG_DEFAULT, sql, expectedError);
  }

  public void assertQueryIsUnplannable(final PlannerConfig plannerConfig, final String sql, String expectedError)
  {
    try {
      testQuery(plannerConfig, sql, CalciteTests.REGULAR_USER_AUTH_RESULT, ImmutableList.of(), ImmutableList.of());
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(
          e,
          new DruidExceptionMatcher(DruidException.Persona.ADMIN, DruidException.Category.INVALID_INPUT, "general")
              .expectMessageIs(
                  StringUtils.format(
                      "Query could not be planned. A possible reason is [%s]",
                      expectedError
                  )
              )
      );
    }
    catch (Exception e) {
      log.error(e, "Expected DruidException for query: %s", sql);
      Assert.fail(sql);
    }
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
      final ResultMatchMode resultsMatchMode,
      final List<Object[]> expectedResults
  )
  {
    testBuilder()
        .sql(sql)
        .expectedQueries(expectedQueries)
        .expectedResults(resultsMatchMode, expectedResults)
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
        .skipVectorize(skipVectorize)
        .msqCompatible(msqCompatible);
  }

  public class CalciteTestConfig implements QueryTestBuilder.QueryTestConfig
  {
    private boolean isRunningMSQ = false;
    private Map<String, Object> baseQueryContext;

    public CalciteTestConfig()
    {
    }

    public CalciteTestConfig(boolean isRunningMSQ)
    {
      this.isRunningMSQ = isRunningMSQ;
    }

    public CalciteTestConfig(Map<String, Object> baseQueryContext)
    {
      this.baseQueryContext = baseQueryContext;
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
    public PlannerFixture plannerFixture(PlannerConfig plannerConfig, AuthConfig authConfig)
    {
      return queryFramework().plannerFixture(BaseCalciteQueryTest.this, plannerConfig, authConfig);
    }

    @Override
    public ObjectMapper jsonMapper()
    {
      return queryFramework().queryJsonMapper();
    }

    @Override
    public ResultsVerifier defaultResultsVerifier(
        List<Object[]> expectedResults,
        ResultMatchMode expectedResultMatchMode,
        RowSignature expectedResultSignature
    )
    {
      return BaseCalciteQueryTest.this.defaultResultsVerifier(
          expectedResults,
          expectedResultMatchMode,
          expectedResultSignature
      );
    }

    @Override
    public boolean isRunningMSQ()
    {
      return isRunningMSQ;
    }

    @Override
    public Map<String, Object> baseQueryContext()
    {
      return baseQueryContext;
    }
  }

  public enum ResultMatchMode
  {
    EQUALS {
      @Override
      void validate(int row, int column, ValueType type, Object expectedCell, Object resultCell)
      {
        assertEquals(
            mismatchMessage(row, column),
            expectedCell,
            resultCell);
      }
    },
    RELAX_NULLS {
      @Override
      void validate(int row, int column, ValueType type, Object expectedCell, Object resultCell)
      {
        if (expectedCell == null) {
          if (resultCell == null) {
            return;
          }
          expectedCell = NullHandling.defaultValueForType(type);
        }
        EQUALS.validate(row, column, type, expectedCell, resultCell);
      }
    },
    EQUALS_EPS {
      @Override
      void validate(int row, int column, ValueType type, Object expectedCell, Object resultCell)
      {
        if (expectedCell instanceof Float) {
          assertEquals(
              mismatchMessage(row, column),
              (Float) expectedCell,
              (Float) resultCell,
              ASSERTION_EPSILON);
        } else if (expectedCell instanceof Double) {
          assertEquals(
              mismatchMessage(row, column),
              (Double) expectedCell,
              (Double) resultCell,
              ASSERTION_EPSILON);
        } else {
          EQUALS.validate(row, column, type, expectedCell, resultCell);
        }
      }
    },
    /**
     * Comparision which accepts 1000 units of least precision.
     */
    EQUALS_RELATIVE_1000_ULPS {
      static final int ASSERTION_ERROR_ULPS = 1000;

      @Override
      void validate(int row, int column, ValueType type, Object expectedCell, Object resultCell)
      {
        if (expectedCell instanceof Float) {
          float eps = ASSERTION_ERROR_ULPS * Math.ulp((Float) expectedCell);
          assertEquals(
              mismatchMessage(row, column),
              (Float) expectedCell,
              (Float) resultCell,
              eps
          );
        } else if (expectedCell instanceof Double) {
          double eps = ASSERTION_ERROR_ULPS * Math.ulp((Double) expectedCell);
          assertEquals(
              mismatchMessage(row, column),
              (Double) expectedCell,
              (Double) resultCell,
              eps
          );
        } else {
          EQUALS.validate(row, column, type, expectedCell, resultCell);
        }
      }
    };

    abstract void validate(int row, int column, ValueType type, Object expectedCell, Object resultCell);

    private static String mismatchMessage(int row, int column)
    {
      return StringUtils.format("column content mismatch at %d,%d", row, column);
    }

  }

  /**
   * Validates the results with slight loosening in case {@link NullHandling} is not sql compatible.
   *
   * In case {@link NullHandling#replaceWithDefault()} is true, if the expected result is <code>null</code> it accepts
   * both <code>null</code> and the default value for that column as actual result.
   */
  public void assertResultsValid(final ResultMatchMode matchMode, final List<Object[]> expected, final QueryResults queryResults)
  {
    final List<Object[]> results = queryResults.results;
    Assert.assertEquals("Result count mismatch", expected.size(), results.size());

    final List<ValueType> types = new ArrayList<>();

    final boolean isMSQ = isMSQRowType(queryResults.signature);

    if (!isMSQ) {
      for (int i = 0; i < queryResults.signature.getColumnNames().size(); i++) {
        Optional<ColumnType> columnType = queryResults.signature.getColumnType(i);
        if (columnType.isPresent()) {
          types.add(columnType.get().getType());
        } else {
          types.add(null);
        }
      }
    }

    final int numRows = results.size();
    for (int row = 0; row < numRows; row++) {
      final Object[] expectedRow = expected.get(row);
      final Object[] resultRow = results.get(row);
      assertEquals("column count mismatch; at row#" + row, expectedRow.length, resultRow.length);

      for (int i = 0; i < resultRow.length; i++) {
        final Object resultCell = resultRow[i];
        final Object expectedCell = expectedRow[i];

        matchMode.validate(
            row,
            i,
            isMSQ ? null : types.get(i),
            expectedCell,
            resultCell);
      }
    }
  }

  private boolean isMSQRowType(RowSignature signature)
  {
    List<String> colNames = signature.getColumnNames();
    return colNames.size() == 1 && "TASK".equals(colNames.get(0));
  }

  public void assertResultsEquals(String sql, List<Object[]> expectedResults, List<Object[]> results)
  {
    int minSize = Math.min(results.size(), expectedResults.size());
    for (int i = 0; i < minSize; i++) {
      Assert.assertArrayEquals(
          StringUtils.format("result #%d: %s", i + 1, sql),
          expectedResults.get(i),
          results.get(i)
      );
    }
    Assert.assertEquals(expectedResults.size(), results.size());
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
  SqlStatementFactory getSqlStatementFactory(
      PlannerConfig plannerConfig,
      AuthConfig authConfig
  )
  {
    return queryFramework().plannerFixture(this, plannerConfig, authConfig).statementFactory();
  }

  protected void cannotVectorize()
  {
    cannotVectorize = true;
  }

  protected void skipVectorize()
  {
    skipVectorize = true;
  }

  protected void msqIncompatible()
  {
    msqCompatible = false;
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
   *
   * @return
   */
  public static <T> Query<?> recursivelyClearContext(final Query<T> query, ObjectMapper queryJsonMapper)
  {
    try {
      Query<T> newQuery = query.withDataSource(recursivelyClearContext(query.getDataSource(), queryJsonMapper));
      final JsonNode newQueryNode = queryJsonMapper.valueToTree(newQuery);
      ((ObjectNode) newQueryNode).remove("context");
      return queryJsonMapper.treeToValue(newQueryNode, Query.class);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Override the contexts of all subqueries of a particular datasource.
   */
  private static DataSource recursivelyClearContext(final DataSource dataSource, ObjectMapper queryJsonMapper)
  {
    if (dataSource instanceof QueryDataSource) {
      final Query<?> subquery = ((QueryDataSource) dataSource).getQuery();
      Query<?> newSubQuery = recursivelyClearContext(subquery, queryJsonMapper);
      return new QueryDataSource(newSubQuery);
    } else {
      return dataSource.withChildren(
          dataSource.getChildren()
                    .stream()
                    .map(ds -> recursivelyClearContext(ds, queryJsonMapper))
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
      output.put(
          GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_GRANULARITY,
          queryFramework().queryJsonMapper().writeValueAsString(granularity)
      );
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

    void verify(String sql, QueryResults queryResults);
  }

  private ResultsVerifier defaultResultsVerifier(
      final List<Object[]> expectedResults,
      ResultMatchMode expectedResultMatchMode,
      final RowSignature expectedSignature
  )
  {
    return new DefaultResultsVerifier(expectedResults, expectedResultMatchMode, expectedSignature);
  }

  public class DefaultResultsVerifier implements ResultsVerifier
  {
    protected final List<Object[]> expectedResults;
    @Nullable
    protected final RowSignature expectedResultRowSignature;
    protected final ResultMatchMode expectedResultMatchMode;

    public DefaultResultsVerifier(List<Object[]> expectedResults, ResultMatchMode expectedResultMatchMode, RowSignature expectedSignature)
    {
      this.expectedResults = expectedResults;
      this.expectedResultMatchMode = expectedResultMatchMode;
      this.expectedResultRowSignature = expectedSignature;
    }

    public DefaultResultsVerifier(List<Object[]> expectedResults, RowSignature expectedSignature)
    {
      this(expectedResults, ResultMatchMode.EQUALS, expectedSignature);
    }

    @Override
    public void verifyRowSignature(RowSignature rowSignature)
    {
      if (expectedResultRowSignature != null) {
        Assert.assertEquals(expectedResultRowSignature, rowSignature);
      }
    }

    @Override
    public void verify(String sql, QueryResults queryResults)
    {
      try {
        assertResultsValid(expectedResultMatchMode, expectedResults, queryResults);
      }
      catch (AssertionError e) {
        log.info("sql: %s", sql);
        log.info(resultsToString("Expected", expectedResults));
        log.info(resultsToString("Actual", queryResults.results));
        throw e;
      }
    }

  }

  /**
   * Dump the expected results in the form of the elements of a Java array which
   * can be used to validate the results. This is a convenient way to create the
   * expected results: let the test fail with empty results. The actual results
   * are printed to the console. Copy them into the test.
   */
  public static String resultsToString(String name, List<Object[]> results)
  {
    return new ResultsPrinter(name, results).getResult();
  }

  static class ResultsPrinter
  {
    private StringBuilder sb;

    private ResultsPrinter(String name, List<Object[]> results)
    {
      sb = new StringBuilder();
      sb.append("-- ");
      sb.append(name);
      sb.append(" results --\n");

      for (int rowIndex = 0; rowIndex < results.size(); rowIndex++) {
        printArray(results.get(rowIndex));
        if (rowIndex < results.size() - 1) {
          outprint(",");
        }
        sb.append('\n');
      }
      sb.append("----");
    }

    private String getResult()
    {
      return sb.toString();
    }

    private void printArray(final Object[] array)
    {
      printArrayImpl(array, "new Object[]{", "}");
    }

    private void printList(final List<?> list)
    {
      printArrayImpl(list.toArray(new Object[0]), "ImmutableList.of(", ")");
    }

    private void printArrayImpl(final Object[] array, final String pre, final String post)
    {
      sb.append(pre);
      for (int colIndex = 0; colIndex < array.length; colIndex++) {
        Object col = array[colIndex];
        if (colIndex > 0) {
          sb.append(", ");
        }
        if (col == null) {
          sb.append("null");
        } else if (col instanceof String) {
          outprint("\"");
          outprint(StringEscapeUtils.escapeJava((String) col));
          outprint("\"");
        } else if (col instanceof Long) {
          outprint(col);
          outprint("L");
        } else if (col instanceof Double) {
          outprint(col);
          outprint("D");
        } else if (col instanceof Float) {
          outprint(col);
          outprint("F");
        } else if (col instanceof Object[]) {
          printArray(array);
        } else if (col instanceof List) {
          printList((List<?>) col);
        } else {
          outprint(col);
        }
      }
      outprint(post);
    }

    private void outprint(Object post)
    {
      sb.append(post);
    }
  }
}
