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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.OperatorFactoryBuilders;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.WindowOperatorQueryBuilder;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMaxAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.GroupingAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.any.DoubleAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.FloatAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.LongAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.StringAnyAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.FloatLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.DecoupledTestConfig.NativeQueryIgnore;
import org.apache.druid.sql.calcite.NotYetSupported.Modes;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.CannotBuildQueryException;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

public class CalciteQueryTest extends BaseCalciteQueryTest
{
  @Test
  public void testInformationSchemaSchemata()
  {
    msqIncompatible();
    testQuery(
        "SELECT DISTINCT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"lookup"},
            new Object[]{"view"},
            new Object[]{"druid"},
            new Object[]{"sys"},
            new Object[]{"INFORMATION_SCHEMA"}
        )
    );
  }

  @Test
  public void testInformationSchemaTables()
  {
    msqIncompatible();
    testQuery(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, IS_JOINABLE, IS_BROADCAST\n"
        + "FROM INFORMATION_SCHEMA.TABLES\n"
        + "WHERE TABLE_TYPE IN ('SYSTEM_TABLE', 'TABLE', 'VIEW')",
        ImmutableList.of(),
        ImmutableList.<Object[]>builder()
                     .add(new Object[]{"druid", CalciteTests.BROADCAST_DATASOURCE, "TABLE", "YES", "YES"})
                     .add(new Object[]{"druid", CalciteTests.DATASOURCE1, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.DATASOURCE2, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.DATASOURCE4, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.DATASOURCE5, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.DATASOURCE3, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.SOME_DATASOURCE, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.SOMEXDATASOURCE, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.USERVISITDATASOURCE, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.WIKIPEDIA, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.WIKIPEDIA_FIRST_LAST, "TABLE", "NO", "NO"})
                     .add(new Object[]{"INFORMATION_SCHEMA", "COLUMNS", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"INFORMATION_SCHEMA", "ROUTINES", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"INFORMATION_SCHEMA", "SCHEMATA", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"INFORMATION_SCHEMA", "TABLES", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"lookup", "lookyloo", "TABLE", "YES", "YES"})
                     .add(new Object[]{"lookup", "lookyloo-chain", "TABLE", "YES", "YES"})
                     .add(new Object[]{"lookup", "lookyloo121", "TABLE", "YES", "YES"})
                     .add(new Object[]{"sys", "segments", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"sys", "server_segments", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"sys", "servers", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"sys", "supervisors", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"sys", "tasks", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"view", "aview", "VIEW", "NO", "NO"})
                     .add(new Object[]{"view", "bview", "VIEW", "NO", "NO"})
                     .add(new Object[]{"view", "cview", "VIEW", "NO", "NO"})
                     .add(new Object[]{"view", "dview", "VIEW", "NO", "NO"})
                     .add(new Object[]{"view", "invalidView", "VIEW", "NO", "NO"})
                     .add(new Object[]{"view", "restrictedView", "VIEW", "NO", "NO"})
                     .build()
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, IS_JOINABLE, IS_BROADCAST\n"
        + "FROM INFORMATION_SCHEMA.TABLES\n"
        + "WHERE TABLE_TYPE IN ('SYSTEM_TABLE', 'TABLE', 'VIEW')",
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.<Object[]>builder()
                     .add(new Object[]{"druid", CalciteTests.BROADCAST_DATASOURCE, "TABLE", "YES", "YES"})
                     .add(new Object[]{"druid", CalciteTests.DATASOURCE1, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.DATASOURCE2, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.DATASOURCE4, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.FORBIDDEN_DATASOURCE, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.DATASOURCE5, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.DATASOURCE3, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.SOME_DATASOURCE, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.SOMEXDATASOURCE, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.USERVISITDATASOURCE, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.WIKIPEDIA, "TABLE", "NO", "NO"})
                     .add(new Object[]{"druid", CalciteTests.WIKIPEDIA_FIRST_LAST, "TABLE", "NO", "NO"})
                     .add(new Object[]{"INFORMATION_SCHEMA", "COLUMNS", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"INFORMATION_SCHEMA", "ROUTINES", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"INFORMATION_SCHEMA", "SCHEMATA", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"INFORMATION_SCHEMA", "TABLES", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"lookup", "lookyloo", "TABLE", "YES", "YES"})
                     .add(new Object[]{"lookup", "lookyloo-chain", "TABLE", "YES", "YES"})
                     .add(new Object[]{"lookup", "lookyloo121", "TABLE", "YES", "YES"})
                     .add(new Object[]{"sys", "segments", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"sys", "server_segments", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"sys", "servers", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"sys", "supervisors", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"sys", "tasks", "SYSTEM_TABLE", "NO", "NO"})
                     .add(new Object[]{"view", "aview", "VIEW", "NO", "NO"})
                     .add(new Object[]{"view", "bview", "VIEW", "NO", "NO"})
                     .add(new Object[]{"view", "cview", "VIEW", "NO", "NO"})
                     .add(new Object[]{"view", "dview", "VIEW", "NO", "NO"})
                     .add(new Object[]{"view", "forbiddenView", "VIEW", "NO", "NO"})
                     .add(new Object[]{"view", "invalidView", "VIEW", "NO", "NO"})
                     .add(new Object[]{"view", "restrictedView", "VIEW", "NO", "NO"})
                     .build()
    );
  }

  @Test
  public void testInformationSchemaColumnsOnTable()
  {
    msqIncompatible();
    testQuery(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"__time", "TIMESTAMP", "NO"},
            new Object[]{"dim1", "VARCHAR", "YES"},
            new Object[]{"dim2", "VARCHAR", "YES"},
            new Object[]{"dim3", "VARCHAR", "YES"},
            new Object[]{"cnt", "BIGINT", useDefault ? "NO" : "YES"},
            new Object[]{"m1", "FLOAT", useDefault ? "NO" : "YES"},
            new Object[]{"m2", "DOUBLE", useDefault ? "NO" : "YES"},
            new Object[]{"unique_dim1", "COMPLEX<hyperUnique>", "YES"}
        )
    );
  }

  @Test
  public void testInformationSchemaColumnsOnForbiddenTable()
  {
    msqIncompatible();
    testQuery(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'forbiddenDatasource'",
        ImmutableList.of(),
        ImmutableList.of()
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'forbiddenDatasource'",
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"__time", "TIMESTAMP", "NO"},
            new Object[]{"dim1", "VARCHAR", "YES"},
            new Object[]{"dim2", "VARCHAR", "YES"},
            new Object[]{"cnt", "BIGINT", useDefault ? "NO" : "YES"},
            new Object[]{"m1", "FLOAT", useDefault ? "NO" : "YES"},
            new Object[]{"m2", "DOUBLE", useDefault ? "NO" : "YES"},
            new Object[]{"unique_dim1", "COMPLEX<hyperUnique>", "YES"}
        )
    );
  }

  @Test
  public void testInformationSchemaColumnsOnView()
  {
    msqIncompatible();
    testQuery(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'view' AND TABLE_NAME = 'aview'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"dim1_firstchar", "VARCHAR", "YES"}
        )
    );
  }

  @Test
  public void testInformationSchemaColumnsOnAnotherView()
  {
    msqIncompatible();
    testQuery(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'view' AND TABLE_NAME = 'cview'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"dim1_firstchar", "VARCHAR", "YES"},
            new Object[]{"dim2", "VARCHAR", "YES"},
            new Object[]{"l2", "BIGINT", useDefault ? "NO" : "YES"}
        )
    );
  }

  @Test
  public void testCannotInsertWithNativeEngine()
  {
    msqIncompatible();
    final DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> testQuery(
            "INSERT INTO dst SELECT * FROM foo PARTITIONED BY ALL",
            ImmutableList.of(),
            ImmutableList.of()
        )
    );

    MatcherAssert.assertThat(
        e,
        invalidSqlIs("INSERT operations are not supported by requested SQL engine [native], consider using MSQ.")
    );
  }

  @Test
  public void testCannotReplaceWithNativeEngine()
  {
    msqIncompatible();
    final DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> testQuery(
            "REPLACE INTO dst OVERWRITE ALL SELECT * FROM foo PARTITIONED BY ALL",
            ImmutableList.of(),
            ImmutableList.of()
        )
    );

    MatcherAssert.assertThat(
        e,
        invalidSqlIs("REPLACE operations are not supported by the requested SQL engine [native].  Consider using MSQ.")
    );
  }

  @Test
  public void testAggregatorsOnInformationSchemaColumns()
  {
    msqIncompatible();
    // Not including COUNT DISTINCT, since it isn't supported by BindableAggregate, and so it can't work.
    testQuery(
        "SELECT\n"
        + "  COUNT(JDBC_TYPE),\n"
        + "  SUM(JDBC_TYPE),\n"
        + "  AVG(JDBC_TYPE),\n"
        + "  MIN(JDBC_TYPE),\n"
        + "  MAX(JDBC_TYPE)\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{8L, 1249L, 156.125, -5L, 1111L}
        )
    );
  }

  @Test
  public void testFilterAggregatorFunctionsOnInformationSchemaRoutines()
  {
    msqIncompatible();
    testQuery(
        "SELECT\n"
        + "  COUNT(*)\n"
        + "FROM INFORMATION_SCHEMA.ROUTINES\n"
        + "WHERE IS_AGGREGATOR = 'YES' AND ROUTINE_NAME = 'COUNT'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testFilterScalarFunctionsOnInformationSchemaRoutines()
  {
    msqIncompatible();
    testQuery(
        "SELECT\n"
        + "  COUNT(*)\n"
        + "FROM INFORMATION_SCHEMA.ROUTINES\n"
        + "WHERE IS_AGGREGATOR = 'NO' AND ROUTINE_NAME = 'CEIL'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testNonExistentSchemaOnInformationSchemaRoutines()
  {
    msqIncompatible();
    testQuery(
        "SELECT *\n"
        + "FROM INFORMATION_SCHEMA.ROUTINES\n"
        + "WHERE ROUTINE_SCHEMA = 'boo'",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testTopNLimitWrapping()
  {
    msqIncompatible();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"", 1L},
          new Object[]{"def", 1L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"def", 1L},
          new Object[]{"abc", 1L}
      );
    }
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                .threshold(2)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .metric(
                    new InvertedTopNMetricSpec(
                        new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC)
                    )
                )
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testTopNLimitWrappingOrderByAgg()
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo GROUP BY 1 ORDER BY 2 DESC",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                .threshold(2)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .metric("a0")
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(new Object[]{"", 1L}, new Object[]{"1", 1L})
    );
  }

  @Test
  public void testGroupByLimitWrapping()
  {
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"def", "abc", 1L},
          new Object[]{"abc", "", 1L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"def", "abc", 1L},
          new Object[]{"abc", null, 1L}
      );
    }
    testQuery(
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo GROUP BY dim1, dim2 ORDER BY dim1 DESC",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(
                    new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING),
                    new DefaultDimensionSpec("dim2", "d1", ColumnType.STRING)
                )
                .setLimitSpec(
                    DefaultLimitSpec
                        .builder()
                        .orderBy(new OrderByColumnSpec("d0", Direction.DESCENDING, StringComparators.LEXICOGRAPHIC))
                        .limit(2)
                        .build()
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testGroupByWithForceLimitPushDown()
  {
    final Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    context.put(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true);

    testQuery(
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo GROUP BY dim1, dim2 limit 1",
        context,
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(
                    new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING),
                    new DefaultDimensionSpec("dim2", "d1", ColumnType.STRING)
                )
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(),
                        1
                    )
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(context)
                .build()
        ),
        ImmutableList.of(new Object[]{"", "a", 1L})
    );
  }

  @Test
  public void testSafeDivide()
  {
    skipVectorize();
    cannotVectorize();
    final Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);

    testQuery(
        "select count(*) c from foo where ((floor(safe_divide(cast(cast(m1 as char) as bigint), 2))) = 0)",
        context,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "floor(safe_divide(CAST(CAST(\"m1\", 'STRING'), 'LONG'),2))", ColumnType.LONG))
                .filters(equality("v0", 0L, ColumnType.LONG))
                .granularity(Granularities.ALL)
                .aggregators(new CountAggregatorFactory("a0"))
                .context(context)
                .build()
        ),
        ImmutableList.of(new Object[]{1L})
    );
  }

  @Test
  public void testGroupByLimitWrappingOrderByAgg()
  {
    testQuery(
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo GROUP BY 1, 2 ORDER BY 3 DESC",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(
                    new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING),
                    new DefaultDimensionSpec("dim2", "d1", ColumnType.STRING)
                )
                .setLimitSpec(
                    DefaultLimitSpec
                        .builder()
                        .orderBy(new OrderByColumnSpec("a0", Direction.DESCENDING, StringComparators.NUMERIC))
                        .limit(2)
                        .build()
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", 1L},
            new Object[]{"1", "a", 1L}
        )
    );
  }

  @Test
  public void testGroupBySingleColumnDescendingNoTopN()
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                .setGranularity(Granularities.ALL)
                .setLimitSpec(
                    DefaultLimitSpec
                        .builder()
                        .orderBy(
                            new OrderByColumnSpec(
                                "d0",
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.LEXICOGRAPHIC
                            )
                        )
                        .build()
                )
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def"},
            new Object[]{"abc"},
            new Object[]{"2"},
            new Object[]{"10.1"},
            new Object[]{"1"},
            new Object[]{""}
        )
    );
  }

  @Test
  public void testEarliestAggregators()
  {
    testQuery(
        "SELECT "
        + "EARLIEST(cnt), EARLIEST(m1), EARLIEST(dim1, 10), EARLIEST(dim1, CAST(10 AS INTEGER)), "
        + "EARLIEST(cnt + 1), EARLIEST(m1 + 1), EARLIEST(dim1 || CAST(cnt AS VARCHAR), 10), "
        + "EARLIEST_BY(cnt, MILLIS_TO_TIMESTAMP(l1)), EARLIEST_BY(m1, MILLIS_TO_TIMESTAMP(l1)), EARLIEST_BY(dim1, MILLIS_TO_TIMESTAMP(l1), 10), "
        + "EARLIEST_BY(cnt + 1, MILLIS_TO_TIMESTAMP(l1)), EARLIEST_BY(m1 + 1, MILLIS_TO_TIMESTAMP(l1)), EARLIEST_BY(dim1 || CAST(cnt AS VARCHAR), MILLIS_TO_TIMESTAMP(l1), 10), EARLIEST_BY(dim1 || CAST(cnt AS VARCHAR), MILLIS_TO_TIMESTAMP(l1), 10) "
        + "FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"cnt\" + 1)", ColumnType.LONG),
                      expressionVirtualColumn("v1", "(\"m1\" + 1)", ColumnType.FLOAT),
                      expressionVirtualColumn("v2", "concat(\"dim1\",CAST(\"cnt\", 'STRING'))", ColumnType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new LongFirstAggregatorFactory("a0", "cnt", null),
                          new FloatFirstAggregatorFactory("a1", "m1", null),
                          new StringFirstAggregatorFactory("a2", "dim1", null, 10),
                          new LongFirstAggregatorFactory("a3", "v0", null),
                          new FloatFirstAggregatorFactory("a4", "v1", null),
                          new StringFirstAggregatorFactory("a5", "v2", null, 10),
                          new LongFirstAggregatorFactory("a6", "cnt", "l1"),
                          new FloatFirstAggregatorFactory("a7", "m1", "l1"),
                          new StringFirstAggregatorFactory("a8", "dim1", "l1", 10),
                          new LongFirstAggregatorFactory("a9", "v0", "l1"),
                          new FloatFirstAggregatorFactory("a10", "v1", "l1"),
                          new StringFirstAggregatorFactory("a11", "v2", "l1", 10)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 1.0f, "", "", 2L, 2.0f, "1", 1L, 3.0f, "2", 2L, 4.0f, "21", "21"}
        )
    );
  }

  @Test
  public void testLatestToLatestByConversion()
  {
    msqIncompatible();
    testQuery(
        "SELECT LATEST(dim1,10) FROM (SELECT DISTINCT __time, dim1 from foo)",
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(
                    GroupByQuery.builder()
                                .setDataSource(CalciteTests.DATASOURCE1)
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimensions(dimensions(
                                    new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                                    new DefaultDimensionSpec("dim1", "d1", ColumnType.STRING)
                                ))
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build())
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setAggregatorSpecs(
                    new StringLastAggregatorFactory("a0", "d1", "d0", 10))
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()),
        ImmutableList.of(new Object[]{"abc"})
    );
  }

  @Test
  public void testLatestVectorAggregators()
  {
    msqIncompatible();
    testQuery(
        "SELECT "
        + "LATEST(cnt), LATEST(cnt + 1), LATEST(m1), LATEST(m1+1) "
        + "FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"cnt\" + 1)", ColumnType.LONG),
                      expressionVirtualColumn("v1", "(\"m1\" + 1)", ColumnType.FLOAT)
                  )
                  .aggregators(
                      aggregators(
                          new LongLastAggregatorFactory("a0", "cnt", null),
                          new LongLastAggregatorFactory("a1", "v0", null),
                          new FloatLastAggregatorFactory("a2", "m1", null),
                          new FloatLastAggregatorFactory("a3", "v1", null)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 2L, 6.0f, 7.0f}
        )
    );
  }

  @Test
  public void testLatestAggregators()
  {
    msqIncompatible();

    testQuery(
        "SELECT "
        + "LATEST(cnt), LATEST(m1), LATEST(dim1, 10), LATEST(dim1, CAST(10 AS INTEGER)), "
        + "LATEST(cnt + 1), LATEST(m1 + 1), LATEST(dim1 || CAST(cnt AS VARCHAR), 10), "
        + "LATEST_BY(cnt, MILLIS_TO_TIMESTAMP(l1)), LATEST_BY(m1, MILLIS_TO_TIMESTAMP(l1)), LATEST_BY(dim1, MILLIS_TO_TIMESTAMP(l1), 10), "
        + "LATEST_BY(cnt + 1, MILLIS_TO_TIMESTAMP(l1)), LATEST_BY(m1 + 1, MILLIS_TO_TIMESTAMP(l1)), LATEST_BY(dim1 || CAST(cnt AS VARCHAR), MILLIS_TO_TIMESTAMP(l1), 10), LATEST_BY(dim1 || CAST(cnt AS VARCHAR), MILLIS_TO_TIMESTAMP(l1), CAST(10 AS INTEGER)) "
        + "FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"cnt\" + 1)", ColumnType.LONG),
                      expressionVirtualColumn("v1", "(\"m1\" + 1)", ColumnType.FLOAT),
                      expressionVirtualColumn("v2", "concat(\"dim1\",CAST(\"cnt\", 'STRING'))", ColumnType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new LongLastAggregatorFactory("a0", "cnt", null),
                          new FloatLastAggregatorFactory("a1", "m1", null),
                          new StringLastAggregatorFactory("a2", "dim1", null, 10),
                          new LongLastAggregatorFactory("a3", "v0", null),
                          new FloatLastAggregatorFactory("a4", "v1", null),
                          new StringLastAggregatorFactory("a5", "v2", null, 10),
                          new LongLastAggregatorFactory("a6", "cnt", "l1"),
                          new FloatLastAggregatorFactory("a7", "m1", "l1"),
                          new StringLastAggregatorFactory("a8", "dim1", "l1", 10),
                          new LongLastAggregatorFactory("a9", "v0", "l1"),
                          new FloatLastAggregatorFactory("a10", "v1", "l1"),
                          new StringLastAggregatorFactory("a11", "v2", "l1", 10)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6.0f, "abc", "abc", 2L, 7.0f, "abc1", 1L, 2.0f, "10.1", 2L, 3.0f, "10.11", "10.11"}
        )
    );
  }

  @Test
  public void testEarliestByInvalidTimestamp()
  {
    expectedException.expect(DruidException.class);
    expectedException.expectMessage("Cannot apply 'EARLIEST_BY' to arguments of type 'EARLIEST_BY(<FLOAT>, <BIGINT>)");

    testQuery(
        "SELECT EARLIEST_BY(m1, l1) FROM druid.numfoo",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testLatestByInvalidTimestamp()
  {
    expectedException.expect(
        invalidSqlContains("Cannot apply 'LATEST_BY' to arguments of type 'LATEST_BY(<FLOAT>, <BIGINT>)")
    );

    testQuery(
        "SELECT LATEST_BY(m1, l1) FROM druid.numfoo",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  // This test the on-heap version of the AnyAggregator (Double/Float/Long/String)
  @Test
  public void testAnyAggregator()
  {
    // Cannot vectorize virtual expressions.
    skipVectorize();

    testQuery(
        "SELECT "
        + "ANY_VALUE(cnt), ANY_VALUE(m1), ANY_VALUE(m2), ANY_VALUE(dim1, 10), "
        + "ANY_VALUE(cnt + 1), ANY_VALUE(m1 + 1), ANY_VALUE(dim1 || CAST(cnt AS VARCHAR), 10) "
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"cnt\" + 1)", ColumnType.LONG),
                      expressionVirtualColumn("v1", "(\"m1\" + 1)", ColumnType.FLOAT),
                      expressionVirtualColumn("v2", "concat(\"dim1\",CAST(\"cnt\", 'STRING'))", ColumnType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new LongAnyAggregatorFactory("a0", "cnt"),
                          new FloatAnyAggregatorFactory("a1", "m1"),
                          new DoubleAnyAggregatorFactory("a2", "m2"),
                          new StringAnyAggregatorFactory("a3", "dim1", 10, true),
                          new LongAnyAggregatorFactory("a4", "v0"),
                          new FloatAnyAggregatorFactory("a5", "v1"),
                          new StringAnyAggregatorFactory("a6", "v2", 10, true)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(new Object[]{1L, 1.0f, 1.0, "", 2L, 2.0f, "1"})
        : ImmutableList.of(new Object[]{1L, 1.0f, 1.0, "", 2L, 2.0f, "1"})
    );
  }

  // This test the on-heap version of the AnyAggregator (Double/Float/Long) against numeric columns
  // that have null values (when run in SQL compatible null mode)
  @Test
  public void testAnyAggregatorsOnHeapNumericNulls()
  {
    testQuery(
        "SELECT ANY_VALUE(l1), ANY_VALUE(d1), ANY_VALUE(f1) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new LongAnyAggregatorFactory("a0", "l1"),
                          new DoubleAnyAggregatorFactory("a1", "d1"),
                          new FloatAnyAggregatorFactory("a2", "f1")
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{7L, 1.0, 1.0f}
        )
    );
  }

  // This test is to check if time expressions are accepted properly by the vectorized last aggregator
  @Test
  public void testLatestVectorAggregatorsOnTimeExpression()
  {
    msqIncompatible();
    testQuery(
        "SELECT \n"
        + "  LATEST_BY(m1, MILLIS_TO_TIMESTAMP(BITWISE_SHIFT_RIGHT(TIMESTAMP_TO_MILLIS(__time), 3)))\n"
        + " FROM druid.foo GROUP BY TIME_FLOOR(__time, 'P1Y', null, 'America/Los_Angeles')",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(new PeriodGranularity(Period.years(1), null, DateTimes.inferTzFromString(LOS_ANGELES)))
                  .virtualColumns(
                      expressionVirtualColumn("v1", "bitwiseShiftRight(\"__time\",3)", ColumnType.LONG)
                  )
                  .aggregators(
                      aggregators(
                          new FloatLastAggregatorFactory("a0", "m1", "v1")
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1.0f},
            new Object[]{4.0f},
            new Object[]{6.0f}
        )
    );
  }

  // This test the off-heap (buffer) version of the AnyAggregator (Double/Float/Long) against numeric columns
  // that have null values (when run in SQL compatible null mode)
  @Test
  public void testAnyAggregatorsOffHeapNumericNulls()
  {
    testQuery(
        "SELECT ANY_VALUE(l1), ANY_VALUE(d1), ANY_VALUE(f1) FROM druid.numfoo GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "_d0")))
                        .setAggregatorSpecs(
                            aggregators(
                                new LongAnyAggregatorFactory("a0", "l1"),
                                new DoubleAnyAggregatorFactory("a1", "d1"),
                                new FloatAnyAggregatorFactory("a2", "f1")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),

        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{325323L, 1.7, 0.1f},
            new Object[]{0L, 0.0, 0.0f},
            new Object[]{7L, 1.0, 1.0f},
            new Object[]{null, null, null}
        )
        : ImmutableList.of(
            new Object[]{325323L, 1.7, 0.1f},
            new Object[]{7L, 1.0, 1.0f},
            new Object[]{0L, 0.0, 0.0f}
        )
    );
  }

  // This test the off-heap (buffer) version of the LatestAggregator (Double/Float/Long)
  @Test
  public void testPrimitiveLatestInSubquery()
  {
    msqIncompatible();
    testQuery(
        "SELECT SUM(val1), SUM(val2), SUM(val3) FROM (SELECT dim2, LATEST(m1) AS val1, LATEST(cnt) AS val2, LATEST(m2) AS val3 FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(
                                                                new FloatLastAggregatorFactory("a0:a", "m1", null),
                                                                new LongLastAggregatorFactory("a1:a", "cnt", null),
                                                                new DoubleLastAggregatorFactory("a2:a", "m2", null)
                                                            )
                                        )
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a"),
                                                new FinalizingFieldAccessPostAggregator("a1", "a1:a"),
                                                new FinalizingFieldAccessPostAggregator("a2", "a2:a")

                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                                                new DoubleSumAggregatorFactory("_a0", "a0"),
                                                new LongSumAggregatorFactory("_a1", "a1"),
                                                new DoubleSumAggregatorFactory("_a2", "a2")
                                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(new Object[]{18.0, 4L, 18.0})
        : ImmutableList.of(new Object[]{15.0, 3L, 15.0})
    );
  }

  @Test
  public void testPrimitiveLatestInSubqueryGroupBy()
  {
    msqIncompatible();
    testQuery(
        "SELECT dim2, LATEST(m1) AS val1 FROM foo GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                        .setAggregatorSpecs(aggregators(
                                                new FloatLastAggregatorFactory("a0", "m1", null)
                                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{null, 6.0f},
            new Object[]{"", 3.0f},
            new Object[]{"a", 4.0f},
            new Object[]{"abc", 5.0f}
        )
        : ImmutableList.of(
            new Object[]{"", 6.0f},
            new Object[]{"a", 4.0f},
            new Object[]{"abc", 5.0f}

        )
    );
  }

  @Test
  public void testStringLatestGroupBy()
  {
    testQuery(
        "SELECT dim2, LATEST(dim4,10) AS val1 FROM druid.numfoo GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "_d0")))
                        .setAggregatorSpecs(aggregators(
                                                new StringLastAggregatorFactory("a0", "dim4", null, 10)
                                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{null, "b"},
            new Object[]{"", "a"},
            new Object[]{"a", "b"},
            new Object[]{"abc", "b"}
        )
        : ImmutableList.of(
            new Object[]{"", "b"},
            new Object[]{"a", "b"},
            new Object[]{"abc", "b"}
        )
    );
  }

  @Test
  public void testNumericLatestEarliestGroupBy()
  {
    testQuery(
        "SELECT isNew, LATEST(long_last_added), EARLIEST(long_first_added), LATEST(float_last_added), EARLIEST(float_first_added), LATEST(double_last_added), EARLIEST(double_first_added) FROM wikipedia_first_last GROUP BY isNew",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.WIKIPEDIA_FIRST_LAST)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("isNew", "d0")))
                        .setAggregatorSpecs(aggregators(
                                                new LongLastAggregatorFactory("a0", "long_last_added", null),
                                                new LongFirstAggregatorFactory("a1", "long_first_added", null),
                                                new FloatLastAggregatorFactory("a2", "float_last_added", null),
                                                new FloatFirstAggregatorFactory("a3", "float_first_added", null),
                                                new DoubleLastAggregatorFactory("a4", "double_last_added", null),
                                                new DoubleFirstAggregatorFactory("a5", "double_first_added", null)
                                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"false", 182L, 36L, 182.0F, 36.0F, 182.0D, 36.0D},
            new Object[]{"true", 113L, 345L, 113.0F, 345.0F, 113.0D, 345.0D}
        )
    );
  }

  @Test
  public void testNumericLatestEarliestWithOpratorsGroupBy()
  {
    testQuery(
        "SELECT isNew, LATEST(long_last_added)+4, EARLIEST(long_first_added)-4, LATEST(float_last_added)*2, EARLIEST(float_first_added)/2f, LATEST(double_last_added)+2.5, EARLIEST(double_first_added)-2.5 FROM wikipedia_first_last GROUP BY isNew",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.WIKIPEDIA_FIRST_LAST)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("isNew", "d0")))
                        .setAggregatorSpecs(aggregators(
                                                new LongLastAggregatorFactory("a0", "long_last_added", null),
                                                new LongFirstAggregatorFactory("a1", "long_first_added", null),
                                                new FloatLastAggregatorFactory("a2", "float_last_added", null),
                                                new FloatFirstAggregatorFactory("a3", "float_first_added", null),
                                                new DoubleLastAggregatorFactory("a4", "double_last_added", null),
                                                new DoubleFirstAggregatorFactory("a5", "double_first_added", null)
                                            )
                        )
                        .setPostAggregatorSpecs(
                            expressionPostAgg("p0", "(\"a0\" + 4)", ColumnType.LONG),
                            expressionPostAgg("p1", "(\"a1\" - 4)", ColumnType.LONG),
                            expressionPostAgg("p2", "(\"a2\" * 2)", ColumnType.FLOAT),
                            expressionPostAgg("p3", "(\"a3\" / 2)", ColumnType.FLOAT),
                            expressionPostAgg("p4", "(\"a4\" + 2.5)", ColumnType.DOUBLE),
                            expressionPostAgg("p5", "(\"a5\" - 2.5)", ColumnType.DOUBLE)
                        )

                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"false", 186L, 32L, 364.0F, 18.0F, 184.5D, 33.5D},
            new Object[]{"true", 117L, 341L, 226.0F, 172.5F, 115.5D, 342.5D}
        )
    );
  }

  @Test
  public void testStringLatestGroupByWithAlwaysFalseCondition()
  {
    testQuery(
        "SELECT LATEST(dim4, 10), dim2 FROM numfoo WHERE (dim1 = 'something' AND dim1 IN('something else')) GROUP BY dim2",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(InlineDataSource.fromIterable(
                      ImmutableList.of(),
                      RowSignature.builder()
                                  .add("EXPR$0", ColumnType.STRING)
                                  .add("dim2", ColumnType.STRING)
                                  .build()
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("EXPR$0", "dim2")
                  .context(QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testStringLatestByGroupByWithAlwaysFalseCondition()
  {
    testQuery(
        "SELECT LATEST_BY(dim4, __time, 10), dim2 FROM numfoo WHERE (dim1 = 'something' AND dim1 IN('something else')) GROUP BY dim2",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(InlineDataSource.fromIterable(
                      ImmutableList.of(),
                      RowSignature.builder()
                                  .add("EXPR$0", ColumnType.STRING)
                                  .add("dim2", ColumnType.STRING)
                                  .build()
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("EXPR$0", "dim2")
                  .context(QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of()
    );
  }

  // This test the off-heap (buffer) version of the EarliestAggregator (Double/Float/Long)
  @Test
  public void testPrimitiveEarliestInSubquery()
  {
    testQuery(
        "SELECT SUM(val1), SUM(val2), SUM(val3) FROM (SELECT dim2, EARLIEST(m1) AS val1, EARLIEST(cnt) AS val2, EARLIEST(m2) AS val3 FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(
                                                                new FloatFirstAggregatorFactory("a0:a", "m1", null),
                                                                new LongFirstAggregatorFactory("a1:a", "cnt", null),
                                                                new DoubleFirstAggregatorFactory("a2:a", "m2", null)
                                                            )
                                        )
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a"),
                                                new FinalizingFieldAccessPostAggregator("a1", "a1:a"),
                                                new FinalizingFieldAccessPostAggregator("a2", "a2:a")

                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                                                new DoubleSumAggregatorFactory("_a0", "a0"),
                                                new LongSumAggregatorFactory("_a1", "a1"),
                                                new DoubleSumAggregatorFactory("_a2", "a2")
                                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(new Object[]{11.0, 4L, 11.0})
        : ImmutableList.of(new Object[]{8.0, 3L, 8.0})
    );
  }

  // This test the off-heap (buffer) version of the LatestAggregator (String)
  @Test
  public void testStringLatestInSubquery()
  {
    testQuery(
        "SELECT SUM(val) FROM (SELECT dim2, LATEST(dim1, 10) AS val FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(new StringLastAggregatorFactory(
                                            "a0:a",
                                            "dim1",
                                            null,
                                            10
                                        )))
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a")
                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "CAST(\"a0\", 'DOUBLE')", ColumnType.DOUBLE)
                        )
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory(
                            "_a0",
                            "v0",
                            null,
                            ExprMacroTable.nil()
                        )))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.sqlCompatible() ? 3 : 1.0}
        )
    );
  }

  // This test the off-heap (buffer) version of the EarliestAggregator (String)
  @Test
  public void testStringEarliestInSubquery()
  {
    testQuery(
        "SELECT SUM(val) FROM (SELECT dim2, EARLIEST(dim1,10) AS val FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(new StringFirstAggregatorFactory(
                                            "a0:a",
                                            "dim1",
                                            null,
                                            10
                                        )))
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a")
                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "CAST(\"a0\", 'DOUBLE')", ColumnType.DOUBLE)
                        )
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory(
                            "_a0",
                            "v0",
                            null,
                            ExprMacroTable.nil()
                        )))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            // default mode subquery results:
            //[, 10.1]
            //[a, ]
            //[abc, def]
            // SQL compatible mode subquery results:
            //[null, 10.1]
            //[, 2]
            //[a, ]
            //[abc, def]
            new Object[]{NullHandling.sqlCompatible() ? 12.1 : 10.1}
        )
    );
  }

  // This test the off-heap (buffer) version of the AnyAggregator (Double/Float/Long)
  @Test
  public void testPrimitiveAnyInSubquery()
  {
    // The grouping works like this
    // dim2 ->    m1   |   m2
    // a    -> [1,4]   | [1,4]
    // null -> [2,3,6] | [2,3,6]
    // abc  -> [5]     | [5]
    // So the acceptable response can be any combination of these values
    testQuery(
        "SELECT SUM(val1), SUM(val2), SUM(val3) FROM (SELECT dim2, ANY_VALUE(m1) AS val1, ANY_VALUE(cnt) AS val2, ANY_VALUE(m2) AS val3 FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(
                                                                new FloatAnyAggregatorFactory("a0:a", "m1"),
                                                                new LongAnyAggregatorFactory("a1:a", "cnt"),
                                                                new DoubleAnyAggregatorFactory("a2:a", "m2")
                                                            )
                                        )
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a"),
                                                new FinalizingFieldAccessPostAggregator("a1", "a1:a"),
                                                new FinalizingFieldAccessPostAggregator("a2", "a2:a")

                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                                                new DoubleSumAggregatorFactory("_a0", "a0"),
                                                new LongSumAggregatorFactory("_a1", "a1"),
                                                new DoubleSumAggregatorFactory("_a2", "a2")
                                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(new Object[]{11.0, 4L, 11.0})
        : ImmutableList.of(new Object[]{8.0, 3L, 8.0})
    );
  }

  @Test
  public void testStringEarliestSingleStringDim()
  {
    testQuery(
        "SELECT dim2, EARLIEST(dim1,10) AS val FROM foo GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                        .setAggregatorSpecs(aggregators(new StringFirstAggregatorFactory(
                            "a0",
                            "dim1",
                            null,
                            10
                        )))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible() ?
        ImmutableList.of(
            new Object[]{null, "10.1"},
            new Object[]{"", "2"},
            new Object[]{"a", ""},
            new Object[]{"abc", "def"}
        ) : ImmutableList.of(
            new Object[]{"", "10.1"},
            new Object[]{"a", ""},
            new Object[]{"abc", "def"}
        )
    );
  }

  @Test
  public void testStringEarliestMultiStringDim()
  {
    testQuery(
        "SELECT dim2, EARLIEST(dim3,10) AS val FROM foo GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                        .setAggregatorSpecs(aggregators(new StringFirstAggregatorFactory(
                            "a0",
                            "dim3",
                            null,
                            10
                        )))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible() ?
        ImmutableList.of(
            new Object[]{null, "[b, c]"},
            new Object[]{"", "d"},
            new Object[]{"a", "[a, b]"},
            new Object[]{"abc", null}
        ) : ImmutableList.of(
            new Object[]{"", "[b, c]"},
            new Object[]{"a", "[a, b]"},
            new Object[]{"abc", ""}
        )
    );
  }

  // This test the off-heap (buffer) version of the AnyAggregator (String)
  @Test
  public void testStringAnyInSubquery()
  {
    testQuery(
        "SELECT SUM(val) FROM (SELECT dim2, ANY_VALUE(dim1, 10) AS val FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(new StringAnyAggregatorFactory(
                                            "a0:a",
                                            "dim1",
                                            10, true
                                        )))
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a")
                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "CAST(\"a0\", 'DOUBLE')", ColumnType.DOUBLE)
                        )
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory(
                            "_a0",
                            "v0",
                            null,
                            ExprMacroTable.nil()
                        )))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.sqlCompatible() ? 12.1 : 10.1}
        )
    );
  }

  @Test
  public void testEarliestAggregatorsNumericNulls()
  {
    testQuery(
        "SELECT EARLIEST(l1), EARLIEST(d1), EARLIEST(f1) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new LongFirstAggregatorFactory("a0", "l1", null),
                          new DoubleFirstAggregatorFactory("a1", "d1", null),
                          new FloatFirstAggregatorFactory("a2", "f1", null)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{7L, 1.0, 1.0f}
        )
    );
  }

  @Test
  public void testLatestAggregatorsNumericNull()
  {
    msqIncompatible();
    testQuery(
        "SELECT LATEST(l1), LATEST(d1), LATEST(f1) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new LongLastAggregatorFactory("a0", "l1", null),
                          new DoubleLastAggregatorFactory("a1", "d1", null),
                          new FloatLastAggregatorFactory("a2", "f1", null)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                NullHandling.defaultLongValue(),
                NullHandling.defaultDoubleValue(),
                NullHandling.defaultFloatValue()
            }
        )
    );
  }

  @Test
  public void testFirstLatestAggregatorsSkipNulls()
  {
    final DimFilter filter;
    if (useDefault) {
      filter = notNull("dim1");
    } else {
      filter = and(
          notNull("dim1"),
          notNull("l1"),
          notNull("d1"),
          notNull("f1")
      );
    }
    testQuery(
        "SELECT EARLIEST(dim1, 32), LATEST(l1), LATEST(d1), LATEST(f1) "
        + "FROM druid.numfoo "
        + "WHERE dim1 IS NOT NULL AND l1 IS NOT NULL AND d1 IS NOT NULL AND f1 is NOT NULL",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(filter)
                  .aggregators(
                      aggregators(
                          new StringFirstAggregatorFactory("a0", "dim1", null, 32),
                          new LongLastAggregatorFactory("a1", "l1", null),
                          new DoubleLastAggregatorFactory("a2", "d1", null),
                          new FloatLastAggregatorFactory("a3", "f1", null)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            // first row of dim1 is empty string, which is null in default mode, last non-null numeric rows are zeros
            new Object[]{useDefault ? "10.1" : "", 0L, 0.0, 0.0f}
        )
    );
  }

  @Test
  public void testAnyAggregatorsDoesNotSkipNulls()
  {
    testQuery(
        "SELECT ANY_VALUE(dim1, 32), ANY_VALUE(l2), ANY_VALUE(d2), ANY_VALUE(f2) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new StringAnyAggregatorFactory("a0", "dim1", 32, true),
                          new LongAnyAggregatorFactory("a1", "l2"),
                          new DoubleAnyAggregatorFactory("a2", "d2"),
                          new FloatAnyAggregatorFactory("a3", "f2")
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        // first row has null for l2, d2, f2 and dim1 as empty string (which is null in default mode)
        ImmutableList.of(
            useDefault ? new Object[]{"", 0L, 0.0, 0f} : new Object[]{"", null, null, null}
        )
    );
  }

  @Test
  public void testAnyAggregatorsSkipNullsWithFilter()
  {
    final DimFilter filter;
    if (useDefault) {
      filter = notNull("dim1");
    } else {
      filter = and(
          notNull("dim1"),
          notNull("l2"),
          notNull("d2"),
          notNull("f2")
      );
    }
    testQuery(
        "SELECT ANY_VALUE(dim1, 32), ANY_VALUE(l2), ANY_VALUE(d2), ANY_VALUE(f2) "
        + "FROM druid.numfoo "
        + "WHERE dim1 IS NOT NULL AND l2 IS NOT NULL AND d2 IS NOT NULL AND f2 is NOT NULL",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(filter)
                  .aggregators(
                      aggregators(
                          new StringAnyAggregatorFactory("a0", "dim1", 32, true),
                          new LongAnyAggregatorFactory("a1", "l2"),
                          new DoubleAnyAggregatorFactory("a2", "d2"),
                          new FloatAnyAggregatorFactory("a3", "f2")
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            // first row of dim1 is empty string, which is null in default mode
            new Object[]{"10.1", 325323L, 1.7, 0.1f}
        )
    );
  }

  @Test
  public void testOrderByEarliestFloat()
  {
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0f},
          new Object[]{"2", 0.0f},
          new Object[]{"abc", 0.0f},
          new Object[]{"def", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f}
      );
    }
    testQuery(
        "SELECT dim1, EARLIEST(f1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new FloatFirstAggregatorFactory("a0", "f1", null)
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByEarliestDouble()
  {
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0},
          new Object[]{"2", 0.0},
          new Object[]{"abc", 0.0},
          new Object[]{"def", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7}
      );
    }
    testQuery(
        "SELECT dim1, EARLIEST(d1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new DoubleFirstAggregatorFactory("a0", "d1", null)
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByEarliestLong()
  {
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0L},
          new Object[]{"2", 0L},
          new Object[]{"abc", 0L},
          new Object[]{"def", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L}
      );
    }
    testQuery(
        "SELECT dim1, EARLIEST(l1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new LongFirstAggregatorFactory("a0", "l1", null)
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByLatestFloat()
  {
    msqIncompatible();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0f},
          new Object[]{"2", 0.0f},
          new Object[]{"abc", 0.0f},
          new Object[]{"def", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f}
      );
    }

    testQuery(
        "SELECT dim1, LATEST(f1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new FloatLastAggregatorFactory("a0", "f1", null)
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByLatestDouble()
  {
    msqIncompatible();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0},
          new Object[]{"2", 0.0},
          new Object[]{"abc", 0.0},
          new Object[]{"def", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7}
      );
    }
    testQuery(
        "SELECT dim1, LATEST(d1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new DoubleLastAggregatorFactory("a0", "d1", null)
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByLatestLong()
  {
    msqIncompatible();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0L},
          new Object[]{"2", 0L},
          new Object[]{"abc", 0L},
          new Object[]{"def", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L}
      );
    }
    testQuery(
        "SELECT dim1, LATEST(l1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new LongLastAggregatorFactory("a0", "l1", null)
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByAnyFloat()
  {
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0f},
          new Object[]{"2", 0.0f},
          new Object[]{"abc", 0.0f},
          new Object[]{"def", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f}
      );
    } else {
      // Disabled test in MSQ with SQL-compatible mode till https://github.com/apache/druid/issues/13951 is resolved
      msqIncompatible();

      expected = ImmutableList.of(
          new Object[]{"2", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f},
          // Nulls are last because of the null first wrapped Comparator in InvertedTopNMetricSpec which is then
          // reversed by TopNNumericResultBuilder.build()
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null}
      );
    }

    testQuery(
        "SELECT dim1, ANY_VALUE(f1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new FloatAnyAggregatorFactory("a0", "f1")
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByAnyDouble()
  {
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0},
          new Object[]{"2", 0.0},
          new Object[]{"abc", 0.0},
          new Object[]{"def", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7}
      );
    } else {
      // Disabled test in MSQ with SQL-compatible mode till https://github.com/apache/druid/issues/13951 is resolved
      msqIncompatible();

      expected = ImmutableList.of(
          new Object[]{"2", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7},
          // Nulls are last because of the null first wrapped Comparator in InvertedTopNMetricSpec which is then
          // reversed by TopNNumericResultBuilder.build()
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null}
      );
    }
    testQuery(
        "SELECT dim1, ANY_VALUE(d1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new DoubleAnyAggregatorFactory("a0", "d1")
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByAnyLong()
  {
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0L},
          new Object[]{"2", 0L},
          new Object[]{"abc", 0L},
          new Object[]{"def", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L}
      );
    } else {
      // Disabled test in MSQ with SQL-compatible mode till https://github.com/apache/druid/issues/13951 is resolved
      msqIncompatible();

      expected = ImmutableList.of(
          new Object[]{"2", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L},
          // Nulls are last because of the null first wrapped Comparator in InvertedTopNMetricSpec which is then
          // reversed by TopNNumericResultBuilder.build()
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null}
      );
    }
    testQuery(
        "SELECT dim1, ANY_VALUE(l1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new LongAnyAggregatorFactory("a0", "l1")
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testGroupByLong()
  {
    testQuery(
        "SELECT cnt, COUNT(*) FROM druid.foo GROUP BY cnt",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L}
        )
    );
  }

  @Test
  public void testGroupByOrdinal()
  {
    testQuery(
        "SELECT cnt, COUNT(*) FROM druid.foo GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L}
        )
    );
  }

  @Test
  @Ignore("Disabled since GROUP BY alias can confuse the validator; see DruidConformance::isGroupByAlias")
  public void testGroupByAndOrderByAlias()
  {
    msqIncompatible();
    testQuery(
        "SELECT cnt AS theCnt, COUNT(*) FROM druid.foo GROUP BY theCnt ORDER BY theCnt ASC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            DefaultLimitSpec
                                .builder()
                                .orderBy(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                )
                                .build()
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L}
        )
    );
  }

  @Test
  public void testGroupByExpressionAliasedAsOriginalColumnName()
  {
    testQuery(
        "SELECT\n"
        + "FLOOR(__time TO MONTH) AS __time,\n"
        + "COUNT(*)\n"
        + "FROM druid.foo\n"
        + "GROUP BY FLOOR(__time TO MONTH)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 3L},
            new Object[]{timestamp("2001-01-01"), 3L}
        )
    );
  }

  @Test
  public void testGroupByAndOrderByOrdinalOfAlias()
  {
    testQuery(
        "SELECT cnt as theCnt, COUNT(*) FROM druid.foo GROUP BY 1 ORDER BY 1 ASC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(NoopLimitSpec.instance())
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L}
        )
    );
  }

  @Test
  public void testGroupByFloat()
  {
    testQuery(
        "SELECT m1, COUNT(*) FROM druid.foo GROUP BY m1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{3.0f, 1L},
            new Object[]{4.0f, 1L},
            new Object[]{5.0f, 1L},
            new Object[]{6.0f, 1L}
        )
    );
  }

  @Test
  public void testGroupByDouble()
  {
    testQuery(
        "SELECT m2, COUNT(*) FROM druid.foo GROUP BY m2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("m2", "d0", ColumnType.DOUBLE)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1.0d, 1L},
            new Object[]{2.0d, 1L},
            new Object[]{3.0d, 1L},
            new Object[]{4.0d, 1L},
            new Object[]{5.0d, 1L},
            new Object[]{6.0d, 1L}
        )
    );
  }

  @Test
  public void testFilterOnFloat()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE m1 = 1.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(equality("m1", 1.0, ColumnType.DOUBLE))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testDECODE_BASE64_UTF8()
  {
    testQuery(
        "SELECT DECODE_BASE64_UTF8('aGVsbG8=') FROM druid.foo limit 1",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      expressionVirtualColumn(
                          "v0",
                          "'hello'",
                          ColumnType.STRING
                      )
                  )
                  .limit(1)
                  .columns(ImmutableList.of("v0"))
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"hello"}
        )
    );
  }

  @Test
  public void testFilterOnDouble()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE m2 = 1.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(equality("m2", 1.0, ColumnType.DOUBLE))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testHavingOnGrandTotal()
  {
    testQuery(
        "SELECT SUM(m1) AS m1_sum FROM foo HAVING m1_sum = 21",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                        .setHavingSpec(
                            having(
                                NullHandling.replaceWithDefault()
                                ? selector("a0", "21")
                                : equality("a0", 21.0, ColumnType.DOUBLE)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{21d}
        )
    );
  }

  @Test
  public void testHavingOnDoubleSum()
  {
    testQuery(
        "SELECT dim1, SUM(m1) AS m1_sum FROM druid.foo GROUP BY dim1 HAVING SUM(m1) > 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                        .setHavingSpec(
                            having(
                                range(
                                    "a0",
                                    ColumnType.LONG,
                                    1L,
                                    null,
                                    true,
                                    false
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"1", 4.0d},
            new Object[]{"10.1", 2.0d},
            new Object[]{"2", 3.0d},
            new Object[]{"abc", 6.0d},
            new Object[]{"def", 5.0d}
        )
    );
  }

  @Test
  public void testHavingOnApproximateCountDistinct()
  {
    testQuery(
        "SELECT dim2, COUNT(DISTINCT m1) FROM druid.foo GROUP BY dim2 HAVING COUNT(DISTINCT m1) > 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                        .setAggregatorSpecs(
                            aggregators(
                                new CardinalityAggregatorFactory(
                                    "a0",
                                    null,
                                    ImmutableList.of(
                                        new DefaultDimensionSpec("m1", "m1", ColumnType.FLOAT)
                                    ),
                                    false,
                                    true
                                )
                            )
                        )
                        .setHavingSpec(
                            having(
                                range(
                                    "a0",
                                    ColumnType.LONG,
                                    1L,
                                    null,
                                    true,
                                    false
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", 3L},
            new Object[]{"a", 2L}
        ) :
        ImmutableList.of(
            new Object[]{null, 2L},
            new Object[]{"a", 2L}
        )
    );
  }

  @Test
  public void testHavingOnExactCountDistinct()
  {
    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT dim2, COUNT(DISTINCT m1) FROM druid.foo GROUP BY dim2 HAVING COUNT(DISTINCT m1) > 1",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(
                                                dimensions(
                                                    new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING),
                                                    new DefaultDimensionSpec("m1", "d1", ColumnType.FLOAT)
                                                )
                                            )
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("d0", "_d0", ColumnType.STRING)))
                        .setAggregatorSpecs(
                            aggregators(
                                useDefault
                                ? new CountAggregatorFactory("a0")
                                : new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a0"),
                                    notNull("d1")
                                )
                            )
                        )
                        .setHavingSpec(
                            having(
                                range(
                                    "a0",
                                    ColumnType.LONG,
                                    1L,
                                    null,
                                    true,
                                    false
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", 3L},
            new Object[]{"a", 2L}
        ) :
        ImmutableList.of(
            new Object[]{null, 2L},
            new Object[]{"a", 2L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testExactCountDistinctWithFilter()
  {
    msqIncompatible();
    final String sqlQuery = "SELECT COUNT(DISTINCT foo.dim1) FILTER(WHERE foo.cnt = 1), SUM(foo.cnt) FROM druid.foo";
    // When useApproximateCountDistinct=false and useGroupingSetForExactDistinct=false, planning fails due
    // to a bug in the Calcite's rule (AggregateExpandDistinctAggregatesRule)
    Assert.assertThrows(
        RuntimeException.class,
        () -> testQuery(
            PLANNER_CONFIG_NO_HLL.withOverrides(
                ImmutableMap.of(
                    PlannerConfig.CTX_KEY_USE_GROUPING_SET_FOR_EXACT_DISTINCT, "false",
                    PlannerConfig.CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT, false
                )
            ), // Enable exact count distinct
            sqlQuery,
            CalciteTests.REGULAR_USER_AUTH_RESULT,
            ImmutableList.of(),
            ImmutableList.of()
        )
    );

    if (NullHandling.sqlCompatible()) {
      // Cannot vectorize due to "istrue" operator.
      cannotVectorize();
    }

    testQuery(
        PLANNER_CONFIG_NO_HLL.withOverrides(
            ImmutableMap.of(
                PlannerConfig.CTX_KEY_USE_GROUPING_SET_FOR_EXACT_DISTINCT,
                "true"
            )
        ),
        sqlQuery,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setVirtualColumns(expressionVirtualColumn(
                                                "v0",
                                                NullHandling.sqlCompatible()
                                                ? "istrue((\"cnt\" == 1))"
                                                : "(\"cnt\" == 1)",
                                                ColumnType.LONG
                                            ))
                                            .setDimensions(dimensions(
                                                new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING),
                                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                                            ))
                                            .setAggregatorSpecs(
                                                aggregators(
                                                    new LongSumAggregatorFactory("a0", "cnt"),
                                                    new GroupingAggregatorFactory(
                                                        "a1",
                                                        Arrays.asList("dim1", "v0")
                                                    )
                                                )
                                            )
                                            .setSubtotalsSpec(
                                                ImmutableList.of(
                                                    ImmutableList.of("d0", "d1"),
                                                    ImmutableList.of()
                                                )
                                            )
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("_a0"),
                                and(
                                    notNull("d0"),
                                    equality("a1", 0L, ColumnType.LONG),
                                    expressionFilter("\"d1\"")
                                )
                            ),
                            new FilteredAggregatorFactory(
                                new LongMinAggregatorFactory("_a1", "a0"),
                                equality("a1", 3L, ColumnType.LONG)
                            )
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 5L : 6L, 6L}
        )
    );
  }

  @Test
  public void testHavingOnFloatSum()
  {
    testQuery(
        "SELECT dim1, CAST(SUM(m1) AS FLOAT) AS m1_sum FROM druid.foo GROUP BY dim1 HAVING CAST(SUM(m1) AS FLOAT) > 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                        .setHavingSpec(
                            having(
                                range(
                                    "a0",
                                    ColumnType.LONG,
                                    1L,
                                    null,
                                    true,
                                    false
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"1", 4.0f},
            new Object[]{"10.1", 2.0f},
            new Object[]{"2", 3.0f},
            new Object[]{"abc", 6.0f},
            new Object[]{"def", 5.0f}
        )
    );
  }

  @Test
  public void testColumnComparison()
  {
    testQuery(
        "SELECT dim1, m1, COUNT(*) FROM druid.foo WHERE m1 - 1 = dim1 GROUP BY dim1, m1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(expressionFilter("((\"m1\" - 1) == CAST(\"dim1\", 'DOUBLE'))"))
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("m1", "d1", ColumnType.FLOAT)
                        ))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", 1.0f, 1L},
            new Object[]{"2", 3.0f, 1L}
        ) :
        ImmutableList.of(
            new Object[]{"2", 3.0f, 1L}
        )
    );
  }

  @Test
  public void testHavingOnRatio()
  {
    // Test for https://github.com/apache/druid/issues/4264

    testQuery(
        "SELECT\n"
        + "  dim1,\n"
        + "  COUNT(*) FILTER(WHERE dim2 <> 'a')/COUNT(*) as ratio\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "HAVING COUNT(*) FILTER(WHERE dim2 <> 'a')/COUNT(*) = 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(aggregators(
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("a0"),
                                not(equality("dim2", "a", ColumnType.STRING))
                            ),
                            new CountAggregatorFactory("a1")
                        ))
                        .setPostAggregatorSpecs(
                            expressionPostAgg("p0", "(\"a0\" / \"a1\")", ColumnType.LONG)
                        )
                        .setHavingSpec(having(expressionFilter("((\"a0\" / \"a1\") == 1)")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
        : ImmutableList.of(
            new Object[]{"2", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testGroupByWithSelectProjections()
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setPostAggregatorSpecs(ImmutableList.of(
                            expressionPostAgg("p0", "substring(\"d0\", 1, -1)", ColumnType.STRING)
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", NULL_STRING},
            new Object[]{"1", NULL_STRING},
            new Object[]{"10.1", "0.1"},
            new Object[]{"2", NULL_STRING},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"}
        )
    );
  }

  @NotYetSupported(Modes.PLAN_MISMATCH)
  @Test
  public void testGroupByWithSelectAndOrderByProjections()
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "ORDER BY CHARACTER_LENGTH(dim1) DESC, dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setPostAggregatorSpecs(ImmutableList.of(
                            expressionPostAgg("p0", "substring(\"d0\", 1, -1)", ColumnType.STRING),
                            expressionPostAgg("p1", "strlen(\"d0\")", ColumnType.LONG)
                        ))
                        .setLimitSpec(
                            DefaultLimitSpec
                                .builder()
                                .orderBy(
                                    new OrderByColumnSpec(
                                        "p1",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    ),
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.LEXICOGRAPHIC
                                    )
                                )
                                .build()
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", "0.1"},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"},
            new Object[]{"1", NULL_STRING},
            new Object[]{"2", NULL_STRING},
            new Object[]{"", NULL_STRING}
        )
    );
  }

  @Test
  public void testTopNWithSelectProjections()
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "d0"))
                .postAggregators(expressionPostAgg("s0", "substring(\"d0\", 1, -1)", ColumnType.STRING))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", NULL_STRING},
            new Object[]{"1", NULL_STRING},
            new Object[]{"10.1", "0.1"},
            new Object[]{"2", NULL_STRING},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"}
        )
    );
  }

  @NotYetSupported(Modes.PLAN_MISMATCH)
  @Test
  public void testTopNWithSelectAndOrderByProjections()
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "ORDER BY CHARACTER_LENGTH(dim1) DESC\n"
        + "LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "d0"))
                .postAggregators(
                    expressionPostAgg("p0", "substring(\"d0\", 1, -1)", ColumnType.STRING),
                    expressionPostAgg("p1", "strlen(\"d0\")", ColumnType.LONG)
                )
                .metric(new NumericTopNMetricSpec("p1"))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", "0.1"},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"},
            new Object[]{"1", NULL_STRING},
            new Object[]{"2", NULL_STRING},
            new Object[]{"", NULL_STRING}
        )
    );
  }

  @NotYetSupported(Modes.UNION_WITH_COMPLEX_OPERAND)
  @Test
  public void testUnionAllQueries()
  {
    msqIncompatible();
    testQuery(
        "SELECT COUNT(*) FROM foo UNION ALL SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build(),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build(),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{6L}, new Object[]{6L}, new Object[]{6L})
    );
  }

  @NotYetSupported(Modes.UNION_WITH_COMPLEX_OPERAND)
  @Test
  public void testUnionAllQueriesWithLimit()
  {
    msqIncompatible();
    testQuery(
        "SELECT * FROM ("
        + "SELECT COUNT(*) FROM foo UNION ALL SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo"
        + ") LIMIT 2",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build(),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{6L}, new Object[]{6L})
    );
  }

  @Test
  public void testPruneDeadAggregators()
  {
    // Test for ProjectAggregatePruneUnusedCallRule.
    testQuery(
        "SELECT\n"
        + "  CASE 'foo'\n"
        + "  WHEN 'bar' THEN SUM(cnt)\n"
        + "  WHEN 'foo' THEN SUM(m1)\n"
        + "  WHEN 'baz' THEN SUM(m2)\n"
        + "  END\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{21.0})
    );
  }

  @Test
  public void testPruneDeadAggregatorsThroughPostProjection()
  {
    // Test for ProjectAggregatePruneUnusedCallRule.
    testQuery(
        "SELECT\n"
        + "  CASE 'foo'\n"
        + "  WHEN 'bar' THEN SUM(cnt) / 10\n"
        + "  WHEN 'foo' THEN SUM(m1) / 10\n"
        + "  WHEN 'baz' THEN SUM(m2) / 10\n"
        + "  END\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                  .postAggregators(expressionPostAgg("p0", "(\"a0\" / 10)", ColumnType.DOUBLE))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{2.1})
    );
  }

  @Test
  public void testPruneDeadAggregatorsThroughHaving()
  {
    // Test for ProjectAggregatePruneUnusedCallRule.
    testQuery(
        "SELECT\n"
        + "  CASE 'foo'\n"
        + "  WHEN 'bar' THEN SUM(cnt)\n"
        + "  WHEN 'foo' THEN SUM(m1)\n"
        + "  WHEN 'baz' THEN SUM(m2)\n"
        + "  END AS theCase\n"
        + "FROM foo\n"
        + "HAVING theCase = 21",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                        .setHavingSpec(
                            having(
                                NullHandling.replaceWithDefault()
                                ? selector("a0", "21")
                                : equality("a0", 21.0, ColumnType.DOUBLE)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{21.0})
    );
  }

  @Test
  public void testGroupByCaseWhen()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  CASE EXTRACT(DAY FROM __time)\n"
        + "    WHEN m1 THEN 'match-m1'\n"
        + "    WHEN cnt THEN 'match-cnt'\n"
        + "    WHEN 0 THEN 'zero'"
        + "    END,"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "GROUP BY"
        + "  CASE EXTRACT(DAY FROM __time)\n"
        + "    WHEN m1 THEN 'match-m1'\n"
        + "    WHEN cnt THEN 'match-cnt'\n"
        + "    WHEN 0 THEN 'zero'"
        + "    END",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched("
                                + "(CAST(timestamp_extract(\"__time\",'DAY','UTC'), 'DOUBLE') == \"m1\"),"
                                + "'match-m1',"
                                + "(timestamp_extract(\"__time\",'DAY','UTC') == \"cnt\"),"
                                + "'match-cnt',"
                                + "(timestamp_extract(\"__time\",'DAY','UTC') == 0),"
                                + "'zero',"
                                + DruidExpression.nullLiteral() + ")",
                                ColumnType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 2L},
            new Object[]{"match-cnt", 1L},
            new Object[]{"match-m1", 3L}
        )
    );
  }

  @Test
  public void testDecomposeCaseWhenThreeArg()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  dim1, dim2, CASE WHEN dim1 = 'abc' THEN dim1 ELSE dim2 END\n"
        + "FROM druid.foo\n"
        + "WHERE\n"
        + "  CASE WHEN dim1 = 'abc' THEN dim1 ELSE dim2 END = 'abc'",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .virtualColumns(
                    expressionVirtualColumn(
                        "v0",
                        "case_searched((\"dim1\" == 'abc'),\"dim1\",\"dim2\")",
                        ColumnType.STRING
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    or(
                        equality("dim1", "abc", ColumnType.STRING),
                        and(
                            equality("dim2", "abc", ColumnType.STRING),
                            NullHandling.sqlCompatible()
                            ? not(istrue(equality("dim1", "abc", ColumnType.STRING)))
                            : not(equality("dim1", "abc", ColumnType.STRING))
                        )
                    )
                )
                .columns("dim1", "dim2", "v0")
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def", "abc", "abc"},
            new Object[]{"abc", NullHandling.defaultStringValue(), "abc"}
        )
    );
  }

  @Test
  public void testDecomposeCaseWhenTwoArg()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  dim1, dim2, CASE WHEN dim1 = 'def' THEN dim2 END\n"
        + "FROM druid.foo\n"
        + "WHERE\n"
        + "  CASE WHEN dim1 = 'def' THEN dim2 END = 'abc'",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .virtualColumns(
                    expressionVirtualColumn("v0", "'def'", ColumnType.STRING),
                    expressionVirtualColumn("v1", "'abc'", ColumnType.STRING)
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    and(
                        equality("dim1", "def", ColumnType.STRING),
                        equality("dim2", "abc", ColumnType.STRING)
                    )
                )
                .columns("v0", "v1")
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def", "abc", "abc"}
        )
    );
  }

  @Test
  public void testGroupByCaseWhenOfTripleAnd()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  CASE WHEN m1 > 1 AND m1 < 5 AND cnt = 1 THEN 'x' ELSE NULL END,"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched((((\"m1\" > 1) && (\"m1\" < 5)) && (\"cnt\" == 1)),'x',null)",
                                ColumnType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 3L},
            new Object[]{"x", 3L}
        )
    );
  }


  @Test
  public void testNullEmptyStringEquality()
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE NULLIF(dim2, 'a') IS NULL",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  // Ideally the following filter should be simplified to (dim2 == 'a' || dim2 IS NULL), the
                  // (dim2 != 'a') component is unnecessary.
                  .filters(
                      or(
                          equality("dim2", "a", ColumnType.STRING),
                          and(
                              isNull("dim2"),
                              NullHandling.sqlCompatible()
                              ? not(istrue(equality("dim2", "a", ColumnType.STRING)))
                              : not(selector("dim2", "a"))
                          )
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            NullHandling.replaceWithDefault()
            // Matches everything but "abc"
            ? new Object[]{5L}
            // match only null values
            : new Object[]{4L}
        )
    );
  }

  @Test
  public void testNullLongFilter()
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE l1 IS NULL",
        useDefault
        ? ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(new Object[]{0L}),
                          RowSignature.builder().add("EXPR$0", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("EXPR$0")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        )
        : ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(isNull("l1"))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault ? new Object[]{0L} : new Object[]{3L}
        )
    );
  }

  @Test
  public void testNullDoubleFilter()
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE d1 IS NULL",
        useDefault
        ? ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(new Object[]{0L}),
                          RowSignature.builder().add("EXPR$0", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("EXPR$0")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        )
        : ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(isNull("d1"))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault ? new Object[]{0L} : new Object[]{3L}
        )
    );
  }

  @Test
  public void testNullFloatFilter()
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE f1 IS NULL",
        useDefault
        ? ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(new Object[]{0L}),
                          RowSignature.builder().add("EXPR$0", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("EXPR$0")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        )
        : ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(isNull("f1"))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault ? new Object[]{0L} : new Object[]{3L}
        )
    );
  }

  /**
   * This test case should be in {@link CalciteUnionQueryTest}. However, there's a bug in the test framework that
   * doesn't reset framework once the merge buffers
   */
  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testUnionAllSameTableThreeTimes()
  {
    testQuery(
        "SELECT\n"
        + "dim1, dim2, SUM(m1), COUNT(*)\n"
        + "FROM (SELECT * FROM foo UNION ALL SELECT * FROM foo UNION ALL SELECT * FROM foo)\n"
        + "WHERE dim2 = 'a' OR dim2 = 'def'\n"
        + "GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new UnionDataSource(
                                ImmutableList.of(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new TableDataSource(CalciteTests.DATASOURCE1)
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(in("dim2", ImmutableList.of("def", "a"), null))
                        .setDimensions(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("dim2", "d1")
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new DoubleSumAggregatorFactory("a0", "m1"),
                                new CountAggregatorFactory("a1")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", 3.0, 3L},
            new Object[]{"1", "a", 12.0, 3L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testExactCountDistinctUsingSubqueryOnUnionAllTables()
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (\n"
        + "  SELECT dim2, SUM(cnt) AS cnt\n"
        + "  FROM (SELECT * FROM druid.foo UNION ALL SELECT * FROM druid.foo)\n"
        + "  GROUP BY dim2\n"
        + ")",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(
                                                new UnionDataSource(
                                                    ImmutableList.of(
                                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                                        new TableDataSource(CalciteTests.DATASOURCE1)
                                                    )
                                                )
                                            )
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("_a0", "a0"),
                            new CountAggregatorFactory("_a1")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{12L, 3L}
        ) :
        ImmutableList.of(
            new Object[]{12L, 4L}
        )
    );
  }

  @Test
  public void testNullDoubleTopN()
  {
    // Disabled test in MSQ with SQL-compatible mode till https://github.com/apache/druid/issues/13951 is resolved
    if (NullHandling.sqlCompatible()) {
      msqIncompatible();
    }
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.of(
          new Object[]{1.7, 1L},
          new Object[]{1.0, 1L},
          new Object[]{0.0, 4L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{null, 3L},
          new Object[]{1.7, 1L},
          new Object[]{1.0, 1L},
          new Object[]{0.0, 1L}
      );
    }
    testQuery(
        "SELECT d1, COUNT(*) FROM druid.numfoo GROUP BY d1 ORDER BY d1 DESC LIMIT 10",
        QUERY_CONTEXT_DEFAULT,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("d1", "_d0", ColumnType.DOUBLE))
                .threshold(10)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .metric(
                    new InvertedTopNMetricSpec(
                        new DimensionTopNMetricSpec(null, StringComparators.NUMERIC)
                    )
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testNullFloatTopN()
  {
    // Disabled test in MSQ with SQL-compatible mode till https://github.com/apache/druid/issues/13951 is resolved
    if (NullHandling.sqlCompatible()) {
      msqIncompatible();
    }
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.of(
          new Object[]{1.0f, 1L},
          new Object[]{0.1f, 1L},
          new Object[]{0.0f, 4L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{null, 3L},
          new Object[]{1.0f, 1L},
          new Object[]{0.1f, 1L},
          new Object[]{0.0f, 1L}
      );
    }
    testQuery(
        "SELECT f1, COUNT(*) FROM druid.numfoo GROUP BY f1 ORDER BY f1 DESC LIMIT 10",
        QUERY_CONTEXT_DEFAULT,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("f1", "_d0", ColumnType.FLOAT))
                .threshold(10)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .metric(
                    new InvertedTopNMetricSpec(
                        new DimensionTopNMetricSpec(null, StringComparators.NUMERIC)
                    )
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testNullLongTopN()
  {
    // Disabled test in MSQ with SQL-compatible mode till https://github.com/apache/druid/issues/13951 is resolved
    if (NullHandling.sqlCompatible()) {
      msqIncompatible();
    }
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.of(
          new Object[]{325323L, 1L},
          new Object[]{7L, 1L},
          new Object[]{0L, 4L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{null, 3L},
          new Object[]{325323L, 1L},
          new Object[]{7L, 1L},
          new Object[]{0L, 1L}
      );
    }
    testQuery(
        "SELECT l1, COUNT(*) FROM druid.numfoo GROUP BY l1 ORDER BY l1 DESC LIMIT 10",
        QUERY_CONTEXT_DEFAULT,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("l1", "_d0", ColumnType.LONG))
                .threshold(10)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .metric(
                    new InvertedTopNMetricSpec(
                        new DimensionTopNMetricSpec(null, StringComparators.NUMERIC)
                    )
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testLongPredicateIsNull()
  {
    testQuery(
        "SELECT l1 is null FROM druid.numfoo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("v0")
                .virtualColumns(
                    expressionVirtualColumn(
                        "v0",
                        NullHandling.replaceWithDefault() ? "0" : "isnull(\"l1\")",
                        ColumnType.LONG
                    )
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{false},
            new Object[]{false},
            new Object[]{false},
            new Object[]{false},
            new Object[]{false},
            new Object[]{false}
        ) :
        ImmutableList.of(
            new Object[]{false},
            new Object[]{false},
            new Object[]{false},
            new Object[]{true},
            new Object[]{true},
            new Object[]{true}
        )
    );
  }

  @Test
  public void testLongPredicateFilterNulls()
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE l1 > 3",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(range("l1", ColumnType.LONG, 3L, null, true, false))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{2L})
    );
  }

  @Test
  public void testDoublePredicateFilterNulls()
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE d1 > 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(range("d1", ColumnType.LONG, 0L, null, true, false))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{2L})
    );
  }

  @Test
  public void testFloatPredicateFilterNulls()
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE f1 > 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(range("f1", ColumnType.LONG, 0L, null, true, false))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{2L})
    );
  }

  @Test
  public void testEmptyStringEquality()
  {
    if (NullHandling.replaceWithDefault()) {
      testQuery(
          "SELECT COUNT(*)\n"
          + "FROM druid.foo\n"
          + "WHERE NULLIF(dim2, 'a') = ''",
          ImmutableList.of(
              Druids.newTimeseriesQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .granularity(Granularities.ALL)
                    .filters(in("dim2", ImmutableList.of("", "a"), null))
                    .aggregators(aggregators(new CountAggregatorFactory("a0")))
                    .context(QUERY_CONTEXT_DEFAULT)
                    .build()
          ),
          ImmutableList.of(
              // Matches everything but "abc"
              new Object[]{5L}
          )
      );
    } else {
      testQuery(
          "SELECT COUNT(*)\n"
          + "FROM druid.foo\n"
          + "WHERE NULLIF(dim2, 'a') = ''",
          ImmutableList.of(
              Druids.newTimeseriesQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .granularity(Granularities.ALL)
                    .filters(equality("dim2", "", ColumnType.STRING))
                    .aggregators(aggregators(new CountAggregatorFactory("a0")))
                    .context(QUERY_CONTEXT_DEFAULT)
                    .build()
          ),
          ImmutableList.of(
              // match only empty string
              new Object[]{1L}
          )
      );
    }
  }

  @Test
  public void testNullStringEquality()
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE NULLIF(dim2, 'a') = null",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(new Object[]{0L}),
                          RowSignature.builder().add("EXPR$0", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("EXPR$0")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(new Object[]{0L})
    );
  }

  @Test
  public void testCoalesceColumns()
  {
    // Doesn't conform to the SQL standard, but it's how we do it.
    // This example is used in the sql.md doc.

    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COALESCE(dim2, dim1), COUNT(*) FROM druid.foo GROUP BY COALESCE(dim2, dim1)\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",\"dim1\")",
                                ColumnType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"a", 2L},
            new Object[]{"abc", 2L}
        ) :
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"a", 2L},
            new Object[]{"abc", 2L}
        )
    );
  }

  @Test
  public void testCoalesceColumnsFilter()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COALESCE(dim2, dim1), COUNT(*) FROM druid.foo WHERE COALESCE(dim2, dim1) IN ('a', 'abc') GROUP BY COALESCE(dim2, dim1)\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",\"dim1\")",
                                ColumnType.STRING
                            )
                        )
                        .setDimFilter(or(
                            in("dim2", ImmutableSet.of("a", "abc"), null),
                            and(isNull("dim2"), in("dim1", ImmutableList.of("a", "abc"), null))
                        ))
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 2L},
            new Object[]{"abc", 2L}
        )
    );
  }

  @Test
  public void testCoalesceColumnsFilterWithEquality()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    // we can remove this test if PlannerContext.CTX_SQL_USE_BOUNDS_AND_SELECTORS ever defaults to false all the time
    // since it otherwise is a duplicate of testCoalesceColumnsFilter

    testQuery(
        "SELECT COALESCE(dim2, dim1), COUNT(*) FROM druid.foo WHERE COALESCE(dim2, dim1) IN ('a', 'abc') GROUP BY COALESCE(dim2, dim1)",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY_USE_EQUALITY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",\"dim1\")",
                                ColumnType.STRING
                            )
                        )
                        .setDimFilter(or(
                            in("dim2", ImmutableSet.of("a", "abc"), null),
                            and(new NullFilter("dim2", null), in("dim1", ImmutableList.of("a", "abc"), null))
                        ))
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY_USE_EQUALITY)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 2L},
            new Object[]{"abc", 2L}
        )
    );
  }

  @Test
  public void testCoalesceMoreColumns()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COALESCE(dim2, dim1), COALESCE(dim2, dim3, dim1), COUNT(*) FROM druid.foo GROUP BY COALESCE(dim2, dim1), COALESCE(dim2, dim3, dim1)\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",\"dim1\")",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "coalesce(\"dim2\",\"dim3\",\"dim1\")",
                                ColumnType.STRING
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.STRING),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"10.1", "b", 1L},
            new Object[]{"10.1", "c", 1L},
            new Object[]{"2", "d", 1L},
            new Object[]{"a", "a", 3L},
            new Object[]{"abc", "abc", 2L}
        ) :
        ImmutableList.of(
            new Object[]{"", "", 1L},
            new Object[]{"10.1", "b", 1L},
            new Object[]{"10.1", "c", 1L},
            new Object[]{"a", "a", 3L},
            new Object[]{"abc", "abc", 2L}
        )
    );
  }

  @Test
  public void testColumnIsNull()
  {
    // Doesn't conform to the SQL standard, but it's how we do it.
    // This example is used in the sql.md doc.
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 IS NULL\n",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(isNull("dim2"))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 3L : 2L}
        )
    );
  }

  @Test
  public void testGroupingWithNullInFilter()
  {
    msqIncompatible();
    testQuery(
        "SELECT COUNT(*) FROM foo WHERE dim1 IN (NULL)",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(new Object[]{0L}),
                        RowSignature.builder().add("EXPR$0", ColumnType.LONG).build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("EXPR$0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(new Object[]{0L})
    );
  }


  @Test
  public void testGroupByNothingWithLiterallyFalseFilter()
  {
    msqIncompatible();
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE 1 = 0",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(new Object[]{0L, null}),
                          RowSignature.builder().add("EXPR$0", ColumnType.LONG).add("EXPR$1", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("EXPR$0", "EXPR$1")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{0L, null}
        )
    );
  }

  @Test
  public void testGroupByNothingWithImpossibleTimeFilter()
  {
    msqIncompatible();
    // Regression test for https://github.com/apache/druid/issues/7671

    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE FLOOR(__time TO DAY) = TIMESTAMP '2000-01-02 01:00:00'\n"
        + "OR FLOOR(__time TO DAY) = TIMESTAMP '2000-01-02 02:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec())
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{0L}
        )
    );
  }

  @Test
  public void testGroupByWithImpossibleTimeFilter()
  {
    // this gets optimized into 'false'
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo\n"
        + "WHERE FLOOR(__time TO DAY) = TIMESTAMP '2000-01-02 01:00:00'\n"
        + "OR FLOOR(__time TO DAY) = TIMESTAMP '2000-01-02 02:00:00'\n"
        + "GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of()))
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setGranularity(Granularities.ALL)
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupByOneColumnWithLiterallyFalseFilter()
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE 1 = 0 GROUP BY dim1",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(),
                          RowSignature.builder().add("EXPR$0", ColumnType.LONG).add("EXPR$1", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("EXPR$0", "EXPR$1")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupByWithFilterMatchingNothing()
  {
    msqIncompatible();
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE dim1 = 'foobar'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(equality("dim1", "foobar", ColumnType.STRING))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new CountAggregatorFactory("a0"),
                      new LongMaxAggregatorFactory("a1", "cnt")
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{0L, NullHandling.sqlCompatible() ? null : Long.MIN_VALUE}
        )
    );
  }

  @Test
  public void testGroupByWithGroupByEmpty()
  {
    testQuery(
        "SELECT COUNT(*), SUM(cnt), MIN(cnt) FROM druid.foo GROUP BY ()",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new CountAggregatorFactory("a0"),
                      new LongSumAggregatorFactory("a1", "cnt"),
                      new LongMinAggregatorFactory("a2", "cnt")
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{6L, 6L, 1L})
    );
  }

  @Test
  public void testGroupByWithFilterMatchingNothingWithGroupByLiteral()
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE dim1 = 'foobar' GROUP BY 'dummy'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(equality("dim1", "foobar", ColumnType.STRING))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new CountAggregatorFactory("a0"),
                      new LongMaxAggregatorFactory("a1", "cnt")
                  ))
                  .context(QUERY_CONTEXT_DO_SKIP_EMPTY_BUCKETS)
                  .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testCountNonNullColumn()
  {
    testQuery(
        "SELECT COUNT(cnt) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          useDefault
                          ? new CountAggregatorFactory("a0")
                          : new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a0"),
                              notNull("cnt")
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountNullableColumn()
  {
    testQuery(
        "SELECT COUNT(dim2) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a0"),
                          notNull("dim2")
                      )
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{3L}
        ) :
        ImmutableList.of(
            new Object[]{4L}
        )
    );
  }

  @Test
  public void testCountNullableExpression()
  {
    testQuery(
        "SELECT COUNT(CASE WHEN dim2 = 'abc' THEN 'yes' WHEN dim2 = 'def' THEN 'yes' END) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a0"),
                          in("dim2", ImmutableList.of("abc", "def"), null)
                      )
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStar()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarOnCommonTableExpression()
  {
    if (NullHandling.sqlCompatible()) {
      // cannot vectorize due to substring expression
      cannotVectorize();
    }
    Druids.TimeseriesQueryBuilder builder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(querySegmentSpec(Filtration.eternity()))
              .granularity(Granularities.ALL)
              .aggregators(aggregators(new CountAggregatorFactory("a0")))
              .context(QUERY_CONTEXT_DEFAULT);
    if (NullHandling.sqlCompatible()) {
      builder = builder.virtualColumns(expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING))
                       .filters(
                           and(
                               equality("dim2", "a", ColumnType.STRING),
                               not(equality("v0", "z", ColumnType.STRING))
                           )
                       );
    } else {
      builder = builder.filters(
          and(
              equality("dim2", "a", ColumnType.STRING),
              not(selector("dim1", "z", new SubstringDimExtractionFn(0, 1)))
          )
      );
    }
    testQuery(
        "WITH beep (dim1_firstchar) AS (SELECT SUBSTRING(dim1, 1, 1) FROM foo WHERE dim2 = 'a')\n"
        + "SELECT COUNT(*) FROM beep WHERE dim1_firstchar <> 'z'",
        ImmutableList.of(builder.build()),
        ImmutableList.of(
            // in sql compatible mode, the expression filter causes us to correctly not match null
            new Object[]{NullHandling.replaceWithDefault() ? 2L : 1L}
        )
    );
  }

  @Test
  public void testCountStarOnView()
  {
    if (NullHandling.sqlCompatible()) {
      // cannot vectorize due to substring expression
      cannotVectorize();
    }

    Druids.TimeseriesQueryBuilder builder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(querySegmentSpec(Filtration.eternity()))
              .granularity(Granularities.ALL)
              .aggregators(aggregators(new CountAggregatorFactory("a0")))
              .context(QUERY_CONTEXT_DEFAULT);
    if (NullHandling.sqlCompatible()) {
      builder = builder.virtualColumns(expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING))
                       .filters(
                           and(
                               equality("dim2", "a", ColumnType.STRING),
                               not(equality("v0", "z", ColumnType.STRING))
                           )
                       );
    } else {
      builder = builder.filters(
          and(
              equality("dim2", "a", ColumnType.STRING),
              not(selector("dim1", "z", new SubstringDimExtractionFn(0, 1)))
          )
      );
    }
    testQuery(
        "SELECT COUNT(*) FROM view.aview WHERE dim1_firstchar <> 'z'",
        ImmutableList.of(builder.build()),
        ImmutableList.of(
            // in sql compatible mode, the expression filter causes us to correctly not match null
            new Object[]{NullHandling.replaceWithDefault() ? 2L : 1L}
        )
    );
  }

  @Test
  public void testConfusedView()
  {
    if (NullHandling.sqlCompatible()) {
      // cannot vectorize due to substring expression
      cannotVectorize();
    }
    Druids.TimeseriesQueryBuilder builder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(querySegmentSpec(Filtration.eternity()))
              .granularity(Granularities.ALL)
              .aggregators(aggregators(new CountAggregatorFactory("a0")))
              .context(QUERY_CONTEXT_DEFAULT);
    if (NullHandling.sqlCompatible()) {
      builder = builder.virtualColumns(
                           expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING)
                       )
                       .filters(
                           and(
                               equality("dim2", "a", ColumnType.STRING),
                               not(equality("v0", "z", ColumnType.STRING))
                           )
                       );
    } else {
      builder = builder.filters(
          and(
              equality("dim2", "a", ColumnType.STRING),
              not(selector("dim1", "z", new SubstringDimExtractionFn(0, 1)))
          )
      );
    }
    testQuery(
        "SELECT COUNT(*) FROM view.dview as druid WHERE druid.numfoo <> 'z'",
        ImmutableList.of(builder.build()),
        ImmutableList.of(
            // in sql compatible mode, the expression filter causes us to correctly not match null
            new Object[]{NullHandling.replaceWithDefault() ? 2L : 1L}
        )
    );
  }

  @Test
  public void testCountStarWithLikeFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim1 like 'a%' OR dim2 like '%xb%' escape 'x'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      or(
                          new LikeDimFilter("dim1", "a%", null, null),
                          new LikeDimFilter("dim2", "%xb%", "x", null)
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarWithLongColumnFilters()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt >= 3 OR cnt = 1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      or(
                          equality("cnt", 1L, ColumnType.LONG),
                          range("cnt", ColumnType.LONG, 3L, null, false, false)
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithLongColumnFiltersForceRange()
  {
    // we can remove this test if PlannerContext.CTX_SQL_USE_BOUNDS_AND_SELECTORS ever defaults to false all the time
    // since it otherwise is a duplicate of testCountStarWithLongColumnFilters
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt >= 3 OR cnt = 1",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY_USE_EQUALITY,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      or(
                          new EqualityFilter("cnt", ColumnType.LONG, 1L, null),
                          new RangeFilter("cnt", ColumnType.LONG, 3L, null, false, false, null)
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_NO_STRINGIFY_ARRAY_USE_EQUALITY)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }


  @Test
  public void testCountStarWithLongColumnFiltersOnFloatLiterals()
  {
    msqIncompatible();
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt > 1.1 and cnt < 100000001.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      NullHandling.replaceWithDefault()
                      ? bound("cnt", "1.1", "100000001.0", true, true, null, StringComparators.NUMERIC)
                      : range("cnt", ColumnType.DOUBLE, 1.1, 100000001.0, true, true)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{0L}
        )
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      equality("cnt", 1.0, ColumnType.DOUBLE)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 100000001.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      NullHandling.replaceWithDefault()
                      ? selector("cnt", "100000001.0")
                      : equality("cnt", 100000001.0, ColumnType.DOUBLE)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{0L}
        )
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1.0 or cnt = 100000001.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      NullHandling.replaceWithDefault()
                      ? in("cnt", ImmutableList.of("1.0", "100000001.0"), null)
                      : or(equality("cnt", 1.0, ColumnType.DOUBLE), equality("cnt", 1.00000001E8, ColumnType.DOUBLE))
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithLongColumnFiltersOnTwoPoints()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1 OR cnt = 2",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      NullHandling.replaceWithDefault()
                      ? in("cnt", ImmutableList.of("1", "2"), null)
                      : or(
                          equality("cnt", 1L, ColumnType.LONG),
                          equality("cnt", 2L, ColumnType.LONG)
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testFilterOnStringAsNumber()
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo WHERE "
        + "dim1 = 10 OR "
        + "(floor(CAST(dim1 AS float)) = 10.00 and CAST(dim1 AS float) > 9 and CAST(dim1 AS float) <= 10.5)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "floor(CAST(\"dim1\", 'DOUBLE'))",
                                ColumnType.DOUBLE
                            )
                        )
                        .setDimFilter(
                            NullHandling.replaceWithDefault()
                            ? or(
                                numericSelector("dim1", "10"),
                                and(
                                    selector("v0", "10.00"),
                                    bound("dim1", "9", "10.5", true, false, null, StringComparators.NUMERIC)
                                )
                            )
                            : or(
                                equality("dim1", 10L, ColumnType.LONG),
                                and(
                                    equality("v0", 10.0, ColumnType.DOUBLE),
                                    range("dim1", ColumnType.DOUBLE, 9.0, 10.5, true, false)
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  public void testFilterOnStringAsNumber2()
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo WHERE CAST(dim1 AS float) > 9 and CAST(dim1 AS float) <= 10.5",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(
                            NullHandling.replaceWithDefault()
                            ? bound("dim1", "9", "10.5", true, false, null, StringComparators.NUMERIC)
                            : range("dim1", ColumnType.DOUBLE, 9.0, 10.5, true, false)
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  public void testSimpleLongAggregations()
  {
    testQuery(
        "SELECT  MIN(l1), MIN(cnt), MAX(l1) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new LongMinAggregatorFactory("a0", "l1"),
                      new LongMinAggregatorFactory("a1", "cnt"),
                      new LongMaxAggregatorFactory("a2", "l1")
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{0L, 1L, 325323L}
        )
    );
  }

  @Test
  public void testSimpleDoubleAggregations()
  {
    testQuery(
        "SELECT  MIN(d1), MAX(d1) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new DoubleMinAggregatorFactory("a0", "d1"),
                      new DoubleMaxAggregatorFactory("a1", "d1")
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{0.0, 1.7}
        )
    );
  }

  @Test
  public void testSimpleFloatAggregations()
  {
    testQuery(
        "SELECT  MIN(m1), MAX(m1) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new FloatMinAggregatorFactory("a0", "m1"),
                      new FloatMaxAggregatorFactory("a1", "m1")
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1.0f, 6.0f}
        )
    );
  }

  @Test
  public void testSimpleAggregations()
  {
    testQuery(
        "SELECT COUNT(*), COUNT(cnt), COUNT(dim1), AVG(cnt), SUM(cnt), SUM(cnt) + MIN(cnt) + MAX(cnt), COUNT(dim2), COUNT(d1), AVG(d1) FROM druid.numfoo",
        ImmutableList.of(

            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      useDefault
                      ? aggregators(
                          new CountAggregatorFactory("a0"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a1"),
                              notNull("dim1")
                          ),
                          new DoubleSumAggregatorFactory("a2:sum", "cnt"),
                          new CountAggregatorFactory("a2:count"),
                          new LongSumAggregatorFactory("a3", "cnt"),
                          new LongMinAggregatorFactory("a4", "cnt"),
                          new LongMaxAggregatorFactory("a5", "cnt"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a6"),
                              notNull("dim2")
                          ),
                          new DoubleSumAggregatorFactory("a7:sum", "d1"),
                          new CountAggregatorFactory("a7:count")
                      )
                      : aggregators(
                          new CountAggregatorFactory("a0"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a1"),
                              notNull("cnt")
                          ),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a2"),
                              notNull("dim1")
                          ),
                          new DoubleSumAggregatorFactory("a3:sum", "cnt"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a3:count"),
                              notNull("cnt")
                          ),
                          new LongSumAggregatorFactory("a4", "cnt"),
                          new LongMinAggregatorFactory("a5", "cnt"),
                          new LongMaxAggregatorFactory("a6", "cnt"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a7"),
                              notNull("dim2")
                          ),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a8"),
                              notNull("d1")
                          ),
                          new DoubleSumAggregatorFactory("a9:sum", "d1"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a9:count"),
                              notNull("d1")
                          )
                      )
                  )
                  .postAggregators(
                      new ArithmeticPostAggregator(
                          useDefault ? "a2" : "a3",
                          "quotient",
                          ImmutableList.of(
                              new FieldAccessPostAggregator(null, useDefault ? "a2:sum" : "a3:sum"),
                              new FieldAccessPostAggregator(null, useDefault ? "a2:count" : "a3:count")
                          )
                      ),
                      new ArithmeticPostAggregator(
                          useDefault ? "a7" : "a9",
                          "quotient",
                          ImmutableList.of(
                              new FieldAccessPostAggregator(null, useDefault ? "a7:sum" : "a9:sum"),
                              new FieldAccessPostAggregator(null, useDefault ? "a7:count" : "a9:count")
                          )
                      ),
                      expressionPostAgg(
                          "p0",
                          useDefault ? "((\"a3\" + \"a4\") + \"a5\")" : "((\"a4\" + \"a5\") + \"a6\")",
                          ColumnType.LONG
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{6L, 6L, 5L, 1.0, 6L, 8L, 3L, 6L, ((1 + 1.7) / 6)}
        ) :
        ImmutableList.of(
            new Object[]{6L, 6L, 6L, 1.0, 6L, 8L, 4L, 3L, ((1 + 1.7) / 3)}
        )
    );
  }

  @NotYetSupported(Modes.PLAN_MISMATCH)
  @Test
  public void testGroupByWithSortOnPostAggregationDefault()
  {
    // By default this query uses topN.

    testQuery(
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "d0"))
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("p0")))
                .aggregators(
                    new FloatMinAggregatorFactory("a0", "m1"),
                    new FloatMaxAggregatorFactory("a1", "m1")
                )
                .postAggregators(expressionPostAgg("p0", "(\"a0\" + \"a1\")", ColumnType.FLOAT))
                .threshold(3)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2.0f},
            new Object[]{"10.1", 4.0f},
            new Object[]{"2", 6.0f}
        )
    );
  }

  @NotYetSupported(Modes.PLAN_MISMATCH)
  @Test
  public void testGroupByWithSortOnPostAggregationNoTopNConfig()
  {
    // Use PlannerConfig to disable topN, so this query becomes a groupBy.
    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(
                            new FloatMinAggregatorFactory("a0", "m1"),
                            new FloatMaxAggregatorFactory("a1", "m1")
                        )
                        .setPostAggregatorSpecs(expressionPostAgg("p0", "(\"a0\" + \"a1\")", ColumnType.FLOAT))
                        .setLimitSpec(
                            DefaultLimitSpec
                                .builder()
                                .orderBy(
                                    new OrderByColumnSpec(
                                        "p0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                )
                                .limit(3)
                                .build()
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2.0f},
            new Object[]{"10.1", 4.0f},
            new Object[]{"2", 6.0f}
        )
    );
  }

  @NotYetSupported(Modes.PLAN_MISMATCH)
  @Test
  public void testGroupByWithSortOnPostAggregationNoTopNContext()
  {
    // Use context to disable topN, so this query becomes a groupBy.

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_NO_TOPN,
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(
                            new FloatMinAggregatorFactory("a0", "m1"),
                            new FloatMaxAggregatorFactory("a1", "m1")
                        )
                        .setPostAggregatorSpecs(
                            expressionPostAgg("p0", "(\"a0\" + \"a1\")", ColumnType.FLOAT)
                        )
                        .setLimitSpec(
                            DefaultLimitSpec
                                .builder()
                                .orderBy(
                                    new OrderByColumnSpec(
                                        "p0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                )
                                .limit(3)
                                .build()
                        )
                        .setContext(QUERY_CONTEXT_NO_TOPN)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2.0f},
            new Object[]{"10.1", 4.0f},
            new Object[]{"2", 6.0f}
        )
    );
  }

  @Test
  public void testFilteredAggregations()
  {
    if (NullHandling.sqlCompatible()) {
      // cannot vectorize due to expression filter
      cannotVectorize();
    }
    Druids.TimeseriesQueryBuilder builder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(querySegmentSpec(Filtration.eternity()))
              .granularity(Granularities.ALL)
              .context(QUERY_CONTEXT_DEFAULT);
    if (NullHandling.sqlCompatible()) {
      builder = builder.virtualColumns(expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING))
                       .aggregators(
                           aggregators(
                               new FilteredAggregatorFactory(
                                   new LongSumAggregatorFactory("a0", "cnt"),
                                   equality("dim1", "abc", ColumnType.STRING)
                               ),
                               new FilteredAggregatorFactory(
                                   new LongSumAggregatorFactory("a1", "cnt"),
                                   not(istrue(equality("dim1", "abc", ColumnType.STRING)))
                               ),
                               new FilteredAggregatorFactory(
                                   new LongSumAggregatorFactory("a2", "cnt"),
                                   equality("v0", "a", ColumnType.STRING)
                               ),
                               new FilteredAggregatorFactory(
                                   new CountAggregatorFactory("a3"),
                                   and(
                                       notNull("dim2"),
                                       not(equality("dim1", "1", ColumnType.STRING))
                                   )
                               ),
                               new FilteredAggregatorFactory(
                                   new CountAggregatorFactory("a4"),
                                   not(equality("dim1", "1", ColumnType.STRING))
                               ),
                               new FilteredAggregatorFactory(
                                   new CountAggregatorFactory("a5"),
                                   not(equality("dim1", "1", ColumnType.STRING))
                               ),
                               new FilteredAggregatorFactory(
                                   new LongSumAggregatorFactory("a6", "cnt"),
                                   equality("dim2", "a", ColumnType.STRING)
                               ),
                               new FilteredAggregatorFactory(
                                   new LongSumAggregatorFactory("a7", "cnt"),
                                   and(
                                       equality("dim2", "a", ColumnType.STRING),
                                       not(equality("dim1", "1", ColumnType.STRING))
                                   )
                               ),
                               new FilteredAggregatorFactory(
                                   new LongSumAggregatorFactory("a8", "cnt"),
                                   not(equality("dim1", "1", ColumnType.STRING))
                               ),
                               new FilteredAggregatorFactory(
                                   new LongMaxAggregatorFactory("a9", "cnt"),
                                   not(equality("dim1", "1", ColumnType.STRING))
                               ),
                               new FilteredAggregatorFactory(
                                   new CardinalityAggregatorFactory(
                                       "a10",
                                       null,
                                       dimensions(new DefaultDimensionSpec("m1", "m1", ColumnType.FLOAT)),
                                       false,
                                       true
                                   ),
                                   not(equality("dim1", "1", ColumnType.STRING))
                               ),
                               new FilteredAggregatorFactory(
                                   new LongSumAggregatorFactory("a11", "cnt"),
                                   and(
                                       equality("dim2", "a", ColumnType.STRING),
                                       equality("dim1", "b", ColumnType.STRING)
                                   )
                               )
                           )
                       );
    } else {
      builder = builder.aggregators(
          aggregators(
              new FilteredAggregatorFactory(
                  new LongSumAggregatorFactory("a0", "cnt"),
                  equality("dim1", "abc", ColumnType.STRING)
              ),
              new FilteredAggregatorFactory(
                  new LongSumAggregatorFactory("a1", "cnt"),
                  not(selector("dim1", "abc"))
              ),
              new FilteredAggregatorFactory(
                  new LongSumAggregatorFactory("a2", "cnt"),
                  selector("dim1", "a", new SubstringDimExtractionFn(0, 1))

              ),
              new FilteredAggregatorFactory(
                  new CountAggregatorFactory("a3"),
                  and(
                      notNull("dim2"),
                      not(equality("dim1", "1", ColumnType.STRING))
                  )
              ),
              new FilteredAggregatorFactory(
                  new CountAggregatorFactory("a4"),
                  not(equality("dim1", "1", ColumnType.STRING))
              ),
              new FilteredAggregatorFactory(
                  new CountAggregatorFactory("a5"),
                  not(equality("dim1", "1", ColumnType.STRING))
              ),
              new FilteredAggregatorFactory(
                  new LongSumAggregatorFactory("a6", "cnt"),
                  equality("dim2", "a", ColumnType.STRING)
              ),
              new FilteredAggregatorFactory(
                  new LongSumAggregatorFactory("a7", "cnt"),
                  and(
                      equality("dim2", "a", ColumnType.STRING),
                      not(equality("dim1", "1", ColumnType.STRING))
                  )
              ),
              new FilteredAggregatorFactory(
                  new LongSumAggregatorFactory("a8", "cnt"),
                  not(equality("dim1", "1", ColumnType.STRING))
              ),
              new FilteredAggregatorFactory(
                  new LongMaxAggregatorFactory("a9", "cnt"),
                  not(equality("dim1", "1", ColumnType.STRING))
              ),
              new FilteredAggregatorFactory(
                  new CardinalityAggregatorFactory(
                      "a10",
                      null,
                      dimensions(new DefaultDimensionSpec("m1", "m1", ColumnType.FLOAT)),
                      false,
                      true
                  ),
                  not(equality("dim1", "1", ColumnType.STRING))
              ),
              new FilteredAggregatorFactory(
                  new LongSumAggregatorFactory("a11", "cnt"),
                  and(
                      equality("dim2", "a", ColumnType.STRING),
                      equality("dim1", "b", ColumnType.STRING)
                  )
              )
          )
      );
    }
    testQuery(
        "SELECT "
        + "SUM(case dim1 when 'abc' then cnt end), "
        + "SUM(case dim1 when 'abc' then null else cnt end), "
        + "SUM(case substring(dim1, 1, 1) when 'a' then cnt end), "
        + "COUNT(dim2) filter(WHERE dim1 <> '1'), "
        + "COUNT(CASE WHEN dim1 <> '1' THEN 'dummy' END), "
        + "SUM(CASE WHEN dim1 <> '1' THEN 1 ELSE 0 END), "
        + "SUM(cnt) filter(WHERE dim2 = 'a'), "
        + "SUM(case when dim1 <> '1' then cnt end) filter(WHERE dim2 = 'a'), "
        + "SUM(CASE WHEN dim1 <> '1' THEN cnt ELSE 0 END), "
        + "MAX(CASE WHEN dim1 <> '1' THEN cnt END), "
        + "COUNT(DISTINCT CASE WHEN dim1 <> '1' THEN m1 END), "
        + "SUM(cnt) filter(WHERE dim2 = 'a' AND dim1 = 'b') "
        + "FROM druid.foo",
        ImmutableList.of(builder.build()),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{1L, 5L, 1L, 2L, 5L, 5L, 2L, 1L, 5L, 1L, 5L, 0L}
        ) :
        ImmutableList.of(
            new Object[]{1L, 5L, 1L, 3L, 5L, 5L, 2L, 1L, 5L, 1L, 5L, null}
        )
    );
  }

  @Test
  public void testCaseFilteredAggregationWithGroupBy()
  {
    testQuery(
        "SELECT\n"
        + "  cnt,\n"
        + "  SUM(CASE WHEN dim1 <> '1' THEN 1 ELSE 0 END) + SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY cnt",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("a0"),
                                not(equality("dim1", "1", ColumnType.STRING))
                            ),
                            new LongSumAggregatorFactory("a1", "cnt")
                        ))
                        .setPostAggregatorSpecs(
                            expressionPostAgg("p0", "(\"a0\" + \"a1\")", ColumnType.LONG)
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 11L}
        )
    );
  }

  @Test
  public void testFilteredAggregationWithNotIn()
  {
    testQuery(
        "SELECT\n"
        + "COUNT(*) filter(WHERE dim1 NOT IN ('1')),\n"
        + "COUNT(dim2) filter(WHERE dim1 NOT IN ('1'))\n"
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a0"),
                              not(equality("dim1", "1", ColumnType.STRING))
                          ),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a1"),
                              and(
                                  notNull("dim2"),
                                  not(equality("dim1", "1", ColumnType.STRING))
                              )
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{5L, 2L}
        ) :
        ImmutableList.of(
            new Object[]{5L, 3L}
        )
    );
  }

  @Test
  public void testExpressionAggregations()
  {
    // Cannot vectorize due to expressions.
    cannotVectorize();

    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();

    testQuery(
        "SELECT\n"
        + "  SUM(cnt * 3),\n"
        + "  LN(SUM(cnt) + SUM(m1)),\n"
        + "  MOD(SUM(cnt), 4),\n"
        + "  SUM(CHARACTER_LENGTH(CAST(cnt * 10 AS VARCHAR))),\n"
        + "  MAX(CHARACTER_LENGTH(dim2) + LN(m1)),\n"
        + "  MIN(CHARACTER_LENGTH(dim2) + LN(m1))\n"
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"cnt\" * 3)", ColumnType.LONG),
                      expressionVirtualColumn("v1", "strlen(CAST((\"cnt\" * 10), 'STRING'))", ColumnType.LONG),
                      expressionVirtualColumn("v2", "(strlen(\"dim2\") + log(\"m1\"))", ColumnType.DOUBLE)
                  )
                  .aggregators(aggregators(
                      new LongSumAggregatorFactory("a0", "v0", null, macroTable),
                      new LongSumAggregatorFactory("a1", "cnt"),
                      new DoubleSumAggregatorFactory("a2", "m1"),
                      new LongSumAggregatorFactory("a3", "v1", null, macroTable),
                      new DoubleMaxAggregatorFactory("a4", "v2", null, macroTable),
                      new DoubleMinAggregatorFactory("a5", "v2", null, macroTable)
                  ))
                  .postAggregators(
                      expressionPostAgg("p0", "log((\"a1\" + \"a2\"))", ColumnType.DOUBLE),
                      expressionPostAgg("p1", "(\"a1\" % 4)", ColumnType.LONG)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{18L, 3.295836866004329, 2, 12L, 3f + (Math.log(5.0)), useDefault ? 0.6931471805599453 : 1.0}
        )
    );
  }

  @Test
  public void testExpressionFilteringAndGrouping()
  {
    testQuery(
        "SELECT\n"
        + "  FLOOR(m1 / 2) * 2,\n"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE FLOOR(m1 / 2) * 2 > -1\n"
        + "GROUP BY FLOOR(m1 / 2) * 2\n"
        + "ORDER BY 1 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "(floor((\"m1\" / 2)) * 2)", ColumnType.FLOAT)
                        )
                        .setDimFilter(range("v0", ColumnType.LONG, -1L, null, true, false))
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.FLOAT)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            DefaultLimitSpec
                                .builder()
                                .orderBy(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                )
                                .build()
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{6.0f, 1L},
            new Object[]{4.0f, 2L},
            new Object[]{2.0f, 2L},
            new Object[]{0.0f, 1L}
        )
    );
  }

  @Test
  public void testExpressionFilteringAndGroupingUsingCastToLong()
  {
    testQuery(
        "SELECT\n"
        + "  CAST(m1 AS BIGINT) / 2 * 2,\n"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE CAST(m1 AS BIGINT) / 2 * 2 > -1\n"
        + "GROUP BY CAST(m1 AS BIGINT) / 2 * 2\n"
        + "ORDER BY 1 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "((CAST(\"m1\", 'LONG') / 2) * 2)", ColumnType.LONG)
                        )
                        .setDimFilter(
                            range("v0", ColumnType.LONG, -1L, null, true, false)
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 1L},
            new Object[]{4L, 2L},
            new Object[]{2L, 2L},
            new Object[]{0L, 1L}
        )
    );
  }

  @Test
  public void testExpressionFilteringAndGroupingOnStringCastToNumber()
  {
    testQuery(
        "SELECT\n"
        + "  FLOOR(CAST(dim1 AS FLOAT) / 2) * 2,\n"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE FLOOR(CAST(dim1 AS FLOAT) / 2) * 2 > -1\n"
        + "GROUP BY FLOOR(CAST(dim1 AS FLOAT) / 2) * 2\n"
        + "ORDER BY 1 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "(floor((CAST(\"dim1\", 'DOUBLE') / 2)) * 2)",
                                ColumnType.FLOAT
                            )
                        )
                        .setDimFilter(
                            range("v0", ColumnType.LONG, -1L, null, true, false)
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.FLOAT)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{10.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{0.0f, 4L}
        ) :
        ImmutableList.of(
            new Object[]{10.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{0.0f, 1L}
        )
    );
  }

  @Test
  public void testInFilter()
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 IN ('abc', 'def', 'ghi') GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(new InDimFilter("dim1", ImmutableList.of("abc", "def", "ghi"), null))
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testNotInFilter()
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 NOT IN ('abc', 'def', 'ghi') GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(not(in("dim1", ImmutableList.of("abc", "def", "ghi"), null)))
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L}
        )
    );
  }

  @Test
  public void testInIsNotTrueFilter()
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 IN ('abc', 'def', 'ghi') IS NOT TRUE GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(
                            NullHandling.sqlCompatible()
                            ? not(istrue(in("dim1", ImmutableList.of("abc", "def", "ghi"), null)))
                            : not(in("dim1", ImmutableList.of("abc", "def", "ghi"), null)))
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L}
        )
    );
  }

  @Test
  public void testNotInOrIsNullFilter()
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 NOT IN ('abc', 'def', 'ghi') OR dim1 IS NULL GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(or(
                            isNull("dim1"),
                            not(in("dim1", ImmutableList.of("abc", "def", "ghi"), null))
                        ))
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L}
        )
    );
  }

  @Test
  public void testNotInAndLessThanFilter()
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 NOT IN ('abc', 'def', 'ghi') AND dim1 < 'zzz' GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(
                            and(
                                range("dim1", ColumnType.STRING, null, "zzz", false, true),
                                not(in("dim1", ImmutableList.of("abc", "def", "ghi"), null))
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L}
        )
    );
  }

  @Test
  public void testInIsNotTrueAndLessThanFilter()
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 IN ('abc', 'def', 'ghi') IS NOT TRUE "
        + "AND dim1 < 'zzz' GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(
                            NullHandling.sqlCompatible()
                            ? and(
                                not(istrue(in("dim1", ImmutableList.of("abc", "def", "ghi"), null))),
                                range("dim1", ColumnType.STRING, null, "zzz", false, true)
                            )
                            : and(
                                not(in("dim1", ImmutableList.of("abc", "def", "ghi"), null)),
                                range("dim1", ColumnType.STRING, null, "zzz", false, true)
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L}
        )
    );
  }

  @Test
  public void testSqlIsNullToInFilter()
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 IS NULL OR dim1 = 'abc' OR dim1 = 'def' OR dim1 = 'ghi' "
        + "GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(
                            NullHandling.replaceWithDefault()
                            ? in("dim1", Arrays.asList("abc", "def", "ghi", ""), null)
                            : or(
                                in("dim1", Arrays.asList("abc", "def", "ghi"), null),
                                isNull("dim1")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible() ? ImmutableList.of(
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        ) : ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testInFilterWith23Elements()
  {
    // Regression test for https://github.com/apache/druid/issues/4203.
    final List<String> elements = new ArrayList<>();
    elements.add("abc");
    elements.add("def");
    elements.add("ghi");
    for (int i = 0; i < 20; i++) {
      elements.add("dummy" + i);
    }

    final String elementsString = Joiner.on(",").join(elements.stream().map(s -> "'" + s + "'").iterator());

    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 IN (" + elementsString + ") GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(new InDimFilter("dim1", elements, null))
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testCountStarWithDegenerateFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 = 'a' and (dim1 > 'a' OR dim1 < 'b' OR dim1 IS NULL)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      equality("dim2", "a", ColumnType.STRING)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }


  @Test
  public void testCountStarWithNotOfDegenerateFilter()
  {

    msqIncompatible();
    // HashJoinSegmentStorageAdapter is not vectorizable
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 = 'a' and not (dim1 > 'a' OR dim1 < 'b')",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(new Object[]{0L}),
                          RowSignature.builder().add("EXPR$0", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("EXPR$0")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{0L}
        )
    );
  }

  @NotYetSupported(Modes.ERROR_HANDLING)
  @Test
  public void testUnplannableScanOrderByNonTime()
  {
    msqIncompatible();
    // All of these queries are unplannable because they rely on features Druid doesn't support.
    // This test is here to confirm that we don't fall back to Calcite's interpreter or enumerable implementation.
    // It's also here so when we do support these features, we can have "real" tests for these queries.

    assertQueryIsUnplannable(
        "SELECT dim1 FROM druid.foo ORDER BY dim1",
        "SQL query requires ordering a table by non-time column [[dim1]], which is not supported."
    );
  }

  @NotYetSupported(Modes.ERROR_HANDLING)
  @Test
  public void testUnplannableJoinQueriesInNonSQLCompatibleMode()
  {
    msqIncompatible();

    Assume.assumeFalse(NullHandling.sqlCompatible());

    assertQueryIsUnplannable(
        // JOIN condition with not-equals (<>).
        "SELECT foo.dim1, foo.dim2, l.k, l.v\n"
        + "FROM foo INNER JOIN lookup.lookyloo l ON foo.dim2 <> l.k",
        "SQL requires a join with 'NOT_EQUALS' condition that is not supported."
    );

    assertQueryIsUnplannable(
        // JOIN condition with a function of both sides.
        "SELECT foo.dim1, foo.dim2, l.k, l.v\n"
        + "FROM foo INNER JOIN lookup.lookyloo l ON CHARACTER_LENGTH(foo.dim2 || l.k) > 3\n",
        "SQL requires a join with 'GREATER_THAN' condition that is not supported."
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyOnMetric()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE 2.5 < m1 AND m1 < 3.5",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(range("m1", ColumnType.DOUBLE, 2.5, 3.5, true, true))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyOr()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE (dim1 >= 'a' and dim1 < 'b') OR dim1 = 'ab'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(range("dim1", ColumnType.STRING, "a", "b", false, true))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @NotYetSupported(Modes.ERROR_HANDLING)
  @Test
  public void testUnplannableExactCountDistinctOnSketch()
  {
    // COUNT DISTINCT on a sketch cannot be exact.
    assertQueryIsUnplannable(
        PLANNER_CONFIG_NO_HLL,
        "SELECT COUNT(distinct unique_dim1) FROM druid.foo",
        "SQL requires a group-by on a column of type COMPLEX<hyperUnique> that is unsupported."
    );
  }

  @Test
  public void testIsNotDistinctFromLiteral()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE (dim1 >= 'a' and dim1 < 'b') OR dim1 IS NOT DISTINCT FROM 'ab'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(range("dim1", ColumnType.STRING, "a", "b", false, true))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  /**
   * Same query as {@link org.apache.druid.msq.test.CalciteSelectQueryMSQTest#testArrayAggQueryOnComplexDatatypes()}.
   * ARRAY_AGG is not supported in MSQ currently, but is supported as sql-native. Once support is added,
   * the overriding test can be removed and msqCompatible() can be added here instead.
   */
  @Test
  public void testArrayAggQueryOnComplexDatatypes()
  {
    msqIncompatible();
    cannotVectorize();
    testQuery(
        "SELECT ARRAY_AGG(unique_dim1) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new ExpressionLambdaAggregatorFactory(
                          "a0",
                          ImmutableSet.of("unique_dim1"),
                          "__acc",
                          "ARRAY<COMPLEX<hyperUnique>>[]",
                          "ARRAY<COMPLEX<hyperUnique>>[]",
                          true,
                          true,
                          false,
                          "array_append(\"__acc\", \"unique_dim1\")",
                          "array_concat(\"__acc\", \"a0\")",
                          null,
                          null,
                          ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                          queryFramework().macroTable()
                      )
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                "[\"AQAAAEAAAA==\",\"AQAAAQAAAAHNBA==\",\"AQAAAQAAAAOzAg==\",\"AQAAAQAAAAFREA==\",\"AQAAAQAAAACyEA==\",\"AQAAAQAAAAEkAQ==\"]"
            }
        )
    );
  }

  @Test
  public void testStringAggQueryOnComplexDatatypes()
  {
    try {
      testQuery("SELECT STRING_AGG(unique_dim1, ',') FROM druid.foo", ImmutableList.of(), ImmutableList.of());
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      Assert.assertTrue(
          e.getMessage().contains("Aggregation [STRING_AGG] does not support type [COMPLEX<hyperUnique>]")
      );
      Assert.assertEquals("invalidInput", e.getErrorCode());
    }
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyAnd()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE (dim1 >= 'a' and dim1 < 'b') and dim1 = 'abc'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(equality("dim1", "abc", ColumnType.STRING))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithFilterOnCastedString()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE CAST(dim1 AS bigint) = 2",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(numericEquality("dim1", 2L, ColumnType.LONG))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithFloorTimeFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE FLOOR(__time TO DAY) >= TIMESTAMP '2000-01-01 00:00:00' AND "
        + "FLOOR(__time TO DAY) < TIMESTAMP '2001-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithMisalignedFloorTimeFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE FLOOR(__time TO DAY) >= TIMESTAMP '2000-01-01 00:00:01' AND "
        + "FLOOR(__time TO DAY) < TIMESTAMP '2001-01-01 00:00:01'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-02/2001-01-02")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeInIntervalFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE TIME_IN_INTERVAL(__time, '2000-01-01/P1Y') "
        + "AND TIME_IN_INTERVAL(CURRENT_TIMESTAMP, '2000/3000') -- Optimized away: always true",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeInCastedIntervalFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE TIME_IN_INTERVAL(__time, CAST('2000-01-01/P1Y' AS VARCHAR)) "
        + "AND TIME_IN_INTERVAL(CURRENT_TIMESTAMP, '2000/3000') -- Optimized away: always true",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeInIntervalFilterLosAngeles()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE TIME_IN_INTERVAL(__time, '2000-01-01/P1Y')",
        QUERY_CONTEXT_LOS_ANGELES,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01T08:00:00/2001-01-01T08:00:00")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_LOS_ANGELES)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeInIntervalFilterInvalidInterval()
  {
    msqIncompatible();
    testQueryThrows(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE TIME_IN_INTERVAL(__time, '2000-01-01/X')",
        expected -> {
          expected.expect(CoreMatchers.instanceOf(CalciteContextException.class));
          expected.expect(ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
              "From line 1, column 38 to line 1, column 77: "
              + "Function 'TIME_IN_INTERVAL' second argument is not a valid ISO8601 interval: "
              + "Invalid format: \"X\"")));
        }
    );
  }

  @Test
  public void testCountStarWithTimeInIntervalFilterNonLiteral()
  {
    msqIncompatible();
    testQueryThrows(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE TIME_IN_INTERVAL(__time, dim1)",
        expected -> {
          expected.expect(CoreMatchers.instanceOf(DruidException.class));
          expected.expect(ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
              "Argument to function 'TIME_IN_INTERVAL' must be a literal (line [1], column [63])")));
        }
    );
  }

  @Test
  public void testCountStarWithBetweenTimeFilterUsingMilliseconds()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithBetweenFloorTimeFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE FLOOR(__time TO DAY) BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithBetweenTimeFilterUsingMillisecondsInStringLiterals()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE __time BETWEEN '2000-01-01 00:00:00' AND '2000-12-31 23:59:59.999'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testRemoveUselessCaseWhen()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "  CASE\n"
        + "    WHEN __time >= TIME_PARSE('2000-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AND __time < TIMESTAMP '2001-01-01 00:00:00'\n"
        + "    THEN true\n"
        + "    ELSE false\n"
        + "  END\n"
        + "OR\n"
        + "  __time >= TIMESTAMP '2010-01-01 00:00:00' AND __time < TIMESTAMP '2011-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000/2001"), Intervals.of("2010/2011")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeMillisecondFilters()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time = TIMESTAMP '2000-01-01 00:00:00.111'\n"
        + "OR (__time >= TIMESTAMP '2000-01-01 00:00:00.888' AND __time < TIMESTAMP '2000-01-02 00:00:00.222')",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      querySegmentSpec(
                          Intervals.of("2000-01-01T00:00:00.111/2000-01-01T00:00:00.112"),
                          Intervals.of("2000-01-01T00:00:00.888/2000-01-02T00:00:00.222")
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterUsingStringLiterals()
  {
    // Strings are implicitly cast to timestamps. Test a few different forms.
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= '2000-01-01 00:00:00' AND __time < '2001-01-01T00:00:00'\n"
        + "OR __time >= '2001-02-01' AND __time < '2001-02-02'\n"
        + "OR __time BETWEEN '2001-03-01' AND '2001-03-02'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      querySegmentSpec(
                          Intervals.of("2000-01-01/2001-01-01"),
                          Intervals.of("2001-02-01/2001-02-02"),
                          Intervals.of("2001-03-01/2001-03-02T00:00:00.001")
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterUsingStringLiteralsInvalid_isUnplannable()
  {
    // Strings are implicitly cast to timestamps. Test an invalid string.
    // This error message isn't ideal but it is at least better than silently ignoring the problem.
    String sql = "SELECT COUNT(*) FROM druid.foo\n"
                 + "WHERE __time >= 'z2000-01-01 00:00:00' AND __time < '2001-01-01 00:00:00'\n";
    try {
      testBuilder().sql(sql).run();
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(
          e,
          invalidSqlIs("Illegal TIMESTAMP constant [CAST('z2000-01-01 00:00:00'):TIMESTAMP(3) NOT NULL]")
      );
    }
    catch (Exception e) {
      log.error(e, "Expected DruidException for query: %s", sql);
      Assert.fail(sql);
    }
  }

  @Test
  public void testCountStarWithSinglePointInTime()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE __time = TIMESTAMP '2000-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2000-01-01T00:00:00.001")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithTwoPointsInTime()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "__time = TIMESTAMP '2000-01-01 00:00:00' OR __time = TIMESTAMP '2000-01-01 00:00:00' + INTERVAL '1' DAY",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      querySegmentSpec(
                          Intervals.of("2000-01-01/2000-01-01T00:00:00.001"),
                          Intervals.of("2000-01-02/2000-01-02T00:00:00.001")
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarWithComplexDisjointTimeFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 = 'a' and ("
        + "  (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "  OR ("
        + "    (__time >= TIMESTAMP '2002-01-01 00:00:00' AND __time < TIMESTAMP '2003-05-01 00:00:00')"
        + "    and (__time >= TIMESTAMP '2002-05-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00')"
        + "    and dim1 = 'abc'"
        + "  )"
        + ")",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000/2001"), Intervals.of("2002-05-01/2003-05-01")))
                  .granularity(Granularities.ALL)
                  .filters(
                      and(
                          equality("dim2", "a", ColumnType.STRING),
                          or(
                              timeRange("2000/2001"),
                              and(
                                  timeRange("2002-05-01/2003-05-01"),
                                  equality("dim1", "abc", ColumnType.STRING)
                              )
                          )
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithNotOfComplexDisjointTimeFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE not (dim2 = 'a' and ("
        + "    (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "    OR ("
        + "      (__time >= TIMESTAMP '2002-01-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00')"
        + "      and (__time >= TIMESTAMP '2002-05-01 00:00:00' AND __time < TIMESTAMP '2003-05-01 00:00:00')"
        + "      and dim1 = 'abc'"
        + "    )"
        + "  )"
        + ")",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(
                      or(
                          not(equality("dim2", "a", ColumnType.STRING)),
                          and(
                              not(timeRange("2000/2001")),
                              or(
                                  not(timeRange("2002-05-01/2003-05-01")),
                                  not(equality("dim1", "abc", ColumnType.STRING))
                              )
                          )
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 5L : 4L}
        )
    );
  }

  @Test
  public void testCountStarWithNotTimeFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim1 <> 'xxx' and not ("
        + "    (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "    OR (__time >= TIMESTAMP '2003-01-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00'))",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      querySegmentSpec(
                          new Interval(DateTimes.MIN, DateTimes.of("2000")),
                          Intervals.of("2001/2003"),
                          new Interval(DateTimes.of("2004"), DateTimes.MAX)
                      )
                  )
                  .filters(not(equality("dim1", "xxx", ColumnType.STRING)))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeAndDimFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 <> 'a' "
        + "and __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                  .filters(not(equality("dim2", "a", ColumnType.STRING)))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 2L : 1L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeOrDimFilter()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 <> 'a' "
        + "or __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(
                      or(
                          not(equality("dim2", "a", ColumnType.STRING)),
                          range(
                              "__time",
                              ColumnType.LONG,
                              timestamp("2000-01-01"),
                              timestamp("2000-12-31T23:59:59.999"),
                              false,
                              false
                          )
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            NullHandling.replaceWithDefault() ? new Object[]{5L} : new Object[]{4L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingExtractEpoch()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01 00:00:00') * 1000 "
        + "AND cnt < EXTRACT(EPOCH FROM TIMESTAMP '1970-01-02 00:00:00') * 1000",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      range(
                          "cnt",
                          ColumnType.LONG,
                          DateTimes.of("1970-01-01").getMillis(),
                          DateTimes.of("1970-01-02").getMillis(),
                          false,
                          true
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingExtractEpochFromDate()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= EXTRACT(EPOCH FROM DATE '1970-01-01') * 1000 "
        + "AND cnt < EXTRACT(EPOCH FROM DATE '1970-01-02') * 1000",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      range(
                          "cnt",
                          ColumnType.LONG,
                          DateTimes.of("1970-01-01").getMillis(),
                          DateTimes.of("1970-01-02").getMillis(),
                          false,
                          true
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingTimestampToMillis()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= TIMESTAMP_TO_MILLIS(TIMESTAMP '1970-01-01 00:00:00') "
        + "AND cnt < TIMESTAMP_TO_MILLIS(TIMESTAMP '1970-01-02 00:00:00')",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      range(
                          "cnt",
                          ColumnType.LONG,
                          DateTimes.of("1970-01-01").getMillis(),
                          DateTimes.of("1970-01-02").getMillis(),
                          false,
                          true
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testSumOfString()
  {
    testQuery(
        "SELECT SUM(CAST(dim1 AS INTEGER)) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "CAST(\"dim1\", 'LONG')", ColumnType.LONG)
                  )
                  .aggregators(aggregators(
                      new LongSumAggregatorFactory(
                          "a0",
                          "v0",
                          null,
                          CalciteTests.createExprMacroTable()
                      )
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{13L}
        )
    );
  }

  @Test
  public void testSumOfExtractionFn()
  {
    // Cannot vectorize due to expressions in aggregators.
    cannotVectorize();

    testQuery(
        "SELECT SUM(CAST(SUBSTRING(dim1, 1, 10) AS INTEGER)) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "CAST(substring(\"dim1\", 0, 10), 'LONG')", ColumnType.LONG)
                  )
                  .aggregators(aggregators(
                      new LongSumAggregatorFactory(
                          "a0",
                          "v0",
                          null,
                          CalciteTests.createExprMacroTable()
                      )
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{13L}
        )
    );
  }

  @Test
  public void testTimeseriesWithTimeFilterOnLongColumnUsingMillisToTimestamp()
  {
    testQuery(
        "SELECT\n"
        + "  FLOOR(MILLIS_TO_TIMESTAMP(cnt) TO YEAR),\n"
        + "  COUNT(*)\n"
        + "FROM\n"
        + "  druid.foo\n"
        + "WHERE\n"
        + "  MILLIS_TO_TIMESTAMP(cnt) >= TIMESTAMP '1970-01-01 00:00:00'\n"
        + "  AND MILLIS_TO_TIMESTAMP(cnt) < TIMESTAMP '1970-01-02 00:00:00'\n"
        + "GROUP BY\n"
        + "  FLOOR(MILLIS_TO_TIMESTAMP(cnt) TO YEAR)",
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setVirtualColumns(
                    expressionVirtualColumn("v0", "timestamp_floor(\"cnt\",'P1Y',null,'UTC')", ColumnType.LONG)
                )
                .setDimFilter(
                    range(
                        "cnt",
                        ColumnType.LONG,
                        DateTimes.of("1970-01-01").getMillis(),
                        DateTimes.of("1970-01-02").getMillis(),
                        false,
                        true
                    )
                )
                .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("1970-01-01"), 6L}
        )
    );
  }

  @Test
  public void testCountDistinct()
  {
    msqIncompatible();
    testQuery(
        "SELECT SUM(cnt), COUNT(distinct dim2), COUNT(distinct unique_dim1) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new CardinalityAggregatorFactory(
                              "a1",
                              null,
                              dimensions(new DefaultDimensionSpec("dim2", null)),
                              false,
                              true
                          ),
                          new HyperUniquesAggregatorFactory("a2", "unique_dim1", false, true)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L, 6L}
        )
    );
  }

  @Test
  public void testCountDistinctOfCaseWhen()
  {
    testQuery(
        "SELECT\n"
        + "COUNT(DISTINCT CASE WHEN m1 >= 4 THEN m1 END),\n"
        + "COUNT(DISTINCT CASE WHEN m1 >= 4 THEN dim1 END),\n"
        + "COUNT(DISTINCT CASE WHEN m1 >= 4 THEN unique_dim1 END)\n"
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new CardinalityAggregatorFactory(
                                  "a0",
                                  null,
                                  ImmutableList.of(new DefaultDimensionSpec("m1", "m1", ColumnType.FLOAT)),
                                  false,
                                  true
                              ),
                              range("m1", ColumnType.LONG, 4L, null, false, false)
                          ),
                          new FilteredAggregatorFactory(
                              new CardinalityAggregatorFactory(
                                  "a1",
                                  null,
                                  ImmutableList.of(new DefaultDimensionSpec("dim1", "dim1", ColumnType.STRING)),
                                  false,
                                  true
                              ),
                              range("m1", ColumnType.LONG, 4L, null, false, false)
                          ),
                          new FilteredAggregatorFactory(
                              new HyperUniquesAggregatorFactory("a2", "unique_dim1", false, true),
                              range("m1", ColumnType.LONG, 4L, null, false, false)
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 3L, 3L}
        )
    );
  }

  @Test
  public void testExactCountDistinct()
  {
    // When HLL is disabled, do exact count distinct through a nested query.

    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT COUNT(distinct dim2) FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("a0"),
                                notNull("d0")
                            )
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 2L : 3L}
        )
    );
  }

  @Test
  public void testApproxCountDistinctWhenHllDisabled()
  {
    if (NullHandling.sqlCompatible()) {
      // Empty string isn't counted properly; see https://github.com/apache/druid/issues/13950
      msqIncompatible();
    }

    // When HLL is disabled, APPROX_COUNT_DISTINCT is still approximate.
    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT APPROX_COUNT_DISTINCT(dim2) FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new CardinalityAggregatorFactory(
                              "a0",
                              null,
                              dimensions(new DefaultDimensionSpec("dim2", null)),
                              false,
                              true
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testApproxCountDistinctBuiltin()
  {
    if (NullHandling.sqlCompatible()) {
      // Empty string isn't counted properly; see https://github.com/apache/druid/issues/13950
      msqIncompatible();
    }

    testQuery(
        "SELECT APPROX_COUNT_DISTINCT_BUILTIN(dim2) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new CardinalityAggregatorFactory(
                              "a0",
                              null,
                              dimensions(new DefaultDimensionSpec("dim2", null)),
                              false,
                              true
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @NotYetSupported(Modes.PLAN_MISMATCH)
  @Test
  public void testExactCountDistinctWithGroupingAndOtherAggregators()
  {
    // When HLL is disabled, do exact count distinct through a nested query.

    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT dim2, SUM(cnt), COUNT(distinct dim1) FROM druid.foo GROUP BY dim2",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(
                                                new DefaultDimensionSpec("dim2", "d0"),
                                                new DefaultDimensionSpec("dim1", "d1")
                                            ))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("d0", "_d0")))
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("_a0", "a0"),
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("_a1"),
                                notNull("d1")
                            )
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", 3L, 3L},
            new Object[]{"a", 2L, 1L},
            new Object[]{"abc", 1L, 1L}
        ) :
        ImmutableList.of(
            new Object[]{null, 2L, 2L},
            new Object[]{"", 1L, 1L},
            new Object[]{"a", 2L, 2L},
            new Object[]{"abc", 1L, 1L}
        )
    );
  }

  @NotYetSupported(Modes.MISSING_JOIN_CONVERSION)
  @Test
  public void testMultipleExactCountDistinctWithGroupingAndOtherAggregatorsUsingJoin()
  {
    // When HLL is disabled, do multiple exact count distincts through joins of nested queries.

    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT dim2, COUNT(*), COUNT(distinct dim1), COUNT(distinct cnt) FROM druid.foo GROUP BY dim2",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new QueryDataSource(
                                GroupByQuery
                                    .builder()
                                    .setDataSource(CalciteTests.DATASOURCE1)
                                    .setGranularity(Granularities.ALL)
                                    .setInterval(querySegmentSpec(Filtration.eternity()))
                                    .setDimensions(new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING))
                                    .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                                    .build()
                            ),
                            new QueryDataSource(
                                GroupByQuery
                                    .builder()
                                    .setDataSource(
                                        new QueryDataSource(
                                            GroupByQuery
                                                .builder()
                                                .setDataSource(CalciteTests.DATASOURCE1)
                                                .setGranularity(Granularities.ALL)
                                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                                .setDimensions(
                                                    new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING),
                                                    new DefaultDimensionSpec("dim1", "d1", ColumnType.STRING)
                                                )
                                                .build()
                                        )
                                    )
                                    .setGranularity(Granularities.ALL)
                                    .setInterval(querySegmentSpec(Filtration.eternity()))
                                    .setDimensions(new DefaultDimensionSpec("d0", "_d0", ColumnType.STRING))
                                    .setAggregatorSpecs(
                                        new FilteredAggregatorFactory(
                                            new CountAggregatorFactory("a0"),
                                            new NotDimFilter(isNull("d1", null))
                                        )
                                    )
                                    .build()
                            ),
                            "j0.",
                            "notdistinctfrom(\"d0\",\"j0._d0\")",
                            JoinType.INNER
                        ),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setGranularity(Granularities.ALL)
                                        .setDataSource(
                                            new QueryDataSource(
                                                GroupByQuery
                                                    .builder()
                                                    .setDataSource(CalciteTests.DATASOURCE1)
                                                    .setGranularity(Granularities.ALL)
                                                    .setInterval(querySegmentSpec(Filtration.eternity()))
                                                    .setDimensions(
                                                        new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING),
                                                        new DefaultDimensionSpec("cnt", "d1", ColumnType.LONG)
                                                    )
                                                    .build()
                                            )
                                        )
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setDimensions(new DefaultDimensionSpec("d0", "_d0", ColumnType.STRING))
                                        .setAggregatorSpecs(
                                            NullHandling.sqlCompatible()
                                            ? new FilteredAggregatorFactory(
                                                new CountAggregatorFactory("a0"),
                                                new NotDimFilter(isNull("d1", null))
                                            )
                                            : new CountAggregatorFactory("a0")
                                        )
                                        .build()
                        ),
                        "_j0.",
                        "notdistinctfrom(\"d0\",\"_j0._d0\")",
                        JoinType.INNER
                    )
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("_j0.a0", "a0", "d0", "j0.a0"))
                .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{null, 2L, 2L, 1L},
            new Object[]{"", 1L, 1L, 1L},
            new Object[]{"a", 2L, 2L, 1L},
            new Object[]{"abc", 1L, 1L, 1L}
        )
        : ImmutableList.of(
            new Object[]{"", 3L, 3L, 1L},
            new Object[]{"a", 2L, 1L, 1L},
            new Object[]{"abc", 1L, 1L, 1L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 4)
  @Test
  public void testMultipleExactCountDistinctWithGroupingUsingGroupingSets()
  {
    msqIncompatible();
    testQuery(
        PLANNER_CONFIG_NO_HLL.withOverrides(
            ImmutableMap.of(
                PlannerConfig.CTX_KEY_USE_GROUPING_SET_FOR_EXACT_DISTINCT,
                "true"
            )
        ),
        "SELECT FLOOR(__time to day), COUNT(distinct city), COUNT(distinct user) FROM druid.visits GROUP BY 1",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.USERVISITDATASOURCE)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setVirtualColumns(expressionVirtualColumn(
                                                "v0",
                                                "timestamp_floor(\"__time\",'P1D',null,'UTC')",
                                                ColumnType.LONG
                                            ))
                                            .setDimensions(dimensions(
                                                new DefaultDimensionSpec("v0", "d0", ColumnType.LONG),
                                                new DefaultDimensionSpec("city", "d1"),
                                                new DefaultDimensionSpec("user", "d2")
                                            ))
                                            .setAggregatorSpecs(aggregators(new GroupingAggregatorFactory(
                                                "a0",
                                                Arrays.asList(
                                                    "v0",
                                                    "city",
                                                    "user"
                                                )
                                            )))
                                            .setSubtotalsSpec(ImmutableList.of(
                                                ImmutableList.of("d0", "d1"),
                                                ImmutableList.of("d0", "d2")
                                            ))
                                            .setContext(withTimestampResultContext(
                                                QUERY_CONTEXT_DEFAULT,
                                                "d0",
                                                0,
                                                Granularities.DAY
                                            ))
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("d0", "_d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("_a0"),
                                and(
                                    notNull("d1"),
                                    equality("a0", 1L, ColumnType.LONG)
                                )
                            ),
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("_a1"),
                                and(
                                    notNull("d2"),
                                    equality("a0", 2L, ColumnType.LONG)
                                )
                            )
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1609459200000L, 3L, 2L},
            new Object[]{1609545600000L, 3L, 4L},
            new Object[]{1609632000000L, 1L, 1L}
        )
    );
  }

  @Test
  public void testApproxCountDistinct()
  {
    msqIncompatible();
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  APPROX_COUNT_DISTINCT(dim2),\n" // uppercase
        + "  approx_count_distinct(dim2) FILTER(WHERE dim2 <> ''),\n" // lowercase; also, filtered
        + "  APPROX_COUNT_DISTINCT(SUBSTRING(dim2, 1, 1)),\n" // on extractionFn
        + "  APPROX_COUNT_DISTINCT(SUBSTRING(dim2, 1, 1) || 'x'),\n" // on expression
        + "  approx_count_distinct(unique_dim1)\n" // on native hyperUnique column
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "concat(substring(\"dim2\", 0, 1),'x')", ColumnType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new CardinalityAggregatorFactory(
                              "a1",
                              null,
                              dimensions(new DefaultDimensionSpec("dim2", "dim2")),
                              false,
                              true
                          ),
                          new FilteredAggregatorFactory(
                              new CardinalityAggregatorFactory(
                                  "a2",
                                  null,
                                  dimensions(new DefaultDimensionSpec("dim2", "dim2")),
                                  false,
                                  true
                              ),
                              not(equality("dim2", "", ColumnType.STRING))
                          ),
                          new CardinalityAggregatorFactory(
                              "a3",
                              null,
                              dimensions(
                                  new ExtractionDimensionSpec(
                                      "dim2",
                                      "dim2",
                                      ColumnType.STRING,
                                      new SubstringDimExtractionFn(0, 1)
                                  )
                              ),
                              false,
                              true
                          ),
                          new CardinalityAggregatorFactory(
                              "a4",
                              null,
                              dimensions(new DefaultDimensionSpec("v0", "v0", ColumnType.STRING)),
                              false,
                              true
                          ),
                          new HyperUniquesAggregatorFactory("a5", "unique_dim1", false, true)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{6L, 3L, 2L, 2L, 2L, 6L}
        ) :
        ImmutableList.of(
            new Object[]{6L, 3L, 2L, 1L, 1L, 6L}
        )
    );
  }

  @Test
  public void testApproxCountDistinctOnVectorizableSingleStringExpression()
  {
    testQuery(
        "SELECT APPROX_COUNT_DISTINCT(dim1 || 'hello') FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "concat(\"dim1\",'hello')", ColumnType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new CardinalityAggregatorFactory(
                              "a0",
                              null,
                              dimensions(DefaultDimensionSpec.of("v0")),
                              false,
                              true
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{6L})
    );
  }

  @Test
  public void testNestedGroupBy()
  {
    testQuery(
        "SELECT\n"
        + "    FLOOR(__time to hour) AS __time,\n"
        + "    dim1,\n"
        + "    COUNT(m2)\n"
        + "FROM (\n"
        + "    SELECT\n"
        + "        MAX(__time) AS __time,\n"
        + "        m2,\n"
        + "        dim1\n"
        + "    FROM druid.foo\n"
        + "    WHERE 1=1\n"
        + "        AND m1 = '5.0'\n"
        + "    GROUP BY m2, dim1\n"
        + ")\n"
        + "GROUP BY FLOOR(__time to hour), dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(
                                            dimensions(
                                                new DefaultDimensionSpec("dim1", "d0"),
                                                new DefaultDimensionSpec("m2", "d1", ColumnType.DOUBLE)
                                            )
                                        )
                                        .setDimFilter(equality("m1", 5.0, ColumnType.FLOAT))
                                        .setAggregatorSpecs(aggregators(new LongMaxAggregatorFactory("a0", "__time")))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"a0\",'PT1H',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("v0", "_d0", ColumnType.LONG),
                            new DefaultDimensionSpec("d0", "_d1", ColumnType.STRING)
                        ))
                        .setAggregatorSpecs(
                            aggregators(
                                useDefault
                                ? new CountAggregatorFactory("_a0")
                                : new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("_a0"),
                                    notNull("d1")
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{978393600000L, "def", 1L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testDoubleNestedGroupBy()
  {
    testQuery(
        "SELECT SUM(cnt), COUNT(*) FROM (\n"
        + "  SELECT dim2, SUM(t1.cnt) cnt FROM (\n"
        + "    SELECT\n"
        + "      dim1,\n"
        + "      dim2,\n"
        + "      COUNT(*) cnt\n"
        + "    FROM druid.foo\n"
        + "    GROUP BY dim1, dim2\n"
        + "  ) t1\n"
        + "  GROUP BY dim2\n"
        + ") t2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(
                                            GroupByQuery.builder()
                                                        .setDataSource(CalciteTests.DATASOURCE1)
                                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                                        .setGranularity(Granularities.ALL)
                                                        .setDimensions(dimensions(
                                                            new DefaultDimensionSpec("dim1", "d0"),
                                                            new DefaultDimensionSpec("dim2", "d1")
                                                        ))
                                                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                                        .build()
                                        )
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("d1", "_d0")))
                                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("_a0", "a0")))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("a0", "_a0"),
                            new CountAggregatorFactory("a1")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(new Object[]{6L, 3L})
        : ImmutableList.of(new Object[]{6L, 4L})
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testDoubleNestedGroupBy2()
  {
    // This test fails when AggregateMergeRule is added to Rules.ABSTRACT_RELATIONAL_RULES. So, we don't add that
    // rule for now. Possible bug in the rule.
    testQuery(
        "SELECT MAX(cnt) FROM (\n"
        + "  SELECT dim2, MAX(t1.cnt) cnt FROM (\n"
        + "    SELECT\n"
        + "      dim1,\n"
        + "      dim2,\n"
        + "      COUNT(*) cnt\n"
        + "    FROM druid.foo\n"
        + "    GROUP BY dim1, dim2\n"
        + "  ) t1\n"
        + "  GROUP BY dim2\n"
        + ") t2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(
                                            GroupByQuery.builder()
                                                        .setDataSource(CalciteTests.DATASOURCE1)
                                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                                        .setGranularity(Granularities.ALL)
                                                        .setDimensions(
                                                            new DefaultDimensionSpec("dim1", "d0"),
                                                            new DefaultDimensionSpec("dim2", "d1")
                                                        )
                                                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                                        .build()
                                        )
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(new DefaultDimensionSpec("d1", "_d0"))
                                        .setAggregatorSpecs(new LongMaxAggregatorFactory("_a0", "a0"))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(new LongMaxAggregatorFactory("a0", "_a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{1L})
    );
  }

  @Test
  public void testExactCountDistinctUsingSubquery()
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("_a0", "a0"),
                            new CountAggregatorFactory("_a1")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{6L, 3L}
        ) :
        ImmutableList.of(
            new Object[]{6L, 4L}
        )
    );
  }

  @Test
  public void testAvgDailyCountDistinct()
  {
    // Cannot vectorize outer query due to inlined inner query.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  AVG(u)\n"
        + "FROM (SELECT FLOOR(__time TO DAY), APPROX_COUNT_DISTINCT(cnt) AS u FROM druid.foo GROUP BY 1)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                Druids.newTimeseriesQueryBuilder()
                                      .dataSource(CalciteTests.DATASOURCE1)
                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                      .granularity(new PeriodGranularity(Period.days(1), null, DateTimeZone.UTC))
                                      .aggregators(
                                          new CardinalityAggregatorFactory(
                                              "a0:a",
                                              null,
                                              dimensions(new DefaultDimensionSpec(
                                                  "cnt",
                                                  "cnt",
                                                  ColumnType.LONG
                                              )),
                                              false,
                                              true
                                          )
                                      )
                                      .postAggregators(
                                          ImmutableList.of(
                                              new HyperUniqueFinalizingPostAggregator("a0", "a0:a")
                                          )
                                      )
                                      .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                                      .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            useDefault
                            ? aggregators(
                                new DoubleSumAggregatorFactory("_a0:sum", "a0"),
                                new CountAggregatorFactory("_a0:count")
                            )
                            : aggregators(
                                new DoubleSumAggregatorFactory("_a0:sum", "a0"),
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("_a0:count"),
                                    notNull("a0")
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new ArithmeticPostAggregator(
                                    "_a0",
                                    "quotient",
                                    ImmutableList.of(
                                        new FieldAccessPostAggregator(null, "_a0:sum"),
                                        new FieldAccessPostAggregator(null, "_a0:count")
                                    )
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{1.0})
    );
  }

  @Test
  public void testHighestMaxNumericInFilter()
  {
    expectedException.expect(UOE.class);
    expectedException.expectMessage("Expected parameter[maxNumericInFilters] cannot exceed system set value of [100]");

    testQuery(
        PLANNER_CONFIG_MAX_NUMERIC_IN_FILTER,
        ImmutableMap.of(QueryContexts.MAX_NUMERIC_IN_FILTERS, 20000),
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE dim6 IN (\n"
        + "1,2,3\n"
        + ")\n",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testQueryWithMoreThanMaxNumericInFilter()
  {
    if (NullHandling.sqlCompatible()) {
      // skip in sql compatible mode, this plans to an OR filter with equality filter children...
      return;
    }
    msqIncompatible();
    expectedException.expect(UOE.class);
    expectedException.expectMessage(
        "The number of values in the IN clause for [dim6] in query exceeds configured maxNumericFilter limit of [2] for INs. Cast [3] values of IN clause to String");

    testQuery(
        PLANNER_CONFIG_MAX_NUMERIC_IN_FILTER,
        ImmutableMap.of(QueryContexts.MAX_NUMERIC_IN_FILTERS, 2),
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE dim6 IN (\n"
        + "1,2,3\n"
        + ")\n",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testExactCountDistinctUsingSubqueryWithWherePushDown()
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)\n"
        + "WHERE dim2 <> ''",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setDimFilter(
                                                not(
                                                    equality("dim2", "", ColumnType.STRING)
                                                )
                                            )
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("_a0", "a0"),
                            new CountAggregatorFactory("_a1")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 2L}
        )
    );

    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)\n"
        + "WHERE dim2 IS NOT NULL",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setDimFilter(notNull("dim2"))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("_a0", "a0"),
                            new CountAggregatorFactory("_a1")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{3L, 2L}
        ) :
        ImmutableList.of(
            new Object[]{4L, 3L}
        )
    );
  }

  @Test
  public void testCompareExactAndApproximateCountDistinctUsingSubquery()
  {
    testQuery(
        "SELECT\n"
        + "  COUNT(*) AS exact_count,\n"
        + "  COUNT(DISTINCT dim1) AS approx_count,\n"
        + "  (CAST(1 AS FLOAT) - COUNT(DISTINCT dim1) / COUNT(*)) * 100 AS error_pct\n"
        + "FROM (SELECT DISTINCT dim1 FROM druid.foo WHERE dim1 <> '')",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimFilter(
                                                not(equality("dim1", "", ColumnType.STRING))
                                            )
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new CountAggregatorFactory("a0"),
                            new CardinalityAggregatorFactory(
                                "a1",
                                null,
                                dimensions(new DefaultDimensionSpec("d0", null)),
                                false,
                                true
                            )
                        ))
                        .setPostAggregatorSpecs(
                            expressionPostAgg("p0", "((1.0 - (\"a1\" / \"a0\")) * 100)", ColumnType.FLOAT)
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{5L, 5L, 0.0f}
        )
    );
  }

  @Test
  public void testHistogramUsingSubquery()
  {
    testQuery(
        "SELECT\n"
        + "  CAST(thecnt AS VARCHAR),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS thecnt FROM druid.foo GROUP BY dim2)\n"
        + "GROUP BY CAST(thecnt AS VARCHAR)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("a0", "_d0")))
                        .setAggregatorSpecs(aggregators(
                            new CountAggregatorFactory("_a0")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"3", 1L}
        ) :
        ImmutableList.of(
            new Object[]{"1", 2L},
            new Object[]{"2", 2L}
        )
    );
  }

  @Test
  public void testHistogramUsingSubqueryWithSort()
  {
    testQuery(
        "SELECT\n"
        + "  CAST(thecnt AS VARCHAR),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS thecnt FROM druid.foo GROUP BY dim2)\n"
        + "GROUP BY CAST(thecnt AS VARCHAR) ORDER BY CAST(thecnt AS VARCHAR) LIMIT 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("a0", "_d0")))
                        .setAggregatorSpecs(aggregators(
                            new CountAggregatorFactory("_a0")
                        ))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(new OrderByColumnSpec(
                                    "_d0",
                                    OrderByColumnSpec.Direction.ASCENDING,
                                    StringComparators.LEXICOGRAPHIC
                                )),
                                2
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"1", 1L},
            new Object[]{"2", 1L}
        ) :
        ImmutableList.of(
            new Object[]{"1", 2L},
            new Object[]{"2", 2L}
        )
    );
  }

  @Test
  public void testCountDistinctArithmetic()
  {
    if (NullHandling.sqlCompatible()) {
      // Empty string isn't counted properly; see https://github.com/apache/druid/issues/13950
      msqIncompatible();
    }

    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(DISTINCT dim2),\n"
        + "  CAST(COUNT(DISTINCT dim2) AS FLOAT),\n"
        + "  SUM(cnt) / COUNT(DISTINCT dim2),\n"
        + "  SUM(cnt) / COUNT(DISTINCT dim2) + 3,\n"
        + "  CAST(SUM(cnt) AS FLOAT) / CAST(COUNT(DISTINCT dim2) AS FLOAT) + 3\n"
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new CardinalityAggregatorFactory(
                              "a1",
                              null,
                              dimensions(new DefaultDimensionSpec("dim2", null)),
                              false,
                              true
                          )
                      )
                  )
                  .postAggregators(
                      expressionPostAgg("p0", "CAST(\"a1\", 'DOUBLE')", ColumnType.FLOAT),
                      expressionPostAgg("p1", "(\"a0\" / \"a1\")", ColumnType.LONG),
                      expressionPostAgg("p2", "((\"a0\" / \"a1\") + 3)", ColumnType.LONG),
                      expressionPostAgg("p3", "((CAST(\"a0\", 'DOUBLE') / CAST(\"a1\", 'DOUBLE')) + 3)", ColumnType.FLOAT)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L, 3.0f, 2L, 5L, 5.0f}
        )
    );
  }

  @Test
  public void testCountDistinctOfSubstring()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(DISTINCT SUBSTRING(dim1, 1, 1)) FROM druid.foo WHERE dim1 <> ''",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(not(equality("dim1", "", ColumnType.STRING)))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new CardinalityAggregatorFactory(
                              "a0",
                              null,
                              dimensions(
                                  new ExtractionDimensionSpec(
                                      "dim1",
                                      null,
                                      new SubstringDimExtractionFn(0, 1)
                                  )
                              ),
                              false,
                              true
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{4L}
        )
    );
  }

  @Test
  public void testCountDistinctOfTrim()
  {
    // Test a couple different syntax variants of TRIM.
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(DISTINCT TRIM(BOTH ' ' FROM dim1)) FROM druid.foo WHERE TRIM(dim1) <> ''",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(expressionVirtualColumn("v0", "trim(\"dim1\",' ')", ColumnType.STRING))
                  .filters(not(equality("v0", "", ColumnType.STRING)))
                  .aggregators(
                      aggregators(
                          new CardinalityAggregatorFactory(
                              "a0",
                              null,
                              dimensions(new DefaultDimensionSpec("v0", "v0", ColumnType.STRING)),
                              false,
                              true
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testSillyQuarters()
  {
    // Like FLOOR(__time TO QUARTER) but silly.

    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT CAST((EXTRACT(MONTH FROM __time) - 1 ) / 3 + 1 AS INTEGER) AS quarter, COUNT(*)\n"
        + "FROM foo\n"
        + "GROUP BY CAST((EXTRACT(MONTH FROM __time) - 1 ) / 3 + 1 AS INTEGER)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "(((timestamp_extract(\"__time\",'MONTH','UTC') - 1) / 3) + 1)",
                            ColumnType.LONG
                        ))
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1, 6L}
        )
    );
  }

  @Test
  public void testRegexpExtract()
  {
    // Cannot vectorize due to extractionFn in dimension spec.
    cannotVectorize();

    GroupByQuery.Builder builder =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(
                        dimensions(
                            new ExtractionDimensionSpec(
                                "dim1",
                                "d0",
                                new RegexDimExtractionFn("^.", 0, true, null)
                            ),
                            new ExtractionDimensionSpec(
                                "dim1",
                                "d1",
                                new RegexDimExtractionFn("^(.)", 1, true, null)
                            )
                        )
                    )
                    .setContext(QUERY_CONTEXT_DEFAULT);
    if (NullHandling.sqlCompatible()) {
      builder = builder.setVirtualColumns(
                           expressionVirtualColumn("v0", "regexp_extract(\"dim1\",'^(.)',1)", ColumnType.STRING)
                       )
                       .setDimFilter(
                           not(equality("v0", "x", ColumnType.STRING))
                       );
    } else {
      builder = builder.setDimFilter(
          not(selector("dim1", "x", new RegexDimExtractionFn("^(.)", 1, true, null)))
      );
    }

    testQuery(
        "SELECT DISTINCT\n"
        + "  REGEXP_EXTRACT(dim1, '^.'),\n"
        + "  REGEXP_EXTRACT(dim1, '^(.)', 1)\n"
        + "FROM foo\n"
        + "WHERE REGEXP_EXTRACT(dim1, '^(.)', 1) <> 'x'",
        ImmutableList.of(builder.build()),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{NULL_STRING, NULL_STRING},
            new Object[]{"1", "1"},
            new Object[]{"2", "2"},
            new Object[]{"a", "a"},
            new Object[]{"d", "d"}
        )
        // sql compatible mode expression filter (correctly) leaves out nulls
        : ImmutableList.of(
            new Object[]{"1", "1"},
            new Object[]{"2", "2"},
            new Object[]{"a", "a"},
            new Object[]{"d", "d"}
        )
    );
  }

  @Test
  public void testRegexpExtractWithBadRegexPattern()
  {
    // Cannot vectorize due to extractionFn in dimension spec.
    cannotVectorize();

    expectedException.expect(DruidException.class);
    expectedException.expectMessage(
        "An invalid pattern [^(.))] was provided for the REGEXP_EXTRACT function, " +
        "error: [Unmatched closing ')' near index 3\n^(.))\n   ^]"
    );

    testQuery(
        "SELECT DISTINCT\n"
        + "  REGEXP_EXTRACT(dim1, '^(.))', 1)\n"
        + "FROM foo",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testRegexpExtractFilterViaNotNullCheck()
  {
    // Cannot vectorize due to extractionFn in dimension spec.
    cannotVectorize();
    Druids.TimeseriesQueryBuilder builder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(querySegmentSpec(Filtration.eternity()))
              .granularity(Granularities.ALL)
              .aggregators(new CountAggregatorFactory("a0"))
              .context(QUERY_CONTEXT_DEFAULT);
    if (NullHandling.sqlCompatible()) {
      builder = builder.virtualColumns(
                           expressionVirtualColumn("v0", "regexp_extract(\"dim1\",'^1')", ColumnType.STRING),
                           expressionVirtualColumn("v1", "regexp_extract(concat('Z',\"dim1\"),'^Z2')", ColumnType.STRING)
                       )
                       .filters(
                           or(
                               notNull("v0"),
                               notNull("v1")
                           )
                       );
    } else {
      builder = builder.virtualColumns(
                           expressionVirtualColumn("v0", "regexp_extract(concat('Z',\"dim1\"),'^Z2')", ColumnType.STRING)
                       )
                       .filters(
                           or(
                               not(selector(
                                   "dim1",
                                   NullHandling.defaultStringValue(),
                                   new RegexDimExtractionFn("^1", 0, true, null)
                               )),
                               notNull("v0")
                           )
                       );
    }
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM foo\n"
        + "WHERE REGEXP_EXTRACT(dim1, '^1') IS NOT NULL OR REGEXP_EXTRACT('Z' || dim1, '^Z2') IS NOT NULL",
        ImmutableList.of(builder.build()),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testRegexpLikeFilter()
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM foo\n"
        + "WHERE REGEXP_LIKE(dim1, '^1') OR REGEXP_LIKE('Z' || dim1, '^Z2')",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "concat('Z',\"dim1\")", ColumnType.STRING)
                  )
                  .filters(
                      or(
                          new RegexDimFilter("dim1", "^1", null),
                          new RegexDimFilter("v0", "^Z2", null)
                      )
                  )
                  .aggregators(new CountAggregatorFactory("a0"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @DecoupledTestConfig(nativeQueryIgnore = NativeQueryIgnore.AGG_COL_EXCHANGE)
  @Test
  public void testGroupBySortPushDown()
  {
    testQuery(
        "SELECT dim2, dim1, SUM(cnt) FROM druid.foo GROUP BY dim2, dim1 ORDER BY dim1 LIMIT 4",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("dim1", "d1")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new LongSumAggregatorFactory("a0", "cnt")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)
                                ),
                                4
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "", 1L},
            new Object[]{"a", "1", 1L},
            new Object[]{NULL_STRING, "10.1", 1L},
            new Object[]{"", "2", 1L}
        )
    );
  }

  @Test
  public void testGroupByLimitPushDownWithHavingOnLong()
  {
    testQuery(
        "SELECT dim1, dim2, SUM(cnt) AS thecnt "
        + "FROM druid.foo "
        + "group by dim1, dim2 "
        + "having SUM(cnt) = 1 "
        + "order by dim2 "
        + "limit 4",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0"),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new LongSumAggregatorFactory("a0", "cnt")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)
                                ),
                                4
                            )
                        )
                        .setHavingSpec(having(equality("a0", 1L, ColumnType.LONG)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"10.1", "", 1L},
            new Object[]{"2", "", 1L},
            new Object[]{"abc", "", 1L},
            new Object[]{"", "a", 1L}
        ) :
        ImmutableList.of(
            new Object[]{"10.1", null, 1L},
            new Object[]{"abc", null, 1L},
            new Object[]{"2", "", 1L},
            new Object[]{"", "a", 1L}
        )
    );
  }

  @DecoupledTestConfig(nativeQueryIgnore = NativeQueryIgnore.IMPROVED_PLAN)
  @Test
  public void testGroupByLimitPushdownExtraction()
  {
    cannotVectorize();

    testQuery(
        "SELECT dim4, substring(dim5, 1, 1), count(*) FROM druid.numfoo WHERE dim4 = 'a' GROUP BY 1,2 LIMIT 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0"),
                                new ExtractionDimensionSpec("dim5", "_d1", new SubstringDimExtractionFn(0, 1))
                            )
                        )
                        .setVirtualColumns(expressionVirtualColumn("v0", "'a'", ColumnType.STRING))
                        .setDimFilter(equality("dim4", "a", ColumnType.STRING))
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(),
                                2
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "a", 2L},
            new Object[]{"a", "b", 1L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloor()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) = TIMESTAMP '2000-01-01 00:00:00'\n"
        + "OR FLOOR(__time TO MONTH) = TIMESTAMP '2000-02-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000/P2M")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testGroupAndFilterOnTimeFloorWithTimeZone()
  {
    testQuery(
        "SELECT TIME_FLOOR(__time, 'P1M', NULL, 'America/Los_Angeles'), COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE\n"
        + "TIME_FLOOR(__time, 'P1M', NULL, 'America/Los_Angeles') = "
        + "  TIME_PARSE('2000-01-01 00:00:00', NULL, 'America/Los_Angeles')\n"
        + "OR TIME_FLOOR(__time, 'P1M', NULL, 'America/Los_Angeles') = "
        + "  TIME_PARSE('2000-02-01 00:00:00', NULL, 'America/Los_Angeles')\n"
        + "GROUP BY 1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01T00-08:00/2000-03-01T00-08:00")))
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimes.inferTzFromString(LOS_ANGELES)))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                Calcites.jodaToCalciteTimestamp(
                    new DateTime("2000-01-01", DateTimes.inferTzFromString(LOS_ANGELES)),
                    DateTimeZone.UTC
                ),
                2L
            }
        )
    );
  }

  @NotYetSupported(Modes.PLAN_MISMATCH)
  @Test
  public void testFilterOnCurrentTimestampWithIntervalArithmetic()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "  __time >= CURRENT_TIMESTAMP + INTERVAL '01:02' HOUR TO MINUTE\n"
        + "  AND __time < TIMESTAMP '2003-02-02 01:00:00' - INTERVAL '1 1' DAY TO HOUR - INTERVAL '1-1' YEAR TO MONTH",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01T01:02/2002")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testFilterOnCurrentTimestampLosAngeles()
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-02T00Z/2002-01-01T08Z")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_LOS_ANGELES)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @NotYetSupported(Modes.PLAN_MISMATCH)
  @Test
  public void testFilterOnCurrentTimestampOnView()
  {
    testQuery(
        "SELECT * FROM view.bview",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-02/2002")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testFilterOnCurrentTimestampLosAngelesOnView()
  {
    // Tests that query context still applies to view SQL; note the result is different from
    // "testFilterOnCurrentTimestampOnView" above.
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT * FROM view.bview",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-02T00Z/2002-01-01T08Z")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_LOS_ANGELES)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testFilterOnNotTimeFloor()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) <> TIMESTAMP '2001-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(
                      new Interval(DateTimes.MIN, DateTimes.of("2001-01-01")),
                      new Interval(DateTimes.of("2001-02-01"), DateTimes.MAX)
                  ))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloorComparison()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) < TIMESTAMP '2000-02-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(new Interval(DateTimes.MIN, DateTimes.of("2000-02-01"))))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloorComparisonMisaligned()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) < TIMESTAMP '2000-02-01 00:00:01'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(new Interval(DateTimes.MIN, DateTimes.of("2000-03-01"))))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeExtract()
  {
    // Cannot vectorize due to expression filter.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE EXTRACT(YEAR FROM __time) = 2000\n"
        + "AND EXTRACT(MONTH FROM __time) = 1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'YEAR','UTC')", ColumnType.LONG),
                      expressionVirtualColumn("v1", "timestamp_extract(\"__time\",'MONTH','UTC')", ColumnType.LONG)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(
                      and(
                          equality("v0", 2000L, ColumnType.LONG),
                          equality("v1", 1L, ColumnType.LONG)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeExtractWithMultipleDays()
  {
    // Cannot vectorize due to expression filters.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE EXTRACT(YEAR FROM __time) = 2000\n"
        + "AND EXTRACT(DAY FROM __time) IN (2, 3, 5)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn(
                          "v0",
                          "timestamp_extract(\"__time\",'YEAR','UTC')",
                          ColumnType.LONG
                      ),
                      expressionVirtualColumn(
                          "v1",
                          "timestamp_extract(\"__time\",'DAY','UTC')",
                          ColumnType.LONG
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(
                      and(
                          equality("v0", 2000L, ColumnType.LONG),
                          NullHandling.replaceWithDefault()
                          ? in("v1", ImmutableList.of("2", "3", "5"), null)
                          : or(
                              equality("v1", 2L, ColumnType.LONG),
                              equality("v1", 3L, ColumnType.LONG),
                              equality("v1", 5L, ColumnType.LONG)
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testFilterOnTimeExtractWithVariousTimeUnits()
  {
    msqIncompatible();
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*) FROM druid.foo4\n"
        + "WHERE EXTRACT(YEAR FROM __time) = 2000\n"
        + "AND EXTRACT(MICROSECOND FROM __time) = 946723\n"
        + "AND EXTRACT(MILLISECOND FROM __time) = 695\n"
        + "AND EXTRACT(ISODOW FROM __time) = 6\n"
        + "AND EXTRACT(ISOYEAR FROM __time) = 2000\n"
        + "AND EXTRACT(DECADE FROM __time) = 200\n"
        + "AND EXTRACT(CENTURY FROM __time) = 20\n"
        + "AND EXTRACT(MILLENNIUM FROM __time) = 2\n",
        QUERY_CONTEXT_DEFAULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE4)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'YEAR','UTC')", ColumnType.LONG),
                      expressionVirtualColumn(
                          "v1",
                          "timestamp_extract(\"__time\",'MICROSECOND','UTC')",
                          ColumnType.LONG
                      ),
                      expressionVirtualColumn(
                          "v2",
                          "timestamp_extract(\"__time\",'MILLISECOND','UTC')",
                          ColumnType.LONG
                      ),
                      expressionVirtualColumn("v3", "timestamp_extract(\"__time\",'ISODOW','UTC')", ColumnType.LONG),
                      expressionVirtualColumn("v4", "timestamp_extract(\"__time\",'ISOYEAR','UTC')", ColumnType.LONG),
                      expressionVirtualColumn("v5", "timestamp_extract(\"__time\",'DECADE','UTC')", ColumnType.LONG),
                      expressionVirtualColumn("v6", "timestamp_extract(\"__time\",'CENTURY','UTC')", ColumnType.LONG),
                      expressionVirtualColumn("v7", "timestamp_extract(\"__time\",'MILLENNIUM','UTC')", ColumnType.LONG)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(
                      and(
                          equality("v0", 2000L, ColumnType.LONG),
                          equality("v1", 946723L, ColumnType.LONG),
                          equality("v2", 695L, ColumnType.LONG),
                          equality("v3", 6L, ColumnType.LONG),
                          equality("v4", 2000L, ColumnType.LONG),
                          equality("v5", 200L, ColumnType.LONG),
                          equality("v6", 20L, ColumnType.LONG),
                          equality("v7", 2L, ColumnType.LONG)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloorMisaligned()
  {
    msqIncompatible();
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE floor(__time TO month) = TIMESTAMP '2000-01-01 00:00:01'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec())
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{0L})
    );
  }

  @Test
  public void testGroupByFloor()
  {
    testQuery(
        "SELECT floor(CAST(dim1 AS float)), COUNT(*) FROM druid.foo GROUP BY floor(CAST(dim1 AS float))",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "floor(CAST(\"dim1\", 'DOUBLE'))", ColumnType.FLOAT)
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.FLOAT)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultFloatValue(), 3L},
            new Object[]{1.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{10.0f, 1L}
        )
    );
  }

  @DecoupledTestConfig(nativeQueryIgnore = NativeQueryIgnore.SLIGHTLY_WORSE_PLAN)
  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testQueryWithSelectProjectAndIdentityProjectDoesNotRename()
  {
    msqIncompatible();
    cannotVectorize();
    skipVectorize();
    testQuery(
        PLANNER_CONFIG_NO_HLL.withOverrides(
            ImmutableMap.of(
                PlannerConfig.CTX_KEY_USE_GROUPING_SET_FOR_EXACT_DISTINCT,
                "true"
            )
        ),
        "SELECT\n"
        + "(SUM(CASE WHEN (TIMESTAMP '2000-01-04 17:00:00'<=__time AND __time<TIMESTAMP '2022-01-05 17:00:00') THEN 1 ELSE 0 END)*1.0/COUNT(DISTINCT CASE WHEN (TIMESTAMP '2000-01-04 17:00:00'<=__time AND __time<TIMESTAMP '2022-01-05 17:00:00') THEN dim1 END))\n"
        + "FROM druid.foo\n"
        + "GROUP BY ()",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setVirtualColumns(
                                                expressionVirtualColumn(
                                                    "v0",
                                                    "((\"__time\" >= 947005200000) && (\"__time\" < 1641402000000))",
                                                    ColumnType.LONG
                                                )
                                            )
                                            .setDimensions(
                                                dimensions(
                                                    new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING),
                                                    new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                                                )
                                            )
                                            .setAggregatorSpecs(
                                                aggregators(
                                                    new FilteredAggregatorFactory(
                                                        new CountAggregatorFactory("a0"),
                                                        range(
                                                            "__time",
                                                            ColumnType.LONG,
                                                            timestamp("2000-01-04T17:00:00"),
                                                            timestamp("2022-01-05T17:00:00"),
                                                            false,
                                                            true
                                                        )
                                                    ),
                                                    new GroupingAggregatorFactory(
                                                        "a1",
                                                        ImmutableList.of("dim1", "v0")
                                                    )
                                                )
                                            )
                                            .setSubtotalsSpec(
                                                ImmutableList.of(
                                                    ImmutableList.of("d0", "d1"),
                                                    ImmutableList.of()
                                                )
                                            )
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            aggregators(
                                new FilteredAggregatorFactory(
                                    new LongMinAggregatorFactory("_a0", "a0"),
                                    equality("a1", 3L, ColumnType.LONG)
                                ),
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("_a1"),
                                    and(
                                        notNull("d0"),
                                        equality("a1", 0L, ColumnType.LONG),
                                        expressionFilter("\"d1\"")
                                    )
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            expressionPostAgg(
                                "p0",
                                "(CAST(\"_a0\", 'DOUBLE') / \"_a1\")",
                                ColumnType.DOUBLE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1.0d}
        )
    );
  }


  @Test
  public void testGroupByFloorWithOrderBy()
  {
    testQuery(
        "SELECT floor(CAST(dim1 AS float)) AS fl, COUNT(*) FROM druid.foo GROUP BY floor(CAST(dim1 AS float)) ORDER BY fl DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "floor(CAST(\"dim1\", 'DOUBLE'))",
                                ColumnType.FLOAT
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec(
                                    "v0",
                                    "d0",
                                    ColumnType.FLOAT
                                )
                            )
                        )
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{10.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{1.0f, 1L},
            new Object[]{NullHandling.defaultFloatValue(), 3L}
        )
    );
  }

  @Test
  public void testGroupByOrderByBoolean()
  {
    // Not msqCompatible until https://github.com/apache/druid/pull/14046 is merged.
    testQuery(
        "SELECT dim1 = 'abc', COUNT(*) FROM druid.foo GROUP BY 1 ORDER BY 1, 2 LIMIT 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "(\"dim1\" == 'abc')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec(
                                    "v0",
                                    "d0",
                                    ColumnType.LONG
                                )
                            )
                        )
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    ),
                                    new OrderByColumnSpec(
                                        "a0",
                                        Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                2
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{false, 5L},
            new Object[]{true, 1L}
        )
    );
  }

  @Test
  public void testGroupByFloorTimeAndOneOtherDimensionWithOrderBy()
  {
    testQuery(
        "SELECT floor(__time TO year), dim2, COUNT(*)"
        + " FROM druid.foo"
        + " GROUP BY floor(__time TO year), dim2"
        + " ORDER BY floor(__time TO year), dim2, COUNT(*) DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1Y',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.LONG),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    ),
                                    new OrderByColumnSpec(
                                        "d1",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.LEXICOGRAPHIC
                                    ),
                                    new OrderByColumnSpec(
                                        "a0",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d0", 0, Granularities.YEAR))
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{timestamp("2000"), "", 2L},
            new Object[]{timestamp("2000"), "a", 1L},
            new Object[]{timestamp("2001"), "", 1L},
            new Object[]{timestamp("2001"), "a", 1L},
            new Object[]{timestamp("2001"), "abc", 1L}
        ) :
        ImmutableList.of(
            new Object[]{timestamp("2000"), null, 1L},
            new Object[]{timestamp("2000"), "", 1L},
            new Object[]{timestamp("2000"), "a", 1L},
            new Object[]{timestamp("2001"), null, 1L},
            new Object[]{timestamp("2001"), "a", 1L},
            new Object[]{timestamp("2001"), "abc", 1L}
        )
    );
  }

  @Test
  public void testGroupByStringLength()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT CHARACTER_LENGTH(dim1), COUNT(*) FROM druid.foo GROUP BY CHARACTER_LENGTH(dim1)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "strlen(\"dim1\")", ColumnType.LONG))
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{0, 1L},
            new Object[]{1, 2L},
            new Object[]{3, 2L},
            new Object[]{4, 1L}
        )
    );
  }

  @Test
  public void testGroupByExpressionFromLookup()
  {
    msqIncompatible();
    // Cannot vectorize direct queries on lookup tables.
    cannotVectorize();

    testQuery(
        "SELECT SUBSTRING(v, 1, 1), COUNT(*) FROM lookup.lookyloo GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(new LookupDataSource("lookyloo"))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new ExtractionDimensionSpec(
                                    "v",
                                    "d0",
                                    new SubstringDimExtractionFn(0, 1)
                                )
                            )
                        )
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"m", 1L},
            new Object[]{"x", 3L}
        )
    );
  }

  @Test
  public void testTimeseries()
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, timestamp("2000-01-01")},
            new Object[]{3L, timestamp("2001-01-01")}
        )
    );
  }

  @Test
  public void testFilteredTimeAggregators()
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt) FILTER(WHERE __time >= TIMESTAMP '2000-01-01 00:00:00'\n"
        + "                    AND __time <  TIMESTAMP '2000-02-01 00:00:00'),\n"
        + "  SUM(cnt) FILTER(WHERE __time >= TIMESTAMP '2000-01-01 00:00:01'\n"
        + "                    AND __time <  TIMESTAMP '2000-02-01 00:00:00'),\n"
        + "  SUM(cnt) FILTER(WHERE __time >= TIMESTAMP '2001-01-01 00:00:00'\n"
        + "                    AND __time <  TIMESTAMP '2001-02-01 00:00:00')\n"
        + "FROM foo\n"
        + "WHERE\n"
        + "  __time >= TIMESTAMP '2000-01-01 00:00:00'\n"
        + "  AND __time < TIMESTAMP '2001-02-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-02-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          range(
                              "__time",
                              ColumnType.LONG,
                              timestamp("2000-01-01T00:00:00"),
                              timestamp("2000-02-01"),
                              false,
                              true
                          )
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a1", "cnt"),
                          range(
                              "__time",
                              ColumnType.LONG,
                              timestamp("2000-01-01T00:00:01"),
                              timestamp("2000-02-01"),
                              false,
                              true
                          )
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a2", "cnt"),
                          range(
                              "__time",
                              ColumnType.LONG,
                              timestamp("2001-01-01"),
                              timestamp("2001-02-01"),
                              false,
                              true
                          )
                      )
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 2L, 3L}
        )
    );
  }


  @Test
  public void testTimeseriesLosAngelesViaQueryContext()
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT FLOOR(__time TO MONTH) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimes.inferTzFromString(LOS_ANGELES)))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_LOS_ANGELES, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2000-01-01", LOS_ANGELES)},
            new Object[]{1L, timestamp("2000-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2001-01-01", LOS_ANGELES)}
        )
    );
  }

  @Test
  public void testTimeseriesLosAngelesViaPlannerConfig()
  {
    testQuery(
        PLANNER_CONFIG_LOS_ANGELES,
        QUERY_CONTEXT_DEFAULT,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT\n"
        + "    FLOOR(__time TO MONTH) AS gran,\n"
        + "    cnt\n"
        + "  FROM druid.foo\n"
        + "  WHERE __time >= TIME_PARSE('1999-12-01 00:00:00') AND __time < TIME_PARSE('2002-01-01 00:00:00')\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("1999-12-01T00-08:00/2002-01-01T00-08:00")))
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimes.inferTzFromString(LOS_ANGELES)))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2000-01-01", LOS_ANGELES)},
            new Object[]{1L, timestamp("2000-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2001-01-01", LOS_ANGELES)}
        )
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloor()
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, timestamp("2000-01-01")},
            new Object[]{3L, timestamp("2001-01-01")}
        )
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloorWithTimeShift()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(TIME_SHIFT(__time, 'P1D', -1), 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(timestamp_shift(\"__time\",'P1D',-1,'UTC'),'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(NoopLimitSpec.instance())
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01")},
            new Object[]{2L, timestamp("2000-01-01")},
            new Object[]{1L, timestamp("2000-12-01")},
            new Object[]{2L, timestamp("2001-01-01")}
        )
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloorWithTimestampAdd()
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(TIMESTAMPADD(DAY, -1, __time), 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor((\"__time\" + -86400000),'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(NoopLimitSpec.instance())
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01")},
            new Object[]{2L, timestamp("2000-01-01")},
            new Object[]{1L, timestamp("2000-12-01")},
            new Object[]{2L, timestamp("2001-01-01")}
        )
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloorWithOrigin()
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M', TIMESTAMP '1970-01-01 01:02:03') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(
                      new PeriodGranularity(
                          Period.months(1),
                          DateTimes.of("1970-01-01T01:02:03"),
                          DateTimeZone.UTC
                      )
                  )
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01T01:02:03")},
            new Object[]{2L, timestamp("2000-01-01T01:02:03")},
            new Object[]{1L, timestamp("2000-12-01T01:02:03")},
            new Object[]{2L, timestamp("2001-01-01T01:02:03")}
        )
    );
  }

  @Test
  public void testTimeseriesLosAngelesUsingTimeFloorConnectionUtc()
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M', CAST(NULL AS TIMESTAMP), 'America/Los_Angeles') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimes.inferTzFromString(LOS_ANGELES)))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01T08")},
            new Object[]{2L, timestamp("2000-01-01T08")},
            new Object[]{1L, timestamp("2000-12-01T08")},
            new Object[]{2L, timestamp("2001-01-01T08")}
        )
    );
  }

  @Test
  public void testTimeseriesLosAngelesUsingTimeFloorConnectionLosAngeles()
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimes.inferTzFromString(LOS_ANGELES)))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_LOS_ANGELES, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2000-01-01", LOS_ANGELES)},
            new Object[]{1L, timestamp("2000-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2001-01-01", LOS_ANGELES)}
        )
    );
  }

  @Test
  public void testTimeseriesDontSkipEmptyBuckets()
  {
    msqIncompatible();
    // Tests that query context parameters are passed through to the underlying query engine.
    Long defaultVal = NullHandling.replaceWithDefault() ? 0L : null;
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT floor(__time TO HOUR) AS gran, cnt FROM druid.foo\n"
        + "  WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00'\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000/2000-01-02")))
                  .granularity(new PeriodGranularity(Period.hours(1), null, DateTimeZone.UTC))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS, "d0"))
                  .build()
        ),
        ImmutableList.<Object[]>builder()
                     .add(new Object[]{1L, timestamp("2000-01-01")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T01")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T02")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T03")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T04")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T05")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T06")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T07")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T08")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T09")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T10")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T11")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T12")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T13")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T14")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T15")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T16")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T17")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T18")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T19")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T20")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T21")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T22")})
                     .add(new Object[]{defaultVal, timestamp("2000-01-01T23")})
                     .build()
    );
  }

  @Test
  public void testTimeseriesUsingCastAsDate()
  {
    testQuery(
        "SELECT SUM(cnt), dt FROM (\n"
        + "  SELECT CAST(__time AS DATE) AS dt,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY dt\n"
        + "ORDER BY dt",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(new PeriodGranularity(Period.days(1), null, DateTimeZone.UTC))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, day("2000-01-01")},
            new Object[]{1L, day("2000-01-02")},
            new Object[]{1L, day("2000-01-03")},
            new Object[]{1L, day("2001-01-01")},
            new Object[]{1L, day("2001-01-02")},
            new Object[]{1L, day("2001-01-03")}
        )
    );
  }

  @Test
  public void testTimeseriesUsingFloorPlusCastAsDate()
  {
    testQuery(
        "SELECT SUM(cnt), dt FROM (\n"
        + "  SELECT CAST(FLOOR(__time TO QUARTER) AS DATE) AS dt,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY dt\n"
        + "ORDER BY dt",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(new PeriodGranularity(Period.months(3), null, DateTimeZone.UTC))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, day("2000-01-01")},
            new Object[]{3L, day("2001-01-01")}
        )
    );
  }

  @Test
  public void testTimeseriesDescending()
  {
    // Cannot vectorize due to descending order.
    cannotVectorize();

    testQuery(
        "SELECT gran, SUM(cnt) FROM (\n"
        + "  SELECT floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran DESC",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .descending(true)
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2001-01-01"), 3L},
            new Object[]{timestamp("2000-01-01"), 3L}
        )
    );
  }

  @Test
  public void testTimeseriesEmptyResultsAggregatorDefaultValues()
  {
    msqIncompatible();
    // timeseries with all granularity have a single group, so should return default results for given aggregators
    testQuery(
        "SELECT\n"
        + " count(*),\n"
        + " COUNT(DISTINCT dim1),\n"
        + " APPROX_COUNT_DISTINCT(distinct dim1),\n"
        + " sum(d1),\n"
        + " max(d1),\n"
        + " min(d1),\n"
        + " sum(l1),\n"
        + " max(l1),\n"
        + " min(l1),\n"
        + " avg(l1),\n"
        + " avg(d1)\n"
        + "FROM druid.numfoo WHERE dim2 = 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(numericEquality("dim2", 0L, ColumnType.LONG))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new CountAggregatorFactory("a0"),

                          new CardinalityAggregatorFactory(
                              "a1",
                              null,
                              ImmutableList.of(DefaultDimensionSpec.of("dim1")),
                              false,
                              true
                          ),
                          new CardinalityAggregatorFactory(
                              "a2",
                              null,
                              ImmutableList.of(DefaultDimensionSpec.of("dim1")),
                              false,
                              true
                          ),
                          new DoubleSumAggregatorFactory("a3", "d1"),
                          new DoubleMaxAggregatorFactory("a4", "d1"),
                          new DoubleMinAggregatorFactory("a5", "d1"),
                          new LongSumAggregatorFactory("a6", "l1"),
                          new LongMaxAggregatorFactory("a7", "l1"),
                          new LongMinAggregatorFactory("a8", "l1"),
                          new DoubleSumAggregatorFactory("a9:sum", "l1"),
                          useDefault
                          ? new CountAggregatorFactory("a9:count")
                          : new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a9:count"),
                              notNull("l1")
                          ),
                          new DoubleSumAggregatorFactory("a10:sum", "d1"),
                          useDefault
                          ? new CountAggregatorFactory("a10:count")
                          : new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a10:count"),
                              notNull("d1")
                          )
                      )
                  )
                  .postAggregators(
                      new ArithmeticPostAggregator(
                          "a9",
                          "quotient",
                          ImmutableList.of(
                              new FieldAccessPostAggregator(null, "a9:sum"),
                              new FieldAccessPostAggregator(null, "a9:count")
                          )
                      ),
                      new ArithmeticPostAggregator(
                          "a10",
                          "quotient",
                          ImmutableList.of(
                              new FieldAccessPostAggregator(null, "a10:sum"),
                              new FieldAccessPostAggregator(null, "a10:count")
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{
                0L,
                0L,
                0L,
                0.0,
                Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY,
                0L,
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                Double.NaN,
                Double.NaN
            }
            : new Object[]{0L, 0L, 0L, null, null, null, null, null, null, null, null}
        )
    );
  }

  @Test
  public void testTimeseriesEmptyResultsAggregatorDefaultValuesNonVectorized()
  {
    // Empty-dataset aggregation queries in MSQ return an empty row, rather than a single row as SQL requires.
    msqIncompatible();

    cannotVectorize();
    skipVectorize();
    // timeseries with all granularity have a single group, so should return default results for given aggregators
    testQuery(
        "SELECT\n"
        + " ANY_VALUE(dim1, 1024),\n"
        + " ANY_VALUE(l1),\n"
        + " EARLIEST(dim1, 1024),\n"
        + " EARLIEST(l1),\n"
        + " LATEST(dim1, 1024),\n"
        + " LATEST(l1),\n"
        + " ARRAY_AGG(DISTINCT dim3),\n"
        + " STRING_AGG(DISTINCT dim3, '|'),\n"
        + " LISTAGG(DISTINCT dim3, '|'),\n"
        + " BIT_AND(l1),\n"
        + " BIT_OR(l1),\n"
        + " BIT_XOR(l1)\n"
        + "FROM druid.numfoo WHERE dim2 = 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(numericEquality("dim2", 0L, ColumnType.LONG))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new StringAnyAggregatorFactory("a0", "dim1", 1024, true),
                          new LongAnyAggregatorFactory("a1", "l1"),
                          new StringFirstAggregatorFactory("a2", "dim1", null, 1024),
                          new LongFirstAggregatorFactory("a3", "l1", null),
                          new StringLastAggregatorFactory("a4", "dim1", null, 1024),
                          new LongLastAggregatorFactory("a5", "l1", null),
                          new ExpressionLambdaAggregatorFactory(
                              "a6",
                              ImmutableSet.of("dim3"),
                              "__acc",
                              "ARRAY<STRING>[]",
                              "ARRAY<STRING>[]",
                              true,
                              true,
                              false,
                              "array_set_add(\"__acc\", \"dim3\")",
                              "array_set_add_all(\"__acc\", \"a6\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a7",
                                  ImmutableSet.of("dim3"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_set_add(\"__acc\", \"dim3\")",
                                  "array_set_add_all(\"__acc\", \"a7\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, '|'))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("dim3")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a8",
                                  ImmutableSet.of("dim3"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_set_add(\"__acc\", \"dim3\")",
                                  "array_set_add_all(\"__acc\", \"a8\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, '|'))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("dim3")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a9",
                                  ImmutableSet.of("l1"),
                                  "__acc",
                                  "0",
                                  "0",
                                  NullHandling.sqlCompatible(),
                                  false,
                                  false,
                                  "bitwiseAnd(\"__acc\", \"l1\")",
                                  "bitwiseAnd(\"__acc\", \"a9\")",
                                  null,
                                  null,
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("l1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a10",
                                  ImmutableSet.of("l1"),
                                  "__acc",
                                  "0",
                                  "0",
                                  NullHandling.sqlCompatible(),
                                  false,
                                  false,
                                  "bitwiseOr(\"__acc\", \"l1\")",
                                  "bitwiseOr(\"__acc\", \"a10\")",
                                  null,
                                  null,
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("l1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a11",
                                  ImmutableSet.of("l1"),
                                  "__acc",
                                  "0",
                                  "0",
                                  NullHandling.sqlCompatible(),
                                  false,
                                  false,
                                  "bitwiseXor(\"__acc\", \"l1\")",
                                  "bitwiseXor(\"__acc\", \"a11\")",
                                  null,
                                  null,
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("l1")
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{"", 0L, "", 0L, "", 0L, null, "", "", 0L, 0L, 0L}
            : new Object[]{null, null, null, null, null, null, null, null, null, null, null, null}
        )
    );
  }


  @Test
  public void testGroupByAggregatorDefaultValues()
  {
    testQuery(
        "SELECT\n"
        + " dim2,\n"
        + " count(*) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " COUNT(DISTINCT dim1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " APPROX_COUNT_DISTINCT(distinct dim1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " sum(d1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " max(d1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " min(d1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " sum(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " max(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " min(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " avg(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " avg(d1) FILTER(WHERE dim1 = 'nonexistent')\n"
        + "FROM druid.numfoo WHERE dim2 = 'a' GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(equality("dim2", "a", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "'a'", ColumnType.STRING))
                        .setDimensions(new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            aggregators(
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a0"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new CardinalityAggregatorFactory(
                                        "a1",
                                        null,
                                        ImmutableList.of(DefaultDimensionSpec.of("dim1")),
                                        false,
                                        true
                                    ),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new CardinalityAggregatorFactory(
                                        "a2",
                                        null,
                                        ImmutableList.of(DefaultDimensionSpec.of("dim1")),
                                        false,
                                        true
                                    ),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new DoubleSumAggregatorFactory("a3", "d1"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new DoubleMaxAggregatorFactory("a4", "d1"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new DoubleMinAggregatorFactory("a5", "d1"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new LongSumAggregatorFactory("a6", "l1"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new LongMaxAggregatorFactory("a7", "l1"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new LongMinAggregatorFactory("a8", "l1"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new DoubleSumAggregatorFactory("a9:sum", "l1"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                useDefault
                                ? new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a9:count"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                )
                                : new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a9:count"),
                                    and(
                                        notNull("l1"),
                                        equality("dim1", "nonexistent", ColumnType.STRING)
                                    )
                                ),
                                new FilteredAggregatorFactory(
                                    new DoubleSumAggregatorFactory("a10:sum", "d1"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                useDefault
                                ? new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a10:count"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                )
                                : new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a10:count"),
                                    and(
                                        notNull("d1"),
                                        equality("dim1", "nonexistent", ColumnType.STRING)
                                    )
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new ArithmeticPostAggregator(
                                    "a9",
                                    "quotient",
                                    ImmutableList.of(new FieldAccessPostAggregator(
                                        null,
                                        "a9:sum"
                                    ), new FieldAccessPostAggregator(null, "a9:count"))
                                ),
                                new ArithmeticPostAggregator(
                                    "a10",
                                    "quotient",
                                    ImmutableList.of(new FieldAccessPostAggregator(
                                        null,
                                        "a10:sum"
                                    ), new FieldAccessPostAggregator(null, "a10:count"))
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{
                "a",
                0L,
                0L,
                0L,
                0.0,
                Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY,
                0L,
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                Double.NaN,
                Double.NaN
            }
            : new Object[]{"a", 0L, 0L, 0L, null, null, null, null, null, null, null, null}
        )
    );
  }


  @Test
  public void testGroupByAggregatorDefaultValuesNonVectorized()
  {
    cannotVectorize();
    skipVectorize();
    testQuery(
        "SELECT\n"
        + " dim2,\n"
        + " ANY_VALUE(dim1, 1024) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " ANY_VALUE(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " EARLIEST(dim1, 1024) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " EARLIEST(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " LATEST(dim1, 1024) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " LATEST(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " ARRAY_AGG(DISTINCT dim3) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " STRING_AGG(DISTINCT dim3, '|') FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " LISTAGG(DISTINCT dim3, '|') FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " BIT_AND(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " BIT_OR(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + " BIT_XOR(l1) FILTER(WHERE dim1 = 'nonexistent')\n"
        + "FROM druid.numfoo WHERE dim2 = 'a' GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(equality("dim2", "a", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "'a'", ColumnType.STRING))
                        .setDimensions(new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            aggregators(
                                new FilteredAggregatorFactory(
                                    new StringAnyAggregatorFactory("a0", "dim1", 1024, true),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new LongAnyAggregatorFactory("a1", "l1"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new StringFirstAggregatorFactory("a2", "dim1", null, 1024),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new LongFirstAggregatorFactory("a3", "l1", null),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new StringLastAggregatorFactory("a4", "dim1", null, 1024),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new LongLastAggregatorFactory("a5", "l1", null),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new ExpressionLambdaAggregatorFactory(
                                        "a6",
                                        ImmutableSet.of("dim3"),
                                        "__acc",
                                        "ARRAY<STRING>[]",
                                        "ARRAY<STRING>[]",
                                        true,
                                        true,
                                        false,
                                        "array_set_add(\"__acc\", \"dim3\")",
                                        "array_set_add_all(\"__acc\", \"a6\")",
                                        null,
                                        null,
                                        ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new ExpressionLambdaAggregatorFactory(
                                        "a7",
                                        ImmutableSet.of("dim3"),
                                        "__acc",
                                        "[]",
                                        "[]",
                                        true,
                                        false,
                                        false,
                                        "array_set_add(\"__acc\", \"dim3\")",
                                        "array_set_add_all(\"__acc\", \"a7\")",
                                        null,
                                        "if(array_length(o) == 0, null, array_to_string(o, '|'))",
                                        ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    and(
                                        notNull("dim3"),
                                        equality("dim1", "nonexistent", ColumnType.STRING)
                                    )
                                ),
                                new FilteredAggregatorFactory(
                                    new ExpressionLambdaAggregatorFactory(
                                        "a8",
                                        ImmutableSet.of("dim3"),
                                        "__acc",
                                        "[]",
                                        "[]",
                                        true,
                                        false,
                                        false,
                                        "array_set_add(\"__acc\", \"dim3\")",
                                        "array_set_add_all(\"__acc\", \"a8\")",
                                        null,
                                        "if(array_length(o) == 0, null, array_to_string(o, '|'))",
                                        ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    and(
                                        notNull("dim3"),
                                        equality("dim1", "nonexistent", ColumnType.STRING)
                                    )
                                ),
                                new FilteredAggregatorFactory(
                                    new ExpressionLambdaAggregatorFactory(
                                        "a9",
                                        ImmutableSet.of("l1"),
                                        "__acc",
                                        "0",
                                        "0",
                                        NullHandling.sqlCompatible(),
                                        false,
                                        false,
                                        "bitwiseAnd(\"__acc\", \"l1\")",
                                        "bitwiseAnd(\"__acc\", \"a9\")",
                                        null,
                                        null,
                                        ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    and(
                                        notNull("l1"),
                                        equality("dim1", "nonexistent", ColumnType.STRING)
                                    )
                                ),
                                new FilteredAggregatorFactory(
                                    new ExpressionLambdaAggregatorFactory(
                                        "a10",
                                        ImmutableSet.of("l1"),
                                        "__acc",
                                        "0",
                                        "0",
                                        NullHandling.sqlCompatible(),
                                        false,
                                        false,
                                        "bitwiseOr(\"__acc\", \"l1\")",
                                        "bitwiseOr(\"__acc\", \"a10\")",
                                        null,
                                        null,
                                        ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    and(
                                        notNull("l1"),
                                        equality("dim1", "nonexistent", ColumnType.STRING)
                                    )
                                ),
                                new FilteredAggregatorFactory(
                                    new ExpressionLambdaAggregatorFactory(
                                        "a11",
                                        ImmutableSet.of("l1"),
                                        "__acc",
                                        "0",
                                        "0",
                                        NullHandling.sqlCompatible(),
                                        false,
                                        false,
                                        "bitwiseXor(\"__acc\", \"l1\")",
                                        "bitwiseXor(\"__acc\", \"a11\")",
                                        null,
                                        null,
                                        ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    and(
                                        notNull("l1"),
                                        equality("dim1", "nonexistent", ColumnType.STRING)
                                    )
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{"a", "", 0L, "", 0L, "", 0L, null, "", "", 0L, 0L, 0L}
            : new Object[]{"a", null, null, null, null, null, null, null, null, null, null, null, null}
        )
    );
  }

  @Test
  public void testGroupByExtractYear()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  EXTRACT(YEAR FROM __time) AS \"year\",\n"
        + "  SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY EXTRACT(YEAR FROM __time)\n"
        + "ORDER BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_extract(\"__time\",'YEAR','UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(NoopLimitSpec.instance())
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{2000L, 3L},
            new Object[]{2001L, 3L}
        )
    );
  }

  @Test
  public void testGroupByFormatYearAndMonth()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  TIME_FORMAt(__time, 'yyyy MM') AS \"year\",\n"
        + "  SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY TIME_FORMAt(__time, 'yyyy MM')\n"
        + "ORDER BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_format(\"__time\",'yyyy MM','UTC')",
                                ColumnType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(NoopLimitSpec.instance())
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"2000 01", 3L},
            new Object[]{"2001 01", 3L}
        )
    );
  }

  @Test
  public void testGroupByExtractFloorTime()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "EXTRACT(YEAR FROM FLOOR(__time TO YEAR)) AS \"year\", SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY EXTRACT(YEAR FROM FLOOR(__time TO YEAR))",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_extract(timestamp_floor(\"__time\",'P1Y',null,'UTC'),'YEAR','UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{2000L, 3L},
            new Object[]{2001L, 3L}
        )
    );
  }

  @Test
  public void testGroupByExtractFloorTimeLosAngeles()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT\n"
        + "EXTRACT(YEAR FROM FLOOR(__time TO YEAR)) AS \"year\", SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY EXTRACT(YEAR FROM FLOOR(__time TO YEAR))",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_extract(timestamp_floor(\"__time\",'P1Y',null,'America/Los_Angeles'),'YEAR','America/Los_Angeles')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_LOS_ANGELES)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1999L, 1L},
            new Object[]{2000L, 3L},
            new Object[]{2001L, 2L}
        )
    );
  }

  @Test
  public void testTimeseriesWithLimitNoTopN()
  {
    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran\n"
        + "LIMIT 1",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .limit(1)
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 3L}
        )
    );
  }

  @Test
  public void testTimeseriesWithLimit()
  {
    testQuery(
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "LIMIT 1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .limit(1)
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 3L}
        )
    );
  }

  @Test
  public void testTimeseriesWithLimitAndOffset()
  {
    // Timeseries cannot handle offsets, so the query morphs into a groupBy.
    testQuery(
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "LIMIT 2\n"
        + "OFFSET 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(DefaultLimitSpec.builder().offset(1).limit(2).build())
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2001-01-01"), 3L}
        )
    );
  }

  @Test
  public void testTimeseriesWithOrderByAndLimit()
  {
    testQuery(
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran\n"
        + "LIMIT 1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .limit(1)
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 3L}
        )
    );
  }

  @Test
  public void testGroupByTimeAndOtherDimension()
  {
    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY dim2, gran\n"
        + "ORDER BY dim2, gran",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(NoopLimitSpec.instance())
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        ) :
        ImmutableList.of(
            new Object[]{null, timestamp("2000-01-01"), 1L},
            new Object[]{null, timestamp("2001-01-01"), 1L},
            new Object[]{"", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        )
    );
  }

  @NotYetSupported(Modes.PLAN_MISMATCH)
  @Test
  public void testGroupByTimeFloorAndDimOnGroupByTimeFloorAndDim()
  {
    testQuery(
        "SELECT dim2, time_floor(gran, 'P1M') gran, sum(s)\n"
        + "FROM (SELECT time_floor(__time, 'P1D') AS gran, dim2, sum(m1) as s FROM druid.foo GROUP BY 1, 2 HAVING sum(m1) > 1) AS x\n"
        + "GROUP BY 1, 2\n"
        + "ORDER BY dim2, gran desc",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setVirtualColumns(
                                                expressionVirtualColumn(
                                                    "v0",
                                                    "timestamp_floor(\"__time\",'P1D',null,'UTC')",
                                                    ColumnType.LONG
                                                )
                                            )
                                            .setDimensions(
                                                dimensions(
                                                    new DefaultDimensionSpec("v0", "d0", ColumnType.LONG),
                                                    new DefaultDimensionSpec("dim2", "d1")
                                                )
                                            )
                                            .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                                            .setPostAggregatorSpecs(
                                                expressionPostAgg(
                                                    "p0",
                                                    "timestamp_floor(\"d0\",'P1M',null,'UTC')",
                                                    ColumnType.LONG
                                                )
                                            )
                                            .setHavingSpec(
                                                having(
                                                    range(
                                                        "a0",
                                                        ColumnType.LONG,
                                                        1L,
                                                        null,
                                                        true,
                                                        false
                                                    )
                                                )
                                            )
                                            .setContext(
                                                withTimestampResultContext(
                                                    QUERY_CONTEXT_DEFAULT,
                                                    "d0",
                                                    0,
                                                    Granularities.DAY
                                                )
                                            )
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("d1", "_d0"),
                                new DefaultDimensionSpec("p0", "_d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("_a0", "a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("_d0", OrderByColumnSpec.Direction.ASCENDING),
                                    new OrderByColumnSpec(
                                        "_d1",
                                        Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", timestamp("2001-01-01"), 6.0},
            new Object[]{"", timestamp("2000-01-01"), 5.0},
            new Object[]{"a", timestamp("2001-01-01"), 4.0},
            new Object[]{"abc", timestamp("2001-01-01"), 5.0}
        ) :
        ImmutableList.of(
            new Object[]{null, timestamp("2001-01-01"), 6.0},
            new Object[]{null, timestamp("2000-01-01"), 2.0},
            new Object[]{"", timestamp("2000-01-01"), 3.0},
            new Object[]{"a", timestamp("2001-01-01"), 4.0},
            new Object[]{"abc", timestamp("2001-01-01"), 5.0}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testGroupingSets()
  {
    msqIncompatible();
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt), GROUPING(dim2, gran)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (dim2, gran), (dim2), (gran), () )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("a0", "cnt"),
                            new GroupingAggregatorFactory("a1", Arrays.asList("v0", "v1"))
                        ))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d1"),
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L, 0L},
            new Object[]{"", timestamp("2001-01-01"), 1L, 0L},
            new Object[]{"a", timestamp("2000-01-01"), 1L, 0L},
            new Object[]{"a", timestamp("2001-01-01"), 1L, 0L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L, 0L},
            new Object[]{"", null, 3L, 1L},
            new Object[]{"a", null, 2L, 1L},
            new Object[]{"abc", null, 1L, 1L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L, 2L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L, 2L},
            new Object[]{NULL_STRING, null, 6L, 3L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testGroupingAggregatorDifferentOrder()
  {
    msqIncompatible();

    testQuery(
        "SELECT dim2, gran, SUM(cnt), GROUPING(gran, dim2)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (dim2, gran), (dim2), (gran), () )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("a0", "cnt"),
                            new GroupingAggregatorFactory("a1", Arrays.asList("v1", "v0"))
                        ))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d1"),
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L, 0L},
            new Object[]{"", timestamp("2001-01-01"), 1L, 0L},
            new Object[]{"a", timestamp("2000-01-01"), 1L, 0L},
            new Object[]{"a", timestamp("2001-01-01"), 1L, 0L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L, 0L},
            new Object[]{"", null, 3L, 2L},
            new Object[]{"a", null, 2L, 2L},
            new Object[]{"abc", null, 1L, 2L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L, 1L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L, 1L},
            new Object[]{NULL_STRING, null, 6L, 3L}
        )
    );
  }

  @Test
  public void testGroupingAggregatorWithPostAggregator()
  {
    msqIncompatible();
    List<Object[]> resultList;
    if (NullHandling.sqlCompatible()) {
      resultList = ImmutableList.of(
          new Object[]{NULL_STRING, 2L, 0L, NULL_STRING},
          new Object[]{"", 1L, 0L, ""},
          new Object[]{"a", 2L, 0L, "a"},
          new Object[]{"abc", 1L, 0L, "abc"},
          new Object[]{NULL_STRING, 6L, 1L, "ALL"}
      );
    } else {
      resultList = ImmutableList.of(
          new Object[]{"", 3L, 0L, ""},
          new Object[]{"a", 2L, 0L, "a"},
          new Object[]{"abc", 1L, 0L, "abc"},
          new Object[]{NULL_STRING, 6L, 1L, "ALL"}
      );
    }
    testQuery(
        "SELECT dim2, SUM(cnt), GROUPING(dim2), \n"
        + "CASE WHEN GROUPING(dim2) = 1 THEN 'ALL' ELSE dim2 END\n"
        + "FROM druid.foo\n"
        + "GROUP BY GROUPING SETS ( (dim2), () )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("a0", "cnt"),
                            new GroupingAggregatorFactory("a1", Collections.singletonList("dim2"))
                        ))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0"),
                                ImmutableList.of()
                            )
                        )
                        .setPostAggregatorSpecs(
                            expressionPostAgg(
                                "p0",
                                "case_searched((\"a1\" == 1),'ALL',\"d0\")",
                                ColumnType.STRING
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        resultList
    );
  }

  @Test
  public void testGroupingSetsWithNumericDimension()
  {
    msqIncompatible();
    testQuery(
        "SELECT cnt, COUNT(*)\n"
        + "FROM foo\n"
        + "GROUP BY GROUPING SETS ( (cnt), () )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L},
            new Object[]{null, 6L}
        )
    );
  }

  @Test
  public void testGroupByRollup()
  {
    msqIncompatible();
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY ROLLUP (dim2, gran)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d1"),
                                ImmutableList.of("d0"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{"", null, 3L},
            new Object[]{"a", null, 2L},
            new Object[]{"abc", null, 1L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @Test
  public void testGroupByRollupDifferentOrder()
  {
    msqIncompatible();
    // Like "testGroupByRollup", but the ROLLUP exprs are in the reverse order.
    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY ROLLUP (gran, dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.LONG),
                                new DefaultDimensionSpec("v1", "d1")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d1"),
                                ImmutableList.of("d0"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d0", 0, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testGroupByCube()
  {
    msqIncompatible();
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY CUBE (dim2, gran)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d1"),
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{"", null, 3L},
            new Object[]{"a", null, 2L},
            new Object[]{"abc", null, 1L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testGroupingSetsWithDummyDimension()
  {
    msqIncompatible();
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (dim2, 'dummy', gran), (dim2), (gran), ('dummy') )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v2",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v2", "d2", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d2"),
                                ImmutableList.of("d0"),
                                ImmutableList.of(),
                                ImmutableList.of("d2")
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d2", 1, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{"", null, 3L},
            new Object[]{"a", null, 2L},
            new Object[]{"abc", null, 1L},
            new Object[]{NULL_STRING, null, 6L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testGroupingSetsNoSuperset()
  {
    msqIncompatible();
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    // Note: the grouping sets are reordered in the output of this query, but this is allowed.
    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (), (dim2), (gran) )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", null, 3L},
            new Object[]{"a", null, 2L},
            new Object[]{"abc", null, 1L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testGroupingSetsWithOrderByDimension()
  {
    msqIncompatible();
    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (), (dim2), (gran) )\n"
        + "ORDER BY gran, dim2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d1",
                                        Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    ),
                                    new OrderByColumnSpec(
                                        "d0",
                                        Direction.DESCENDING,
                                        StringComparators.LEXICOGRAPHIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", null, 1L},
            new Object[]{"a", null, 2L},
            new Object[]{"", null, 3L},
            new Object[]{NULL_STRING, null, 6L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testGroupingSetsWithOrderByAggregator()
  {
    msqIncompatible();
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (), (dim2), (gran) )\n"
        + "ORDER BY SUM(cnt)\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "a0",
                                        Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", null, 1L},
            new Object[]{"a", null, 2L},
            new Object[]{"", null, 3L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testGroupingSetsWithOrderByAggregatorWithLimit()
  {
    msqIncompatible();
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (), (dim2), (gran) )\n"
        + "ORDER BY SUM(cnt)\n"
        + "LIMIT 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "a0",
                                        Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                1
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", null, 1L}
        )
    );
  }

  @Test
  public void testTimeExtractWithTooFewArguments()
  {
    // Regression test for https://github.com/apache/druid/pull/7710.
    try {
      testQuery("SELECT TIME_EXTRACT(__time) FROM druid.foo", ImmutableList.of(), ImmutableList.of());
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(
          e,
          invalidSqlIs(
              "Invalid number of arguments to function 'TIME_EXTRACT'. Was expecting 2 arguments (line [1], column [8])"
          )
      );
    }
  }

  @Test
  public void testUsingSubqueryWithoutLimit()
  {
    testQuery(
        "SELECT COUNT(*) AS cnt FROM ( SELECT * FROM druid.foo ) tmpA",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testUnicodeFilterAndGroupBy()
  {
    testQuery(
        "SELECT\n"
        + "  dim1,\n"
        + "  dim2,\n"
        + "  COUNT(*)\n"
        + "FROM foo2\n"
        + "WHERE\n"
        + "  dim1 LIKE U&'\u05D3\\05E8%'\n" // First char is actually in the string; second is a SQL U& escape
        + "  OR dim1 = 'друид'\n"
        + "GROUP BY dim1, dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE2)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(or(
                            new LikeDimFilter("dim1", "דר%", null, null),
                            equality("dim1", "друид", ColumnType.STRING)
                        ))
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("dim2", "d1")
                        ))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"друид", "ru", 1L},
            new Object[]{"דרואיד", "he", 1L}
        )
    );
  }

  @Test
  public void testOrderByAlongWithAliasOrderByTimeGroupByMulti()
  {
    testQuery(
        "select  __time as bug, dim2  from druid.foo group by 1, 2 order by 1 limit 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                                new DefaultDimensionSpec("dim2", "d1", ColumnType.STRING)
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                Collections.singletonList(
                                    new OrderByColumnSpec("d0", Direction.ASCENDING, StringComparators.NUMERIC)
                                ),
                                1
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, "a"}
        )
    );
  }

  @Test
  public void testOrderByAlongWithAliasOrderByTimeGroupByOneCol()
  {
    testQuery(
        "select __time as bug from druid.foo group by 1 order by 1 limit 1",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(
                    new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)
                )
                .threshold(1)
                .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L}
        )
    );
  }

  @Test
  public void testProjectAfterSort()
  {
    testQuery(
        "select dim1 from (select dim1, dim2, count(*) cnt from druid.foo group by dim1, dim2 order by cnt)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0"),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"10.1"},
            new Object[]{"2"},
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testProjectAfterSort2()
  {
    testQuery(
        "select s / cnt, dim1, dim2, s from (select dim1, dim2, count(*) cnt, sum(m2) s from druid.foo group by dim1, dim2 order by cnt)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0"),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(new CountAggregatorFactory("a0"), new DoubleSumAggregatorFactory("a1", "m2"))
                        )
                        .setPostAggregatorSpecs(
                            expressionPostAgg(
                                "p0",
                                "(\"a1\" / \"a0\")",
                                ColumnType.DOUBLE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1.0, "", "a", 1.0},
            new Object[]{4.0, "1", "a", 4.0},
            new Object[]{2.0, "10.1", NullHandling.defaultStringValue(), 2.0},
            new Object[]{3.0, "2", "", 3.0},
            new Object[]{6.0, "abc", NullHandling.defaultStringValue(), 6.0},
            new Object[]{5.0, "def", "abc", 5.0}
        )
    );
  }

  @Test
  @Ignore("In Calcite 1.17, this test worked, but after upgrading to Calcite 1.21, this query fails with:"
          + " org.apache.calcite.sql.validate.SqlValidatorException: Column 'dim1' is ambiguous")
  public void testProjectAfterSort3()
  {
    msqIncompatible();
    testQuery(
        "select dim1 from (select dim1, dim1, count(*) cnt from druid.foo group by dim1, dim1 order by cnt)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                Collections.singletonList(
                                    new OrderByColumnSpec("a0", Direction.ASCENDING, StringComparators.NUMERIC)
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"10.1"},
            new Object[]{"2"},
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testProjectAfterSort3WithoutAmbiguity()
  {
    msqIncompatible();
    // This query is equivalent to the one in testProjectAfterSort3 but renames the second grouping column
    // to avoid the ambiguous name exception. The inner sort is also optimized out in Calcite 1.21.
    testQuery(
        "select copydim1 from (select dim1, dim1 AS copydim1, count(*) cnt from druid.foo group by dim1, dim1 order by cnt)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"10.1"},
            new Object[]{"2"},
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSortProjectAfterNestedGroupBy()
  {
    testQuery(
        "SELECT "
        + "  cnt "
        + "FROM ("
        + "  SELECT "
        + "    __time, "
        + "    dim1, "
        + "    COUNT(m2) AS cnt "
        + "  FROM ("
        + "    SELECT "
        + "        __time, "
        + "        m2, "
        + "        dim1 "
        + "    FROM druid.foo "
        + "    GROUP BY __time, m2, dim1 "
        + "  ) "
        + "  GROUP BY __time, dim1 "
        + "  ORDER BY cnt"
        + ")",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(
                                            dimensions(
                                                new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                                                new DefaultDimensionSpec("dim1", "d1"),
                                                new DefaultDimensionSpec("m2", "d2", ColumnType.DOUBLE)
                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("d0", "_d0", ColumnType.LONG),
                                new DefaultDimensionSpec("d1", "_d1", ColumnType.STRING)
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                useDefault
                                ? new CountAggregatorFactory("a0")
                                : new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a0"),
                                    notNull("d2")
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testPostAggWithTimeseries()
  {
    // Cannot vectorize due to descending order.
    cannotVectorize();

    testQuery(
        "SELECT "
        + "  FLOOR(__time TO YEAR), "
        + "  SUM(m1), "
        + "  SUM(m1) + SUM(m2) "
        + "FROM "
        + "  druid.foo "
        + "WHERE "
        + "  dim2 = 'a' "
        + "GROUP BY FLOOR(__time TO YEAR) "
        + "ORDER BY FLOOR(__time TO YEAR) desc",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(equality("dim2", "a", ColumnType.STRING))
                  .granularity(Granularities.YEAR)
                  .aggregators(
                      aggregators(
                          new DoubleSumAggregatorFactory("a0", "m1"),
                          new DoubleSumAggregatorFactory("a1", "m2")
                      )
                  )
                  .postAggregators(
                      expressionPostAgg("s0", "(\"a0\" + \"a1\")", ColumnType.DOUBLE)
                  )
                  .descending(true)
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{978307200000L, 4.0, 8.0},
            new Object[]{946684800000L, 1.0, 2.0}
        )
    );
  }

  @Test
  public void testPostAggWithTopN()
  {
    testQuery(
        "SELECT "
        + "  AVG(m2), "
        + "  SUM(m1) + SUM(m2) "
        + "FROM "
        + "  druid.foo "
        + "WHERE "
        + "  dim2 = 'a' "
        + "GROUP BY m1 "
        + "ORDER BY m1 "
        + "LIMIT 5",
        Collections.singletonList(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT))
                .filters(equality("dim2", "a", ColumnType.STRING))
                .aggregators(
                    useDefault
                    ? aggregators(
                        new DoubleSumAggregatorFactory("a0:sum", "m2"),
                        new CountAggregatorFactory("a0:count"),
                        new DoubleSumAggregatorFactory("a1", "m1"),
                        new DoubleSumAggregatorFactory("a2", "m2")
                    )
                    : aggregators(
                        new DoubleSumAggregatorFactory("a0:sum", "m2"),
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("a0:count"),
                            notNull("m2")
                        ),
                        new DoubleSumAggregatorFactory("a1", "m1"),
                        new DoubleSumAggregatorFactory("a2", "m2")
                    )
                )
                .postAggregators(
                    new ArithmeticPostAggregator(
                        "a0",
                        "quotient",
                        ImmutableList.of(
                            new FieldAccessPostAggregator(null, "a0:sum"),
                            new FieldAccessPostAggregator(null, "a0:count")
                        )
                    ),
                    expressionPostAgg("s0", "(\"a1\" + \"a2\")", ColumnType.DOUBLE)
                )
                .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
                .threshold(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{1.0, 2.0},
            new Object[]{4.0, 8.0}
        )
    );
  }

  @Test
  public void testConcat()
  {
    testQuery(
        "SELECT CONCAT(dim1, '-', dim1, '_', dim1) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn(
                    "v0",
                    "concat(\"dim1\",'-',\"dim1\",'_',\"dim1\")",
                    ColumnType.STRING
                ))
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"-_"},
            new Object[]{"10.1-10.1_10.1"},
            new Object[]{"2-2_2"},
            new Object[]{"1-1_1"},
            new Object[]{"def-def_def"},
            new Object[]{"abc-abc_abc"}
        )
    );
  }

  @Test
  public void testConcat2()
  {
    // Tests flattening CONCAT, and tests reduction of concat('x', 'y') => 'xy'
    testQuery(
        "SELECT CONCAt(dim1, CONCAt(dim2,concat('x', 'y')), m2, 9999, dim1) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn(
                    "v0",
                    "concat(\"dim1\",\"dim2\",'xy',\"m2\",9999,\"dim1\")",
                    ColumnType.STRING
                ))
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"axy1.09999"},
            new Object[]{NullHandling.sqlCompatible() ? null : "10.1xy2.0999910.1"}, // dim2 is null
            new Object[]{"2xy3.099992"},
            new Object[]{"1axy4.099991"},
            new Object[]{"defabcxy5.09999def"},
            new Object[]{NullHandling.sqlCompatible() ? null : "abcxy6.09999abc"} // dim2 is null
        )
    );
  }

  @Test
  public void testConcatGroup()
  {
    testQuery(
        "SELECT CONCAT(dim1, '-', dim1, '_', dim1) as dimX FROM foo GROUP BY 1",
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setVirtualColumns(expressionVirtualColumn(
                    "v0",
                    "concat(\"dim1\",'-',\"dim1\",'_',\"dim1\")",
                    ColumnType.STRING
                ))
                .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0")))
                .setGranularity(Granularities.ALL)
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"-_"},
            new Object[]{"1-1_1"},
            new Object[]{"10.1-10.1_10.1"},
            new Object[]{"2-2_2"},
            new Object[]{"abc-abc_abc"},
            new Object[]{"def-def_def"}
        )
    );
  }

  @Test
  public void testConcatGroup2()
  {
    final List<Object[]> results;
    if (useDefault) {
      results = ImmutableList.of(
          new Object[]{"10.1x2.0999910.1"},
          new Object[]{"1ax4.099991"},
          new Object[]{"2x3.099992"},
          new Object[]{"abcx6.09999abc"},
          new Object[]{"ax1.09999"},
          new Object[]{"defabcx5.09999def"}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{null},
          new Object[]{"1ax4.099991"},
          new Object[]{"2x3.099992"},
          new Object[]{"ax1.09999"},
          new Object[]{"defabcx5.09999def"}
      );
    }

    testQuery(
        "SELECT CONCAT(dim1, CONCAT(dim2,'x'), m2, 9999, dim1) as dimX FROM foo GROUP BY 1",
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setVirtualColumns(expressionVirtualColumn(
                    "v0",
                    "concat(\"dim1\",\"dim2\",'x',\"m2\",9999,\"dim1\")",
                    ColumnType.STRING
                ))
                .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0")))
                .setGranularity(Granularities.ALL)
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()

        ),
        results
    );
  }

  @Test
  public void testConcatDecomposeAlwaysFalseOrUnknown()
  {
    testQuery(
        "SELECT CONCAT(dim1, 'x', dim2) as dimX\n"
        + "FROM foo\n"
        + "WHERE CONCAT(dim1, 'x', dim2) IN ('1a', '3x4')",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "'3x4'", ColumnType.STRING))
                .filters(and(
                    equality("dim1", "3", ColumnType.STRING),
                    equality("dim2", "4", ColumnType.STRING)
                ))
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testConcatDecomposeAlwaysFalseOrUnknownNegated()
  {
    testQuery(
        "SELECT CONCAT(dim1, 'x', dim2) as dimX\n"
        + "FROM foo\n"
        + "WHERE CONCAT(dim1, 'x', dim2) NOT IN ('1a', '3x4', '4x5')\n",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn(
                    "v0",
                    "concat(\"dim1\",'x',\"dim2\")",
                    ColumnType.STRING
                ))
                .filters(
                    NullHandling.sqlCompatible()
                    ? and(
                        or(
                            not(equality("dim1", "3", ColumnType.STRING)),
                            not(equality("dim2", "4", ColumnType.STRING))
                        ),
                        or(
                            not(equality("dim1", "4", ColumnType.STRING)),
                            not(equality("dim2", "5", ColumnType.STRING))
                        ),
                        notNull("dim1"),
                        notNull("dim2")
                    )
                    : and(
                        or(
                            not(equality("dim1", "3", ColumnType.STRING)),
                            not(equality("dim2", "4", ColumnType.STRING))
                        ),
                        or(
                            not(equality("dim1", "4", ColumnType.STRING)),
                            not(equality("dim2", "5", ColumnType.STRING))
                        )
                    )
                )
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{"xa"},
            new Object[]{"2x"},
            new Object[]{"1xa"},
            new Object[]{"defxabc"}
        )
        : ImmutableList.of(
            new Object[]{"xa"},
            new Object[]{"10.1x"},
            new Object[]{"2x"},
            new Object[]{"1xa"},
            new Object[]{"defxabc"},
            new Object[]{"abcx"}
        )
    );
  }

  @Test
  public void testConcatDecomposeIsNull()
  {
    testQuery(
        "SELECT dim1, dim2, CONCAT(dim1, 'x', dim2) as dimX\n"
        + "FROM foo\n"
        + "WHERE CONCAT(dim1, 'x', dim2) IS NULL",
        ImmutableList.of(
            NullHandling.sqlCompatible()
            ? newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn(
                    "v0",
                    "concat(\"dim1\",'x',\"dim2\")",
                    ColumnType.STRING
                ))
                .filters(or(isNull("dim1"), isNull("dim2")))
                .columns("dim1", "dim2", "v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
            : Druids.newScanQueryBuilder()
                    .dataSource(
                        InlineDataSource.fromIterable(
                            ImmutableList.of(),
                            RowSignature.builder()
                                        .add("dim1", ColumnType.STRING)
                                        .add("dim2", ColumnType.STRING)
                                        .add("dimX", ColumnType.STRING)
                                        .build()
                        )
                    )
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .columns("dim1", "dim2", "dimX")
                    .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                    .legacy(false)
                    .build()

        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{"10.1", null, null},
            new Object[]{"abc", null, null}
        )
        : ImmutableList.of()
    );
  }

  @Test
  public void testConcatDoubleBarsDecompose()
  {
    testQuery(
        "SELECT dim1 || LOWER('x') || dim2 || 'z' as dimX\n"
        + "FROM foo\n"
        + "WHERE dim1 || LOWER('x') || dim2 || 'z' IN ('1xaz', '3x4z')",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat(\"dim1\",'x',\"dim2\",'z')", ColumnType.STRING))
                .filters(or(
                    and(equality("dim1", "1", ColumnType.STRING), equality("dim2", "a", ColumnType.STRING)),
                    and(equality("dim1", "3", ColumnType.STRING), equality("dim2", "4", ColumnType.STRING))
                ))
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"1xaz"}
        )
    );
  }

  @Test
  public void testTextcat()
  {
    testQuery(
        "SELECT textcat(dim1, dim1) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat(\"dim1\",\"dim1\")", ColumnType.STRING))
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"10.110.1"},
            new Object[]{"22"},
            new Object[]{"11"},
            new Object[]{"defdef"},
            new Object[]{"abcabc"}
        )
    );

    testQuery(
        "SELECT textcat(dim1, CAST(m2 as VARCHAR)) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn(
                    "v0",
                    "concat(\"dim1\",CAST(\"m2\", 'STRING'))",
                    ColumnType.STRING
                ))
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"1.0"},
            new Object[]{"10.12.0"},
            new Object[]{"23.0"},
            new Object[]{"14.0"},
            new Object[]{"def5.0"},
            new Object[]{"abc6.0"}
        )
    );
  }

  @NotYetSupported(Modes.MISSING_JOIN_CONVERSION)
  @Test
  public void testRequireTimeConditionPositive()
  {
    // simple timeseries
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT __time as t, floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "WHERE t >= '2000-01-01' and t < '2002-01-01'"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2002-01-01")))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, timestamp("2000-01-01")},
            new Object[]{3L, timestamp("2001-01-01")}
        )
    );

    // nested GROUP BY only requires time condition for inner most query
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo WHERE __time >= '2000-01-01' GROUP BY dim2)",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Intervals.utc(
                                                DateTimes.of("2000-01-01").getMillis(),
                                                JodaUtils.MAX_INSTANT
                                            )))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("_a0", "a0"),
                            new CountAggregatorFactory("_a1")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{6L, 3L}
        ) :
        ImmutableList.of(
            new Object[]{6L, 4L}
        )
    );

    // Cannot vectorize next test due to extraction dimension spec.
    cannotVectorize();

    // semi-join requires time condition on both left and right query
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= '2000-01-01' AND SUBSTRING(dim2, 1, 1) IN (\n"
        + "  SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo\n"
        + "  WHERE dim1 <> '' AND __time >= '2000-01-01'\n"
        + ")",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE1),
                          new QueryDataSource(
                              GroupByQuery.builder()
                                          .setDataSource(CalciteTests.DATASOURCE1)
                                          .setInterval(
                                              querySegmentSpec(
                                                  Intervals.utc(
                                                      DateTimes.of("2000-01-01").getMillis(),
                                                      JodaUtils.MAX_INSTANT
                                                  )
                                              )
                                          )
                                          .setDimFilter(not(equality("dim1", "", ColumnType.STRING)))
                                          .setGranularity(Granularities.ALL)
                                          .setDimensions(
                                              new ExtractionDimensionSpec(
                                                  "dim1",
                                                  "d0",
                                                  ColumnType.STRING,
                                                  new SubstringDimExtractionFn(0, 1)
                                              )
                                          )
                                          .setContext(QUERY_CONTEXT_DEFAULT)
                                          .build()
                          ),
                          "j0.",
                          equalsCondition(
                              makeExpression("substring(\"dim2\", 0, 1)"),
                              DruidExpression.ofColumn(ColumnType.STRING, "j0.d0")
                          ),
                          JoinType.INNER
                      )
                  )
                  .intervals(
                      querySegmentSpec(
                          Intervals.utc(
                              DateTimes.of("2000-01-01").getMillis(),
                              JodaUtils.MAX_INSTANT
                          )
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testRequireTimeConditionLogicalValuePositive()
  {
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT 2 + 2 AS a",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(InlineDataSource.fromIterable(
                    ImmutableList.of(new Object[]{4L}),
                    RowSignature.builder().add("a", ColumnType.LONG).build()
                ))
                .columns("a")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(new Object[]{4})
    );
  }

  @Test
  public void testRequireTimeConditionSimpleQueryNegative()
  {
    msqIncompatible();
    expectedException.expect(CannotBuildQueryException.class);
    expectedException.expectMessage("__time column");

    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT __time as t, floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testRequireTimeConditionSubQueryNegative()
  {
    msqIncompatible();
    expectedException.expect(CannotBuildQueryException.class);
    expectedException.expectMessage("__time column");

    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @NotYetSupported(Modes.ERROR_HANDLING)
  @Test
  public void testRequireTimeConditionSemiJoinNegative()
  {
    msqIncompatible();
    expectedException.expect(CannotBuildQueryException.class);
    expectedException.expectMessage("__time column");

    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE SUBSTRING(dim2, 1, 1) IN (\n"
        + "  SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo\n"
        + "  WHERE dim1 <> '' AND __time >= '2000-01-01'\n"
        + ")",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterFloatDimension()
  {
    testQuery(
        "SELECT dim1 FROM numfoo WHERE f1 = 0.1 LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1")
                .filters(equality("f1", 0.1, ColumnType.DOUBLE))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  public void testFilterDoubleDimension()
  {
    testQuery(
        "SELECT dim1 FROM numfoo WHERE d1 = 1.7 LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1")
                .filters(equality("d1", 1.7, ColumnType.DOUBLE))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  public void testFilterLongDimension()
  {
    testQuery(
        "SELECT dim1 FROM numfoo WHERE l1 = 7 LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1")
                .filters(equality("l1", 7L, ColumnType.LONG))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""}
        )
    );
  }

  @Test
  public void testTrigonometricFunction()
  {
    msqIncompatible();
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        "SELECT exp(count(*)) + 10, sin(pi / 6), cos(pi / 6), tan(pi / 6), cot(pi / 6)," +
        "asin(exp(count(*)) / 2), acos(exp(count(*)) / 2), atan(exp(count(*)) / 2), atan2(exp(count(*)), 1) " +
        "FROM druid.foo WHERE  dim2 = 0",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .filters(numericEquality("dim2", 0L, ColumnType.LONG))
                               .granularity(Granularities.ALL)
                               .aggregators(aggregators(
                                   new CountAggregatorFactory("a0")
                               ))
                               // after upgrading to Calcite 1.21, expressions like sin(pi/6) that only reference
                               // literals are optimized into literals
                               .postAggregators(
                                   expressionPostAgg("p0", "(exp(\"a0\") + 10)", ColumnType.DOUBLE),
                                   expressionPostAgg("p1", "0.49999999999999994", ColumnType.DOUBLE),
                                   expressionPostAgg("p2", "0.8660254037844387", ColumnType.DOUBLE),
                                   expressionPostAgg("p3", "0.5773502691896257", ColumnType.DOUBLE),
                                   expressionPostAgg("p4", "1.7320508075688776", ColumnType.DOUBLE),
                                   expressionPostAgg("p5", "asin((exp(\"a0\") / 2))", ColumnType.DOUBLE),
                                   expressionPostAgg("p6", "acos((exp(\"a0\") / 2))", ColumnType.DOUBLE),
                                   expressionPostAgg("p7", "atan((exp(\"a0\") / 2))", ColumnType.DOUBLE),
                                   expressionPostAgg("p8", "atan2(exp(\"a0\"),1)", ColumnType.DOUBLE)
                               )
                               .context(QUERY_CONTEXT_DEFAULT)
                               .build()),
        ImmutableList.of(
            new Object[]{
                11.0,
                Math.sin(Math.PI / 6),
                Math.cos(Math.PI / 6),
                Math.tan(Math.PI / 6),
                Math.cos(Math.PI / 6) / Math.sin(Math.PI / 6),
                Math.asin(0.5),
                Math.acos(0.5),
                Math.atan(0.5),
                Math.atan2(1, 1)
            }
        )
    );
  }

  @Test
  public void testRadiansAndDegrees()
  {
    testQuery(
        "SELECT RADIANS(m1 * 15)/DEGREES(m2) FROM numfoo WHERE dim1 = '1'",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "(toRadians((\"m1\" * 15)) / toDegrees(\"m2\"))", ColumnType.DOUBLE)
                )
                .columns("v0")
                .filters(equality("dim1", "1", ColumnType.STRING))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{Math.toRadians(60) / Math.toDegrees(4)}
        )
    );
  }

  @Test
  public void testTimestampDiff()
  {
    testQuery(
        "SELECT TIMESTAMPDIFF(DAY, TIMESTAMP '1999-01-01 00:00:00', __time), \n"
        + "TIMESTAMPDIFF(DAY, __time, DATE '2001-01-01'), \n"
        + "TIMESTAMPDIFF(HOUR, TIMESTAMP '1999-12-31 01:00:00', __time), \n"
        + "TIMESTAMPDIFF(MINUTE, TIMESTAMP '1999-12-31 23:58:03', __time), \n"
        + "TIMESTAMPDIFF(SECOND, TIMESTAMP '1999-12-31 23:59:03', __time), \n"
        + "TIMESTAMPDIFF(MONTH, TIMESTAMP '1999-11-01 00:00:00', __time), \n"
        + "TIMESTAMPDIFF(YEAR, TIMESTAMP '1996-11-01 00:00:00', __time), \n"
        + "TIMESTAMPDIFF(QUARTER, TIMESTAMP '1996-10-01 00:00:00', __time), \n"
        + "TIMESTAMPDIFF(WEEK, TIMESTAMP '1998-10-01 00:00:00', __time) \n"
        + "FROM druid.foo\n"
        + "LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "div((\"__time\" - 915148800000),86400000)", ColumnType.LONG),
                    expressionVirtualColumn("v1", "div((978307200000 - \"__time\"),86400000)", ColumnType.LONG),
                    expressionVirtualColumn("v2", "div((\"__time\" - 946602000000),3600000)", ColumnType.LONG),
                    expressionVirtualColumn("v3", "div((\"__time\" - 946684683000),60000)", ColumnType.LONG),
                    expressionVirtualColumn("v4", "div((\"__time\" - 946684743000),1000)", ColumnType.LONG),
                    expressionVirtualColumn("v5", "subtract_months(\"__time\",941414400000,'UTC')", ColumnType.LONG),
                    expressionVirtualColumn(
                        "v6",
                        "div(subtract_months(\"__time\",846806400000,'UTC'),12)",
                        ColumnType.LONG
                    ),
                    expressionVirtualColumn(
                        "v7",
                        "div(subtract_months(\"__time\",844128000000,'UTC'),3)",
                        ColumnType.LONG
                    ),
                    expressionVirtualColumn("v8", "div(div((\"__time\" - 907200000000),1000),604800)", ColumnType.LONG)
                )
                .columns("v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8")
                .limit(2)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()

        ),
        ImmutableList.of(
            new Object[]{365, 366, 23, 1, 57, 2, 3, 13, 65},
            new Object[]{366, 365, 47, 1441, 86457, 2, 3, 13, 65}
        )
    );
  }

  @Test
  public void testTimestampCeil()
  {
    testQuery(
        "SELECT CEIL(TIMESTAMP '2000-01-01 00:00:00' TO DAY), \n"
        + "CEIL(TIMESTAMP '2000-01-01 01:00:00' TO DAY) \n"
        + "FROM druid.foo\n"
        + "LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "946684800000", ColumnType.LONG),
                    expressionVirtualColumn("v1", "946771200000", ColumnType.LONG)
                )
                .columns("v0", "v1")
                .limit(1)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()

        ),
        ImmutableList.of(
            new Object[]{
                Calcites.jodaToCalciteTimestamp(
                    DateTimes.of("2000-01-01"),
                    DateTimeZone.UTC
                ),
                Calcites.jodaToCalciteTimestamp(
                    DateTimes.of("2000-01-02"),
                    DateTimeZone.UTC
                )
            }
        )
    );
  }

  @Test
  public void testNvlColumns()
  {
    testQuery(
        "SELECT NVL(dim2, dim1), COUNT(*) FROM druid.foo GROUP BY NVL(dim2, dim1)\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",\"dim1\")",
                                ColumnType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"a", 2L},
            new Object[]{"abc", 2L}
        ) :
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"a", 2L},
            new Object[]{"abc", 2L}
        )
    );
  }

  @DecoupledTestConfig(nativeQueryIgnore = NativeQueryIgnore.IMPROVED_PLAN)
  @Test
  public void testGroupByWithLiteralInSubqueryGrouping()
  {
    testQuery(
        "SELECT \n"
        + "   t1, t2\n"
        + "  FROM\n"
        + "   ( SELECT\n"
        + "     'dummy' as t1,\n"
        + "     CASE\n"
        + "       WHEN \n"
        + "         dim4 = 'b'\n"
        + "       THEN dim4\n"
        + "       ELSE NULL\n"
        + "     END AS t2\n"
        + "     FROM\n"
        + "       numfoo\n"
        + "     GROUP BY\n"
        + "       dim4\n"
        + "   )\n"
        + " GROUP BY\n"
        + "   t1,t2\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE3)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(new DefaultDimensionSpec("dim4", "_d0", ColumnType.STRING))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "\'dummy\'",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "case_searched((\"_d0\" == 'b'),\"_d0\",null)",
                                ColumnType.STRING
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.STRING),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.STRING)
                            )
                        )
                        .setGranularity(Granularities.ALL)
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"dummy", NULL_STRING},
            new Object[]{"dummy", "b"}
        )
    );
  }

  @Test
  public void testLeftRightStringOperators()
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  LEFT(dim1, 2),\n"
        + "  RIGHT(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setPostAggregatorSpecs(ImmutableList.of(
                            expressionPostAgg("p0", "left(\"d0\",2)", ColumnType.STRING),
                            expressionPostAgg("p1", "right(\"d0\",2)", ColumnType.STRING)
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", "", ""},
            new Object[]{"1", "1", "1"},
            new Object[]{"10.1", "10", ".1"},
            new Object[]{"2", "2", "2"},
            new Object[]{"abc", "ab", "bc"},
            new Object[]{"def", "de", "ef"}
        )
    );
  }

  @Test
  public void testQueryContextOuterLimit()
  {
    msqIncompatible();
    Map<String, Object> outerLimitContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    outerLimitContext.put(PlannerContext.CTX_SQL_OUTER_LIMIT, 4);

    TopNQueryBuilder baseBuilder = new TopNQueryBuilder()
        .dataSource(CalciteTests.DATASOURCE1)
        .intervals(querySegmentSpec(Filtration.eternity()))
        .granularity(Granularities.ALL)
        .dimension(new DefaultDimensionSpec("dim1", "d0"))
        .metric(
            new InvertedTopNMetricSpec(
                new DimensionTopNMetricSpec(
                    null,
                    StringComparators.LEXICOGRAPHIC
                )
            )
        )
        .context(outerLimitContext);

    List<Object[]> results1;
    if (NullHandling.replaceWithDefault()) {
      results1 = ImmutableList.of(
          new Object[]{""},
          new Object[]{"def"},
          new Object[]{"abc"},
          new Object[]{"2"}
      );
    } else {
      results1 = ImmutableList.of(
          new Object[]{"def"},
          new Object[]{"abc"},
          new Object[]{"2"},
          new Object[]{"10.1"}
      );
    }

    // no existing limit
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        outerLimitContext,
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            baseBuilder.threshold(4).build()
        ),
        results1
    );

    // existing limit greater than context limit, override existing limit
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        outerLimitContext,
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC LIMIT 9",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            baseBuilder.threshold(4).build()
        ),
        results1
    );


    List<Object[]> results2;
    if (NullHandling.replaceWithDefault()) {
      results2 = ImmutableList.of(
          new Object[]{""},
          new Object[]{"def"}
      );
    } else {
      results2 = ImmutableList.of(
          new Object[]{"def"},
          new Object[]{"abc"}
      );
    }

    // existing limit less than context limit, keep existing limit
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        outerLimitContext,
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC LIMIT 2",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            baseBuilder.threshold(2).build()

        ),
        results2
    );
  }

  @DecoupledTestConfig(nativeQueryIgnore = NativeQueryIgnore.IMPROVED_PLAN)
  @Test
  public void testRepeatedIdenticalVirtualExpressionGrouping()
  {
    final String query = "SELECT \n"
                         + "\tCASE dim1 WHEN NULL THEN FALSE ELSE TRUE END AS col_a,\n"
                         + "\tCASE dim2 WHEN NULL THEN FALSE ELSE TRUE END AS col_b\n"
                         + "FROM foo\n"
                         + "GROUP BY 1, 2";

    testQuery(
        query,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "1", ColumnType.LONG))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.LONG),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{true, true}
        )
    );
  }

  @Test
  public void testValidationErrorNullLiteralIllegal()
  {
    expectedException.expectMessage("Illegal use of 'NULL'");

    testQuery(
        "SELECT REGEXP_LIKE('x', NULL)",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testValidationErrorNonLiteralIllegal()
  {
    expectedException.expectMessage("Argument to function 'REGEXP_LIKE' must be a literal");

    testQuery(
        "SELECT REGEXP_LIKE('x', dim1) FROM foo",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testValidationErrorWrongTypeLiteral()
  {
    expectedException.expectMessage("Cannot apply 'REGEXP_LIKE' to arguments");

    testQuery(
        "SELECT REGEXP_LIKE('x', 1) FROM foo",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testTimeStampAddZeroDayPeriod()
  {
    testQuery(
        "SELECT TIMESTAMPADD(DAY, 0, \"__time\") FROM druid.foo",

        // verify if SQL matches given native query
        ImmutableList.of(newScanQueryBuilder()
                             .dataSource(CalciteTests.DATASOURCE1)
                             .intervals(querySegmentSpec(Filtration.eternity()))
                             .virtualColumns(
                                 expressionVirtualColumn("v0", "(\"__time\" + 0)", ColumnType.LONG)
                             )
                             .columns("v0")
                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                             .context(QUERY_CONTEXT_DEFAULT)
                             .build()),

        //Since adding a zero period does not change the timestamp, just compare the stamp with the orignal
        TestDataBuilder.ROWS1.stream()
                             .map(row -> new Object[]{row.getTimestampFromEpoch()})
                             .collect(Collectors.toList())
    );
  }

  @Test
  public void testTimeStampAddZeroMonthPeriod()
  {
    testQuery(
        "SELECT TIMESTAMPADD(MONTH, 0, \"__time\") FROM druid.foo",

        // verify if SQL matches given native query
        ImmutableList.of(newScanQueryBuilder()
                             .dataSource(CalciteTests.DATASOURCE1)
                             .intervals(querySegmentSpec(Filtration.eternity()))
                             .virtualColumns(
                                 expressionVirtualColumn(
                                     "v0",
                                     "timestamp_shift(\"__time\",'P0M',1,'UTC')",
                                     ColumnType.LONG
                                 )
                             )
                             .columns("v0")
                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                             .context(QUERY_CONTEXT_DEFAULT)
                             .build()),

        //Since adding a zero period does not change the timestamp, just compare the stamp with the orignal
        TestDataBuilder.ROWS1.stream()
                             .map(row -> new Object[]{row.getTimestampFromEpoch()})
                             .collect(Collectors.toList())
    );
  }

  @Test
  public void testTimeStampAddZeroYearPeriod()
  {
    skipVectorize();

    testQuery(
        "SELECT TIMESTAMPADD(YEAR, 0, \"__time\") FROM druid.foo",

        // verify if SQL matches given native query
        ImmutableList.of(newScanQueryBuilder()
                             .dataSource(CalciteTests.DATASOURCE1)
                             .intervals(querySegmentSpec(Filtration.eternity()))
                             .virtualColumns(
                                 expressionVirtualColumn(
                                     "v0",
                                     "timestamp_shift(\"__time\",'P0M',1,'UTC')",
                                     ColumnType.LONG
                                 )
                             )
                             .columns("v0")
                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                             .context(QUERY_CONTEXT_DEFAULT)
                             .build()),

        //Since adding a zero period does not change the timestamp, just compare the stamp with the orignal
        TestDataBuilder.ROWS1.stream()
                             .map(row -> new Object[]{row.getTimestampFromEpoch()})
                             .collect(Collectors.toList())
    );
  }

  /**
   * TIMESTAMPADD is converted to timestamp_shift function call and its parameters will be converted to a Period string or an expression
   * see https://github.com/apache/druid/issues/10530 for more information
   */
  @Test
  public void testTimeStampAddConversion()
  {
    final PeriodGranularity periodGranularity = new PeriodGranularity(new Period("P1M"), null, null);

    //
    // 2nd parameter for TIMESTAMPADD is literal, it will be translated to 'P1M' string
    //
    testQuery(
        "SELECT TIMESTAMPADD(MONTH, 1, \"__time\") FROM druid.foo",

        // verify if SQL matches given native query
        ImmutableList.of(newScanQueryBuilder()
                             .dataSource(CalciteTests.DATASOURCE1)
                             .intervals(querySegmentSpec(Filtration.eternity()))
                             .virtualColumns(
                                 expressionVirtualColumn(
                                     "v0",
                                     "timestamp_shift(\"__time\",'P1M',1,'UTC')",
                                     ColumnType.LONG
                                 )
                             )
                             .columns("v0")
                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                             .context(QUERY_CONTEXT_DEFAULT)
                             .build()),

        // verify if query results match the given
        TestDataBuilder.ROWS1.stream()
                             .map(r -> new Object[]{periodGranularity.increment(r.getTimestamp()).getMillis()})
                             .collect(Collectors.toList())
    );

    //
    // 2nd parameter for TIMESTAMPADD is an expression, it will be explained as a function call: concat('P', (1 * \"cnt\"), 'M')
    //
    testQuery(
        "SELECT TIMESTAMPADD(MONTH, \"cnt\", \"__time\") FROM druid.foo",

        // verify if SQL matches given native query
        ImmutableList.of(newScanQueryBuilder()
                             .dataSource(CalciteTests.DATASOURCE1)
                             .intervals(querySegmentSpec(Filtration.eternity()))
                             .virtualColumns(
                                 expressionVirtualColumn(
                                     "v0",
                                     "timestamp_shift(\"__time\",concat('P', (1 * \"cnt\"), 'M'),1,'UTC')",
                                     ColumnType.LONG
                                 )
                             )
                             .columns("v0")
                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                             .context(QUERY_CONTEXT_DEFAULT)
                             .build()),

        // verify if query results match the given
        // "cnt" for each row is 1
        TestDataBuilder.ROWS1.stream()
                             .map(row -> new Object[]{periodGranularity.increment(row.getTimestamp()).getMillis()})
                             .collect(Collectors.toList())
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testGroupingSetsWithLimit()
  {
    msqIncompatible();
    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (dim2, gran), (dim2), (gran), () ) LIMIT 100",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d1"),
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(),
                                100
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{"", null, 3L},
            new Object[]{"a", null, 2L},
            new Object[]{"abc", null, 1L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @SqlTestFrameworkConfig(numMergeBuffers = 3)
  @Test
  public void testGroupingSetsWithLimitOrderByGran()
  {
    msqIncompatible();
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (dim2, gran), (dim2), (gran), () ) ORDER BY x.gran LIMIT 100",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "nvl(\"dim2\",'')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d1"),
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d1",
                                        Direction.ASCENDING,
                                        new StringComparators.NumericComparator()
                                    )
                                ),
                                100
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        ImmutableList.<Object[]>builder().add(
            new Object[]{"", null, 2L},
            new Object[]{"a", null, 1L},
            new Object[]{"", null, 1L},
            new Object[]{"a", null, 1L},
            new Object[]{"abc", null, 1L},
            new Object[]{NULL_STRING, null, 6L},
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L}
        ).build()
    );
  }

  @Test
  public void testRoundFunc()
  {
    testQuery(
        "SELECT f1, round(f1) FROM druid.numfoo",
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "round(\"f1\")", ColumnType.FLOAT)
                )
                .columns("f1", "v0")
                .legacy(false)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{1.0f, 1.0f},
            new Object[]{0.1f, 0.0f},
            new Object[]{0.0f, 0.0f},
            new Object[]{null, null},
            new Object[]{null, null},
            new Object[]{null, null}
        )
        : ImmutableList.of(
            new Object[]{1.0f, 1.0f},
            new Object[]{0.1f, 0.0f},
            new Object[]{0.0f, 0.0f},
            new Object[]{0.0f, 0.0f},
            new Object[]{0.0f, 0.0f},
            new Object[]{0.0f, 0.0f}
        )
    );
  }

  @Test
  public void testCountAndAverageByConstantVirtualColumn()
  {
    List<VirtualColumn> virtualColumns;
    List<AggregatorFactory> aggs;
    if (useDefault) {
      aggs = ImmutableList.of(
          new FilteredAggregatorFactory(
              new CountAggregatorFactory("a0"),
              notNull("v0")
          ),
          new DoubleSumAggregatorFactory("a1:sum", "v1", null, TestExprMacroTable.INSTANCE),
          new CountAggregatorFactory("a1:count")
      );
      virtualColumns = ImmutableList.of(
          expressionVirtualColumn("v0", "'10.1'", ColumnType.STRING),
          expressionVirtualColumn("v1", "325323", ColumnType.LONG)
      );
    } else {
      aggs = ImmutableList.of(
          new FilteredAggregatorFactory(
              new CountAggregatorFactory("a0"),
              notNull("v0")
          ),
          new DoubleSumAggregatorFactory("a1:sum", "v1"),
          new FilteredAggregatorFactory(
              new CountAggregatorFactory("a1:count"),
              notNull("v1")
          )
      );
      virtualColumns = ImmutableList.of(
          expressionVirtualColumn("v0", "'10.1'", ColumnType.STRING),
          expressionVirtualColumn("v1", "325323", ColumnType.LONG)
      );

    }
    testQuery(
        "SELECT dim5, COUNT(dim1), AVG(l1) FROM druid.numfoo WHERE dim1 = '10.1' AND l1 = 325323 GROUP BY dim5",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(
                            and(
                                equality("dim1", "10.1", ColumnType.STRING),
                                equality("l1", 325323L, ColumnType.LONG)
                            )
                        )
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(VirtualColumns.create(virtualColumns))
                        .setDimensions(new DefaultDimensionSpec("dim5", "_d0", ColumnType.STRING))
                        .setAggregatorSpecs(aggs)
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new ArithmeticPostAggregator(
                                    "a1",
                                    "quotient",
                                    ImmutableList.of(
                                        new FieldAccessPostAggregator(null, "a1:sum"),
                                        new FieldAccessPostAggregator(null, "a1:count")
                                    )
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"ab", 1L, 325323.0}
        )
    );
  }

  @Test
  public void testExpressionCounts()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + " COUNT(reverse(dim2)),\n"
        + " COUNT(left(dim2, 5)),\n"
        + " COUNT(strpos(dim2, 'a'))\n"
        + "FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "reverse(\"dim2\")", ColumnType.STRING),
                      expressionVirtualColumn("v1", "left(\"dim2\",5)", ColumnType.STRING),
                      expressionVirtualColumn("v2", "(strpos(\"dim2\",'a') + 1)", ColumnType.LONG)
                  )
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a0"),
                              notNull("v0")
                          ),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a1"),
                              notNull("v1")
                          ),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a2"),
                              notNull("v2")
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            // in default mode strpos is 6 because the '+ 1' of the expression (no null numbers in
            // default mode so is 0 + 1 for null rows)
            ? new Object[]{3L, 3L, 6L}
            : new Object[]{4L, 4L, 4L}
        )
    );
  }

  @Test
  public void testBitwiseAggregatorsTimeseries()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + " BIT_AND(l1),\n"
        + " BIT_OR(l1),\n"
        + " BIT_XOR(l1)\n"
        + "FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a0",
                                  ImmutableSet.of("l1"),
                                  "__acc",
                                  "0",
                                  "0",
                                  NullHandling.sqlCompatible(),
                                  false,
                                  false,
                                  "bitwiseAnd(\"__acc\", \"l1\")",
                                  "bitwiseAnd(\"__acc\", \"a0\")",
                                  null,
                                  null,
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("l1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a1",
                                  ImmutableSet.of("l1"),
                                  "__acc",
                                  "0",
                                  "0",
                                  NullHandling.sqlCompatible(),
                                  false,
                                  false,
                                  "bitwiseOr(\"__acc\", \"l1\")",
                                  "bitwiseOr(\"__acc\", \"a1\")",
                                  null,
                                  null,
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("l1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a2",
                                  ImmutableSet.of("l1"),
                                  "__acc",
                                  "0",
                                  "0",
                                  NullHandling.sqlCompatible(),
                                  false,
                                  false,
                                  "bitwiseXor(\"__acc\", \"l1\")",
                                  "bitwiseXor(\"__acc\", \"a2\")",
                                  null,
                                  null,
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("l1")
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{0L, 325327L, 325324L}
            : new Object[]{0L, 325327L, 325324L}
        )
    );
  }

  @Test
  public void testBitwiseAggregatorsGroupBy()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + " dim2,\n"
        + " BIT_AND(l1),\n"
        + " BIT_OR(l1),\n"
        + " BIT_XOR(l1)\n"
        + "FROM druid.numfoo GROUP BY 1 ORDER BY 4",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("dim2", "_d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            aggregators(
                                new FilteredAggregatorFactory(
                                    new ExpressionLambdaAggregatorFactory(
                                        "a0",
                                        ImmutableSet.of("l1"),
                                        "__acc",
                                        "0",
                                        "0",
                                        NullHandling.sqlCompatible(),
                                        false,
                                        false,
                                        "bitwiseAnd(\"__acc\", \"l1\")",
                                        "bitwiseAnd(\"__acc\", \"a0\")",
                                        null,
                                        null,
                                        ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    notNull("l1")
                                ),
                                new FilteredAggregatorFactory(
                                    new ExpressionLambdaAggregatorFactory(
                                        "a1",
                                        ImmutableSet.of("l1"),
                                        "__acc",
                                        "0",
                                        "0",
                                        NullHandling.sqlCompatible(),
                                        false,
                                        false,
                                        "bitwiseOr(\"__acc\", \"l1\")",
                                        "bitwiseOr(\"__acc\", \"a1\")",
                                        null,
                                        null,
                                        ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    notNull("l1")
                                ),
                                new FilteredAggregatorFactory(
                                    new ExpressionLambdaAggregatorFactory(
                                        "a2",
                                        ImmutableSet.of("l1"),
                                        "__acc",
                                        "0",
                                        "0",
                                        NullHandling.sqlCompatible(),
                                        false,
                                        false,
                                        "bitwiseXor(\"__acc\", \"l1\")",
                                        "bitwiseXor(\"__acc\", \"a2\")",
                                        null,
                                        null,
                                        ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    notNull("l1")
                                )
                            )
                        )
                        .setLimitSpec(
                            DefaultLimitSpec.builder()
                                            .orderBy(
                                                new OrderByColumnSpec(
                                                    "a2",
                                                    Direction.ASCENDING,
                                                    StringComparators.NUMERIC
                                                )
                                            )
                                            .build()
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        useDefault
        ? ImmutableList.of(
            new Object[]{"abc", 0L, 0L, 0L},
            new Object[]{"a", 0L, 7L, 7L},
            new Object[]{"", 0L, 325323L, 325323L}
        )
        : ImmutableList.of(
            new Object[]{"abc", null, null, null},
            new Object[]{"", 0L, 0L, 0L},
            new Object[]{"a", 0L, 7L, 7L},
            new Object[]{null, 0L, 325323L, 325323L}
        )
    );
  }

  @Test
  public void testStringAgg()
  {
    cannotVectorize();
    testQuery(
        "SELECT STRING_AGG(dim1,','), STRING_AGG(DISTINCT dim1, ','), STRING_AGG(DISTINCT dim1,',') FILTER(WHERE dim1 = 'shazbot') FROM foo WHERE dim1 is not null",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(notNull("dim1"))
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a0",
                                  ImmutableSet.of("dim1"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_append(\"__acc\", \"dim1\")",
                                  "array_concat(\"__acc\", \"a0\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("dim1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a1",
                                  ImmutableSet.of("dim1"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_set_add(\"__acc\", \"dim1\")",
                                  "array_set_add_all(\"__acc\", \"a1\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("dim1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a2",
                                  ImmutableSet.of("dim1"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_set_add(\"__acc\", \"dim1\")",
                                  "array_set_add_all(\"__acc\", \"a2\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              and(
                                  notNull("dim1"),
                                  equality("dim1", "shazbot", ColumnType.STRING)
                              )
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{"10.1,2,1,def,abc", "1,10.1,2,abc,def", ""}
            : new Object[]{",10.1,2,1,def,abc", ",1,10.1,2,abc,def", null}
        )
    );
  }

  @Test
  public void testStringAggMultiValue()
  {
    cannotVectorize();
    testQuery(
        "SELECT STRING_AGG(dim3, ','), STRING_AGG(DISTINCT dim3, ',') FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a0",
                                  ImmutableSet.of("dim3"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_append(\"__acc\", \"dim3\")",
                                  "array_concat(\"__acc\", \"a0\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("dim3")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a1",
                                  ImmutableSet.of("dim3"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_set_add(\"__acc\", \"dim3\")",
                                  "array_set_add_all(\"__acc\", \"a1\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("dim3")
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{"a,b,b,c,d", "a,b,c,d"}
            : new Object[]{"a,b,b,c,d,", ",a,b,c,d"}
        )
    );
  }

  @Test
  public void testStringAggNumeric()
  {
    cannotVectorize();
    testQuery(
        "SELECT STRING_AGG(l1, ','), STRING_AGG(DISTINCT l1, ','), STRING_AGG(d1, ','), STRING_AGG(DISTINCT d1, ','), STRING_AGG(f1, ','), STRING_AGG(DISTINCT f1, ',') FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a0",
                                  ImmutableSet.of("l1"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_append(\"__acc\", \"l1\")",
                                  "array_concat(\"__acc\", \"a0\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("l1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a1",
                                  ImmutableSet.of("l1"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_set_add(\"__acc\", \"l1\")",
                                  "array_set_add_all(\"__acc\", \"a1\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("l1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a2",
                                  ImmutableSet.of("d1"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_append(\"__acc\", \"d1\")",
                                  "array_concat(\"__acc\", \"a2\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("d1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a3",
                                  ImmutableSet.of("d1"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_set_add(\"__acc\", \"d1\")",
                                  "array_set_add_all(\"__acc\", \"a3\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("d1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a4",
                                  ImmutableSet.of("f1"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_append(\"__acc\", \"f1\")",
                                  "array_concat(\"__acc\", \"a4\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("f1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a5",
                                  ImmutableSet.of("f1"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_set_add(\"__acc\", \"f1\")",
                                  "array_set_add_all(\"__acc\", \"a5\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("f1")
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{
                "7,325323,0,0,0,0",
                "0,325323,7",
                "1.0,1.7,0.0,0.0,0.0,0.0",
                "0.0,1.0,1.7",
                "1.0,0.10000000149011612,0.0,0.0,0.0,0.0",
                "0.0,0.10000000149011612,1.0"
            }
            : new Object[]{
                "7,325323,0",
                "0,325323,7",
                "1.0,1.7,0.0",
                "0.0,1.0,1.7",
                "1.0,0.10000000149011612,0.0",
                "0.0,0.10000000149011612,1.0"
            }
        )
    );
  }

  @Test
  public void testStringAggExpression()
  {
    cannotVectorize();
    skipVectorize();
    testQuery(
        "SELECT\n"
        + "  STRING_AGG(DISTINCT CONCAT(dim1, dim2), ','),\n"
        + "  STRING_AGG(DISTINCT CONCAT(dim1, dim2), CONCAT('|', '|'))\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "concat(\"dim1\",\"dim2\")", ColumnType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a0",
                                  ImmutableSet.of("v0"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_set_add(\"__acc\", \"v0\")",
                                  "array_set_add_all(\"__acc\", \"a0\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("v0")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a1",
                                  ImmutableSet.of("v0"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_set_add(\"__acc\", \"v0\")",
                                  "array_set_add_all(\"__acc\", \"a1\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, '||'))",
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("v0")
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{"10.1,1a,2,a,abc,defabc", "10.1||1a||2||a||abc||defabc"}
            : new Object[]{"1a,2,a,defabc", "1a||2||a||defabc"}
        )
    );
  }

  @Test(expected = DruidException.class)
  public void testStringAggExpressionNonConstantSeparator()
  {
    testQuery(
        "SELECT STRING_AGG(DISTINCT CONCAT(dim1, dim2), CONCAT('|', dim1)) FROM foo",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testStringAnyAggArgValidation()
  {
    DruidException e = assertThrows(DruidException.class, () -> testBuilder()
        .sql("SELECT ANY_VALUE(dim3, 1000, 'true') FROM foo")
        .queryContext(ImmutableMap.of())
        .run());
    assertThat(e, invalidSqlIs(
        "Cannot apply 'ANY_VALUE' to arguments of type 'ANY_VALUE(<VARCHAR>, <INTEGER>, <CHAR(4)>)'. Supported form(s): 'ANY_VALUE(<expr>, [<maxBytesPerStringInt>, [<aggregateMultipleValuesBoolean>]])' (line [1], column [8])"));
    DruidException e1 = assertThrows(DruidException.class, () -> testBuilder()
        .sql("SELECT ANY_VALUE(dim3, 1000, null) FROM foo")
        .queryContext(ImmutableMap.of()).run());
    Assert.assertEquals("Illegal use of 'NULL' (line [1], column [30])", e1.getMessage());
    DruidException e2 = assertThrows(DruidException.class, () -> testBuilder()
        .sql("SELECT ANY_VALUE(dim3, null, true) FROM foo")
        .queryContext(ImmutableMap.of()).run());
    Assert.assertEquals("Illegal use of 'NULL' (line [1], column [24])", e2.getMessage());
  }

  @Test
  public void testStringAggMaxBytes()
  {
    cannotVectorize();
    testQuery(
        "SELECT STRING_AGG(l1, ',', 128), STRING_AGG(DISTINCT l1, ',', CAST(128 AS INTEGER)) FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a0",
                                  ImmutableSet.of("l1"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_append(\"__acc\", \"l1\")",
                                  "array_concat(\"__acc\", \"a0\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  new HumanReadableBytes(128),
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("l1")
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a1",
                                  ImmutableSet.of("l1"),
                                  "__acc",
                                  "[]",
                                  "[]",
                                  true,
                                  false,
                                  false,
                                  "array_set_add(\"__acc\", \"l1\")",
                                  "array_set_add_all(\"__acc\", \"a1\")",
                                  null,
                                  "if(array_length(o) == 0, null, array_to_string(o, ','))",
                                  new HumanReadableBytes(128),
                                  TestExprMacroTable.INSTANCE
                              ),
                              notNull("l1")
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{"7,325323,0,0,0,0", "0,325323,7"}
            : new Object[]{"7,325323,0", "0,325323,7"}
        )
    );
  }

  /**
   * see {@link TestDataBuilder#RAW_ROWS1_WITH_NUMERIC_DIMS}
   * for the input data source of this test
   */
  @Test
  public void testHumanReadableFormatFunction()
  {
    // For the row where dim1 = '1', m1 = 4.0 and l1 is null
    testQuery(
        "SELECT m1, "
        + "HUMAN_READABLE_BINARY_BYTE_FORMAT(45678),"
        + "HUMAN_READABLE_BINARY_BYTE_FORMAT(m1*12345),"
        + "HUMAN_READABLE_BINARY_BYTE_FORMAT(m1*12345, 0), "
        + "HUMAN_READABLE_DECIMAL_BYTE_FORMAT(m1*12345), "
        + "HUMAN_READABLE_DECIMAL_FORMAT(m1*12345), "
        + "HUMAN_READABLE_BINARY_BYTE_FORMAT(l1),"
        + "HUMAN_READABLE_DECIMAL_BYTE_FORMAT(l1), "
        + "HUMAN_READABLE_DECIMAL_FORMAT(l1) "
        + "FROM numfoo WHERE dim1 = '1' LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                //
                // NOTE: the first expression HUMAN_READABLE_BINARY_BYTE_FORMAT(45678) in SQL is calculated during SQL parse phase,
                // so the converted Druid native query is its result intead of the raw function call
                //
                .virtualColumns(
                    expressionVirtualColumn("v0", "'44.61 KiB'", ColumnType.STRING),
                    expressionVirtualColumn(
                        "v1",
                        "human_readable_binary_byte_format((\"m1\" * 12345))",
                        ColumnType.STRING
                    ),
                    expressionVirtualColumn(
                        "v2",
                        "human_readable_binary_byte_format((\"m1\" * 12345),0)",
                        ColumnType.STRING
                    ),
                    expressionVirtualColumn(
                        "v3",
                        "human_readable_decimal_byte_format((\"m1\" * 12345))",
                        ColumnType.STRING
                    ),
                    expressionVirtualColumn("v4", "human_readable_decimal_format((\"m1\" * 12345))", ColumnType.STRING),
                    expressionVirtualColumn("v5", "human_readable_binary_byte_format(\"l1\")", ColumnType.STRING),
                    expressionVirtualColumn("v6", "human_readable_decimal_byte_format(\"l1\")", ColumnType.STRING),
                    expressionVirtualColumn("v7", "human_readable_decimal_format(\"l1\")", ColumnType.STRING)
                )
                .columns("m1", "v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7")
                .filters(equality("dim1", "1", ColumnType.STRING))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                (float) 4.0,
                "44.61 KiB", // 45678 / 1024
                "48.22 KiB", // = m1(4.0) * 12345 / 1024
                "48 KiB", // = m1(4.0) * 12345 / 1024, precision = 0
                "49.38 KB", // decimal byte format, m1(4.0) * 12345 / 1000,
                "49.38 K", // decimal format, m1(4.0) * 12345 / 1000,
                NullHandling.replaceWithDefault() ? "0 B" : null,
                NullHandling.replaceWithDefault() ? "0 B" : null,
                NullHandling.replaceWithDefault() ? "0" : null
            }
        )
    );
  }

  @Test
  public void testHumanReadableFormatFunctionExceptionWithWrongNumberType()
  {
    this.expectedException.expect(DruidException.class);
    this.expectedException.expectMessage("Supported form(s): HUMAN_READABLE_BINARY_BYTE_FORMAT(Number, [Precision])");
    testQuery(
        "SELECT HUMAN_READABLE_BINARY_BYTE_FORMAT('45678')",
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  @Test
  public void testHumanReadableFormatFunctionWithWrongPrecisionType()
  {
    this.expectedException.expect(DruidException.class);
    this.expectedException.expectMessage("Supported form(s): HUMAN_READABLE_BINARY_BYTE_FORMAT(Number, [Precision])");
    testQuery(
        "SELECT HUMAN_READABLE_BINARY_BYTE_FORMAT(45678, '2')",
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  @Test
  public void testHumanReadableFormatFunctionWithInvalidNumberOfArguments()
  {
    this.expectedException.expect(DruidException.class);

    /*
     * frankly speaking, the exception message thrown here is a little bit confusing
     * it says it's 'expecting 1 arguments' but actually HUMAN_READABLE_BINARY_BYTE_FORMAT supports 1 or 2 arguments
     *
     * The message is returned from {@link org.apache.calcite.sql.validate.SqlValidatorImpl#handleUnresolvedFunction},
     * and we can see from its implementation that it gets the min number arguments to format the exception message.
     *
     */
    this.expectedException.expectMessage(
        "Invalid number of arguments to function 'HUMAN_READABLE_BINARY_BYTE_FORMAT'. Was expecting 1 arguments");
    testQuery(
        "SELECT HUMAN_READABLE_BINARY_BYTE_FORMAT(45678, 2, 1)",
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  @Test
  public void testCommonVirtualExpressionWithDifferentValueType()
  {
    testQuery(
        "select\n"
        + " dim1,\n"
        + " sum(cast(0 as bigint)) as s1,\n"
        + " sum(cast(0 as double)) as s2\n"
        + "from druid.foo\n"
        + "where dim1 = 'none'\n"
        + "group by dim1\n"
        + "limit 1",
        ImmutableList.of(new TopNQueryBuilder()
                             .dataSource(CalciteTests.DATASOURCE1)
                             .intervals(querySegmentSpec(Filtration.eternity()))
                             .filters(equality("dim1", "none", ColumnType.STRING))
                             .granularity(Granularities.ALL)
                             .virtualColumns(
                                 expressionVirtualColumn("v0", "'none'", ColumnType.STRING),
                                 expressionVirtualColumn("v1", "0", ColumnType.LONG),
                                 expressionVirtualColumn("v2", "0.0", ColumnType.DOUBLE)
                             )
                             .dimension(
                                 new DefaultDimensionSpec("v0", "d0")
                             )
                             .aggregators(
                                 aggregators(
                                     new LongSumAggregatorFactory("a0", "v1", null, ExprMacroTable.nil()),
                                     new DoubleSumAggregatorFactory("a1", "v2", null, ExprMacroTable.nil())
                                 ))
                             .context(QUERY_CONTEXT_DEFAULT)
                             .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                             .threshold(1)
                             .build()),
        ImmutableList.of()
    );
  }

  // When optimization in Grouping#applyProject is applied, and it reduces a Group By query to a timeseries, we
  // want it to return empty bucket if no row matches
  @Test
  public void testReturnEmptyRowWhenGroupByIsConvertedToTimeseriesWithSingleConstantDimension()
  {
    skipVectorize();
    testQuery(
        "SELECT 'A' from foo WHERE m1 = 50 AND dim1 = 'wat' GROUP BY 'foobar'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(
                      and(
                          NullHandling.replaceWithDefault()
                          ? selector("m1", "50")
                          : equality("m1", 50.0, ColumnType.FLOAT),
                          equality("dim1", "wat", ColumnType.STRING)
                      )
                  )
                  .granularity(Granularities.ALL)
                  .postAggregators(
                      expressionPostAgg("p0", "'A'", ColumnType.STRING)
                  )
                  .context(QUERY_CONTEXT_DO_SKIP_EMPTY_BUCKETS)
                  .build()
        ),
        ImmutableList.of()
    );


    // dim1 is not getting reduced to 'wat' in this case in Calcite (ProjectMergeRule is not getting applied),
    // therefore the query is not optimized to a timeseries query
    testQuery(
        "SELECT 'A' from foo WHERE dim1 = 'wat' GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            "foo"
                        )
                        .setInterval(querySegmentSpec(Intervals.ETERNITY))
                        .setGranularity(Granularities.ALL)
                        .addDimension(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                        .setDimFilter(equality("dim1", "wat", ColumnType.STRING))
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                expressionPostAgg("p0", "'A'", ColumnType.STRING)
                            )
                        )

                        .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testReturnEmptyRowWhenGroupByIsConvertedToTimeseriesWithMultipleConstantDimensions()
  {
    skipVectorize();
    testQuery(
        "SELECT 'A', dim1 from foo WHERE m1 = 50 AND dim1 = 'wat' GROUP BY dim1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(
                      and(
                          NullHandling.replaceWithDefault()
                          ? selector("m1", "50")
                          : equality("m1", 50.0, ColumnType.FLOAT),
                          equality("dim1", "wat", ColumnType.STRING)
                      )
                  )
                  .granularity(Granularities.ALL)
                  .postAggregators(
                      expressionPostAgg("p0", "'A'", ColumnType.STRING),
                      expressionPostAgg("p1", "'wat'", ColumnType.STRING)
                  )
                  .context(QUERY_CONTEXT_DO_SKIP_EMPTY_BUCKETS)
                  .build()
        ),
        ImmutableList.of()
    );

    // Sanity test, that even when dimensions are reduced, but should produce a valid result (i.e. when filters are
    // correct, then they should
    testQuery(
        "SELECT 'A', dim1 from foo WHERE m1 = 2.0 AND dim1 = '10.1' GROUP BY dim1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(
                      and(
                          equality("m1", 2.0, ColumnType.DOUBLE),
                          equality("dim1", "10.1", ColumnType.STRING)
                      )
                  )
                  .granularity(Granularities.ALL)
                  .postAggregators(
                      expressionPostAgg("p0", "'A'", ColumnType.STRING),
                      expressionPostAgg("p1", "'10.1'", ColumnType.STRING)
                  )
                  .context(QUERY_CONTEXT_DO_SKIP_EMPTY_BUCKETS)
                  .build()
        ),
        ImmutableList.of(new Object[]{"A", "10.1"})
    );
  }

  @Test
  public void testPlanWithInFilterLessThanInSubQueryThreshold()
  {
    String query = "SELECT l1 FROM numfoo WHERE l1 IN (4842, 4844, 4845, 14905, 4853, 29064)";

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        query,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .columns("l1")
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .legacy(false)
                  .filters(
                      NullHandling.replaceWithDefault()
                      ? in("l1", ImmutableList.of("4842", "4844", "4845", "14905", "4853", "29064"), null)
                      : or(
                          equality("l1", 4842L, ColumnType.LONG),
                          equality("l1", 4844L, ColumnType.LONG),
                          equality("l1", 4845L, ColumnType.LONG),
                          equality("l1", 4853L, ColumnType.LONG),
                          equality("l1", 14905L, ColumnType.LONG),
                          equality("l1", 29064L, ColumnType.LONG)
                      )
                  )
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        ),
        (sql, result) -> {
          // Ignore the results, only need to check that the type of query is a filter.
        },
        null
    );
  }

  @Test
  public void testGreatestFunctionForNumberWithIsNull()
  {
    String query = "SELECT dim1, MAX(GREATEST(l1, l2)) IS NULL FROM druid.numfoo GROUP BY dim1";

    List<Object[]> expectedResult;
    List<Query<?>> expectedQueries;

    if (NullHandling.replaceWithDefault()) {
      expectedResult = ImmutableList.of(
          new Object[]{"", false},
          new Object[]{"1", false},
          new Object[]{"10.1", false},
          new Object[]{"2", false},
          new Object[]{"abc", false},
          new Object[]{"def", false}
      );
      expectedQueries = ImmutableList.of(
          GroupByQuery.builder()
                      .setDataSource(CalciteTests.DATASOURCE3)
                      .setInterval(querySegmentSpec(Intervals.ETERNITY))
                      .setGranularity(Granularities.ALL)
                      .addDimension(new DefaultDimensionSpec("dim1", "_d0"))
                      .setPostAggregatorSpecs(ImmutableList.of(
                          expressionPostAgg("p0", "0", ColumnType.LONG)
                      ))
                      .build()
      );
    } else {
      cannotVectorize();
      expectedResult = ImmutableList.of(
          new Object[]{"", false},
          new Object[]{"1", true},
          new Object[]{"10.1", false},
          new Object[]{"2", false},
          new Object[]{"abc", true},
          new Object[]{"def", true}
      );
      expectedQueries = ImmutableList.of(
          GroupByQuery.builder()
                      .setDataSource(CalciteTests.DATASOURCE3)
                      .setInterval(querySegmentSpec(Intervals.ETERNITY))
                      .setVirtualColumns(
                          expressionVirtualColumn(
                              "v0",
                              "greatest(\"l1\",\"l2\")",
                              ColumnType.LONG
                          )
                      )
                      .setGranularity(Granularities.ALL)
                      .addDimension(new DefaultDimensionSpec("dim1", "_d0"))
                      .addAggregator(new LongMaxAggregatorFactory("a0", "v0"))
                      .setPostAggregatorSpecs(ImmutableList.of(
                          expressionPostAgg("p0", "isnull(\"a0\")", ColumnType.LONG)
                      ))
                      .build()
      );
    }

    testQuery(
        query,
        expectedQueries,
        expectedResult
    );
  }

  @Test
  public void testGreatestFunctionForStringWithIsNull()
  {
    msqIncompatible();
    cannotVectorize();

    String query = "SELECT l1, LATEST(GREATEST(dim1, dim2)) IS NULL FROM druid.numfoo GROUP BY l1";

    testQuery(
        query,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Intervals.ETERNITY))
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "greatest(\"dim1\",\"dim2\")",
                                ColumnType.STRING
                            )
                        )
                        .setGranularity(Granularities.ALL)
                        .addDimension(new DefaultDimensionSpec("l1", "_d0", ColumnType.LONG))
                        .addAggregator(new StringLastAggregatorFactory("a0", "v0", null, 1024))
                        .setPostAggregatorSpecs(ImmutableList.of(
                            expressionPostAgg("p0", "isnull(\"a0\")", ColumnType.LONG)
                        ))
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{0L, false},
            new Object[]{7L, false},
            new Object[]{325323L, false}
        ) :
        ImmutableList.of(
            new Object[]{null, false},
            new Object[]{0L, false},
            new Object[]{7L, false},
            new Object[]{325323L, false}
        )
    );
  }

  @DecoupledTestConfig(nativeQueryIgnore = NativeQueryIgnore.AGGREGATE_REMOVE_NOT_FIRED)
  @Test
  public void testSubqueryTypeMismatchWithLiterals()
  {
    testQuery(
        "SELECT \n"
        + "  dim1,\n"
        + "  SUM(CASE WHEN sum_l1 = 0 THEN 1 ELSE 0 END) AS outer_l1\n"
        + "from (\n"
        + "  select \n"
        + "    dim1,\n"
        + "    SUM(l1) as sum_l1\n"
        + "  from numfoo\n"
        + "  group by dim1\n"
        + ")\n"
        + "group by 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Intervals.ETERNITY))
                        .setGranularity(Granularities.ALL)
                        .addDimension(new DefaultDimensionSpec("dim1", "_d0", ColumnType.STRING))
                        .addAggregator(new LongSumAggregatorFactory("a0", "l1"))
                        .setPostAggregatorSpecs(ImmutableList.of(
                            expressionPostAgg("p0", "case_searched((\"a0\" == 0),1,0)", ColumnType.LONG)))
                        .build()

        ),
        useDefault ? ImmutableList.of(
            new Object[]{"", 0L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 0L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        ) : ImmutableList.of(
            // in sql compatible mode, null does not equal 0 so the values which were 1 previously are not in this mode
            new Object[]{"", 0L},
            new Object[]{"1", 0L},
            new Object[]{"10.1", 0L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 0L},
            new Object[]{"def", 0L}
        )
    );
  }

  @Test
  public void testTimeseriesQueryWithEmptyInlineDatasourceAndGranularity()
  {
    // TODO(gianm): this test does not actually test the below thing, b/c the timestamp_floor got baked in
    msqIncompatible();
    //msqCompatible();

    // the SQL query contains an always FALSE filter ('bar' = 'baz'), which optimizes the query to also remove time
    // filter. the converted query hence contains ETERNITY interval but still a MONTH granularity due to the grouping.
    // Such a query should plan into a GroupBy query with a timestamp_floor function, instead of a timeseries
    // with granularity MONTH, to avoid excessive materialization of time grains.
    //
    // See DruidQuery#canUseQueryGranularity for the relevant check.

    cannotVectorize();

    testQuery(
        "SELECT TIME_FLOOR(__time, 'P1m'), max(m1) from "
        + "(VALUES (TIMESTAMP '2000-01-01', 1.0), (TIMESTAMP '2000-01-02', 2.0)) t (__time, m1)\n"
        + "GROUP BY 1\n"
        + "ORDER BY 1 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(InlineDataSource.fromIterable(
                            ImmutableList.of(
                                new Object[]{timestamp("2000-01-01"), 1.0},

                                // Floor to month is applied while creating inline source, so this is
                                // 2000-01-01, not 2000-01-02.
                                new Object[]{timestamp("2000-01-01"), 2.0}
                            ),
                            RowSignature.builder()
                                        .add("EXPR$0", ColumnType.LONG)
                                        .add("m1", ColumnType.DOUBLE)
                                        .build()
                        ))
                        .setInterval(querySegmentSpec(Intervals.ETERNITY))
                        .setGranularity(Granularities.ALL)
                        .addDimension(new DefaultDimensionSpec("EXPR$0", "d0", ColumnType.LONG))
                        .addAggregator(new DoubleMaxAggregatorFactory("a0", "m1"))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d0", Direction.DESCENDING, StringComparators.NUMERIC)
                                ),
                                null
                            )
                        )
                        .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 2.0}
        )
    );
  }


  @Test
  public void testComplexDecode()
  {
    cannotVectorize();
    for (String complexDecode : Arrays.asList("COMPLEX_DECODE_BASE64", "DECODE_BASE64_COMPLEX")) {
      testQuery(
          StringUtils.format(
              "SELECT %s('hyperUnique',PARSE_JSON(TO_JSON_STRING(unique_dim1))) from druid.foo LIMIT 10",
              complexDecode
          ),
          ImmutableList.of(
              Druids.newScanQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .columns("v0")
                    .virtualColumns(
                        expressionVirtualColumn(
                            "v0",
                            "complex_decode_base64('hyperUnique',parse_json(to_json_string(\"unique_dim1\")))",
                            ColumnType.ofComplex("hyperUnique")
                        )
                    )
                    .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                    .legacy(false)
                    .limit(10)
                    .build()
          ),
          ImmutableList.of(
              new Object[]{"\"AQAAAEAAAA==\""},
              new Object[]{"\"AQAAAQAAAAHNBA==\""},
              new Object[]{"\"AQAAAQAAAAOzAg==\""},
              new Object[]{"\"AQAAAQAAAAFREA==\""},
              new Object[]{"\"AQAAAQAAAACyEA==\""},
              new Object[]{"\"AQAAAQAAAAEkAQ==\""}
          )
      );
    }
  }

  @Test
  public void testComplexDecodeAgg()
  {
    msqIncompatible();
    cannotVectorize();
    testQuery(
        "SELECT APPROX_COUNT_DISTINCT_BUILTIN(COMPLEX_DECODE_BASE64('hyperUnique',PARSE_JSON(TO_JSON_STRING(unique_dim1)))) from druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      expressionVirtualColumn(
                          "v0",
                          "complex_decode_base64('hyperUnique',parse_json(to_json_string(\"unique_dim1\")))",
                          ColumnType.ofComplex("hyperUnique")
                      )
                  )
                  .aggregators(
                      new HyperUniquesAggregatorFactory(
                          "a0",
                          "v0",
                          false,
                          true
                      )
                  )
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testComplexDecodeAggWithCastedTypeName()
  {
    msqIncompatible();
    cannotVectorize();
    testQuery(
        "SELECT "
        + "APPROX_COUNT_DISTINCT_BUILTIN(COMPLEX_DECODE_BASE64(CAST('hyperUnique' AS VARCHAR),PARSE_JSON(TO_JSON_STRING(unique_dim1)))) "
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      expressionVirtualColumn(
                          "v0",
                          "complex_decode_base64('hyperUnique',parse_json(to_json_string(\"unique_dim1\")))",
                          ColumnType.ofComplex("hyperUnique")
                      )
                  )
                  .aggregators(
                      new HyperUniquesAggregatorFactory(
                          "a0",
                          "v0",
                          false,
                          true
                      )
                  )
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @NotYetSupported(Modes.MISSING_JOIN_CONVERSION)
  @Test
  public void testOrderByAlongWithInternalScanQuery()
  {
    testQuery(
        "select __time as t, m1 from druid.foo where (m1 in (select distinct m1 from druid.foo)) order by 1 limit 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dataSource(
                    JoinDataSource.create(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setGranularity(Granularities.ALL)
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Intervals.of(
                                            "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")))
                                        .setDimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT))
                                        .build()
                        ),
                        "j0.",
                        "(\"m1\" == \"j0.d0\")",
                        JoinType.INNER,
                        null,
                        ExprMacroTable.nil(),
                        CalciteTests.createJoinableFactoryWrapper()
                    )
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .intervals(querySegmentSpec(Intervals.of(
                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")))
                .limit(1)
                .columns(ImmutableList.of("__time", "m1"))
                .order(ScanQuery.Order.ASCENDING)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1.0f}
        )
    );
  }

  @NotYetSupported(Modes.MISSING_JOIN_CONVERSION)
  @Test
  public void testOrderByAlongWithInternalScanQueryNoDistinct()
  {
    testQuery(
        "select __time, m1 from druid.foo where (m1 in (select m1 from druid.foo)) order by __time DESC limit 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dataSource(
                    JoinDataSource.create(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setGranularity(Granularities.ALL)
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Intervals.of(
                                            "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")))
                                        .setDimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT))
                                        .build()
                        ),
                        "j0.",
                        "(\"m1\" == \"j0.d0\")",
                        JoinType.INNER,
                        null,
                        ExprMacroTable.nil(),
                        CalciteTests.createJoinableFactoryWrapper()
                    )
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .intervals(querySegmentSpec(Intervals.of(
                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")))
                .limit(1)
                .columns(ImmutableList.of("__time", "m1"))
                .order(ScanQuery.Order.DESCENDING)
                .build()
        ),
        ImmutableList.of(
            new Object[]{978480000000L, 6.0f}
        )
    );
  }

  @Test
  public void testFilterWithNVLAndNotIn()
  {
    testQuery(
        "select __time, dim1 from druid.foo where nvl(dim1, '') NOT IN ('a' , '')",
        ImmutableList.of(
            newScanQueryBuilder()
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                .context(QUERY_CONTEXT_DEFAULT)
                .intervals(querySegmentSpec(Intervals.of(
                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")))
                .columns(ImmutableList.of("__time", "dim1"))
                .filters(not(in("dim1", Arrays.asList("", "a"), null)))
                .build()
        ),
        ImmutableList.of(
            new Object[]{946771200000L, "10.1"},
            new Object[]{946857600000L, "2"},
            new Object[]{978307200000L, "1"},
            new Object[]{978393600000L, "def"},
            new Object[]{978480000000L, "abc"}
        )
    );
  }

  @Test
  public void testFilterWithNVLAndInIsNotTrue()
  {
    testQuery(
        "select __time, dim1 from druid.foo where (nvl(dim1, '') IN ('a' , '')) IS NOT TRUE",
        ImmutableList.of(
            newScanQueryBuilder()
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                .context(QUERY_CONTEXT_DEFAULT)
                .intervals(querySegmentSpec(Intervals.of(
                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")))
                .columns(ImmutableList.of("__time", "dim1"))
                .filters(not(in("dim1", Arrays.asList("", "a"), null)))
                .build()
        ),
        ImmutableList.of(
            new Object[]{946771200000L, "10.1"},
            new Object[]{946857600000L, "2"},
            new Object[]{978307200000L, "1"},
            new Object[]{978393600000L, "def"},
            new Object[]{978480000000L, "abc"}
        )
    );
  }

  @Test
  public void testFilterWithNvlishCaseAndNotIn()
  {
    testQuery(
        "select __time, dim1 from druid.foo where CASE WHEN dim1 IS NULL THEN '' ELSE dim1 END NOT IN ('a' , '')",
        ImmutableList.of(
            newScanQueryBuilder()
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                .context(QUERY_CONTEXT_DEFAULT)
                .intervals(querySegmentSpec(Intervals.of(
                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")))
                .columns(ImmutableList.of("__time", "dim1"))
                .filters(not(in("dim1", Arrays.asList("", "a"), null)))
                .build()
        ),
        ImmutableList.of(
            new Object[]{946771200000L, "10.1"},
            new Object[]{946857600000L, "2"},
            new Object[]{978307200000L, "1"},
            new Object[]{978393600000L, "def"},
            new Object[]{978480000000L, "abc"}
        )
    );
  }

  @Test
  public void testFilterWithNvlishCase2AndNotIn()
  {
    testQuery(
        "select __time, dim1 from druid.foo where CASE WHEN dim1 IS NOT NULL THEN dim1 ELSE '' END NOT IN ('a' , '')",
        ImmutableList.of(
            newScanQueryBuilder()
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                .context(QUERY_CONTEXT_DEFAULT)
                .intervals(querySegmentSpec(Intervals.of(
                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")))
                .columns(ImmutableList.of("__time", "dim1"))
                .filters(not(in("dim1", Arrays.asList("", "a"), null)))
                .build()
        ),
        ImmutableList.of(
            new Object[]{946771200000L, "10.1"},
            new Object[]{946857600000L, "2"},
            new Object[]{978307200000L, "1"},
            new Object[]{978393600000L, "def"},
            new Object[]{978480000000L, "abc"}
        )
    );
  }
  @Test
  public void testGroupByDateTrunc()
  {
    testQuery(
        "select DATE_TRUNC('HOUR', __time), COUNT(*) from druid.foo group by DATE_TRUNC('HOUR', __time)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .granularity(Granularities.HOUR)
                           .aggregators(aggregators(new CountAggregatorFactory("a0")))
                           .context(QUERY_CONTEXT_DEFAULT)
                           .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1L},
            new Object[]{946771200000L, 1L},
            new Object[]{946857600000L, 1L},
            new Object[]{978307200000L, 1L},
            new Object[]{978393600000L, 1L},
            new Object[]{978480000000L, 1L}
        )
    );
  }

  @Test
  public void testLatestByOnStringColumnWithoutMaxBytesSpecified()
  {
    String defaultString = useDefault ? "" : null;
    cannotVectorize();
    testQuery(
        "SELECT dim2,LATEST(dim3),LATEST_BY(dim1, __time),EARLIEST(dim3),EARLIEST_BY(dim1, __time),ANY_VALUE(dim3) FROM druid.foo where dim2='abc' group by 1",
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setVirtualColumns(
                    expressionVirtualColumn("v0", "'abc'", ColumnType.STRING))
                .setDimFilter(equality("dim2", "abc", ColumnType.STRING))
                .setDimensions(
                    dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)))
                .setAggregatorSpecs(
                    aggregators(
                        new StringLastAggregatorFactory("a0", "dim3", "__time", 1024),
                        new StringLastAggregatorFactory("a1", "dim1", "__time", 1024),
                        new StringFirstAggregatorFactory("a2", "dim3", "__time", 1024),
                        new StringFirstAggregatorFactory("a3", "dim1", "__time", 1024),
                        new StringAnyAggregatorFactory("a4", "dim3", 1024, true)))
                .build()

        ),
        ImmutableList.of(
            new Object[] {"abc", defaultString, "def", defaultString, "def", defaultString}
        ));
  }

  @Test
  public void testUnSupportedNullsFirst()
  {
    DruidException e = assertThrows(DruidException.class, () -> testBuilder()
        .queryContext(ImmutableMap.of(PlannerContext.CTX_ENABLE_WINDOW_FNS, true))
        .sql("SELECT dim1,ROW_NUMBER() OVER (ORDER BY dim1 DESC NULLS FIRST) from druid.foo")
        .run());

    assertThat(e, invalidSqlIs("DESCENDING ordering with NULLS FIRST is not supported! (line [1], column [41])"));
  }

  @Test
  public void testUnSupportedNullsLast()
  {
    DruidException e = assertThrows(DruidException.class, () -> testBuilder()
        .queryContext(ImmutableMap.of(PlannerContext.CTX_ENABLE_WINDOW_FNS, true))
        .sql("SELECT dim1,ROW_NUMBER() OVER (ORDER BY dim1 NULLS LAST) from druid.foo")
        .run());
    assertThat(e, invalidSqlIs("ASCENDING ordering with NULLS LAST is not supported! (line [1], column [41])"));
  }

  @Test
  public void testUnSupportedRangeBounds()
  {
    assumeFeatureAvailable(EngineFeature.WINDOW_FUNCTIONS);

    DruidException e = assertThrows(DruidException.class, () -> testBuilder()
        .queryContext(ImmutableMap.of(PlannerContext.CTX_ENABLE_WINDOW_FNS, true))
        .sql("SELECT dim1,ROW_NUMBER() OVER (ORDER BY dim1 RANGE BETWEEN 3 PRECEDING AND 2 FOLLOWING) from druid.foo")
        .run());
    assertThat(e, invalidSqlIs("The query contains a window frame which may return incorrect results. To disregard this warning, set [windowingStrictValidation] to false in the query context. (line [1], column [31])"));
  }

  @Test
  public void testUnSupportedWindowBoundExpressions()
  {
    assumeFeatureAvailable(EngineFeature.WINDOW_FUNCTIONS);

    DruidException e = assertThrows(DruidException.class, () -> testBuilder()
        .queryContext(ImmutableMap.of(PlannerContext.CTX_ENABLE_WINDOW_FNS, true))
        .sql("SELECT dim1,ROW_NUMBER() OVER (ORDER BY dim1 ROWS BETWEEN dim1 PRECEDING AND dim1 FOLLOWING) from druid.foo")
        .run());
    assertThat(e, invalidSqlIs("Window frames with expression based lower/upper bounds are not supported. (line [1], column [31])"));
  }


  @Test
  public void testUnSupportedWindowBoundTypes()
  {
    assumeFeatureAvailable(EngineFeature.WINDOW_FUNCTIONS);

    DruidException e;
    e = assertThrows(DruidException.class, () -> testBuilder()
        .queryContext(ImmutableMap.of(PlannerContext.CTX_ENABLE_WINDOW_FNS, true))
        .sql("SELECT dim1,ROW_NUMBER() OVER (ORDER BY dim1 ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) from druid.foo")
        .run());
    assertThat(e, invalidSqlIs("Query bounds with both lower and upper bounds as PRECEDING or FOLLOWING is not supported. (line [1], column [31])"));

    e = assertThrows(DruidException.class, () -> testBuilder()
        .queryContext(ImmutableMap.of(PlannerContext.CTX_ENABLE_WINDOW_FNS, true))
        .sql("SELECT dim1,ROW_NUMBER() OVER (ORDER BY dim1 ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) from druid.foo")
        .run());
    assertThat(e, invalidSqlIs("Query bounds with both lower and upper bounds as PRECEDING or FOLLOWING is not supported. (line [1], column [31])"));
  }


  @Test
  public void testWindowingErrorWithoutFeatureFlag()
  {
    DruidException e = assertThrows(DruidException.class, () -> testBuilder()
        .queryContext(ImmutableMap.of(PlannerContext.CTX_ENABLE_WINDOW_FNS, false))
        .sql("SELECT dim1,ROW_NUMBER() OVER () from druid.foo")
        .run());

    assertThat(e, invalidSqlIs("The query contains window functions; To run these window functions, specify [enableWindowing] in query context. (line [1], column [13])"));
  }

  @Test
  public void testInGroupByLimitOutGroupByOrderBy()
  {
    skipVectorize();
    cannotVectorize();

    testBuilder()
        .sql(
            "with t AS (SELECT m2, COUNT(m1) as trend_score\n"
                + "FROM \"foo\"\n"
                + "GROUP BY 1 \n"
                + "LIMIT 10\n"
                + ")\n"
                + "select m2, (MAX(trend_score)) from t\n"
                + "where m2 > 2\n"
                + "GROUP BY 1 \n"
                + "ORDER BY 2 DESC"
        )
        .expectedQuery(
            WindowOperatorQueryBuilder.builder()
                .setDataSource(
                    new TopNQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Filtration.eternity()))
                        .dimension(new DefaultDimensionSpec("m2", "d0", ColumnType.DOUBLE))
                        .threshold(10)
                        .aggregators(
                            aggregators(
                                useDefault
                                    ? new CountAggregatorFactory("a0")
                                    : new FilteredAggregatorFactory(
                                        new CountAggregatorFactory("a0"),
                                        notNull("m1")
                                    )
                            )
                        )
                        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
                        .context(OUTER_LIMIT_CONTEXT)
                        .build()
                )
                .setSignature(
                    RowSignature.builder()
                        .add("d0", ColumnType.DOUBLE)
                        .add("a0", ColumnType.LONG)
                        .build()
                )
                .setOperators(
                    OperatorFactoryBuilders.naiveSortOperator("a0", ColumnWithDirection.Direction.DESC)
                )
                .setLeafOperators(
                    OperatorFactoryBuilders.scanOperatorFactoryBuilder()
                        .setOffsetLimit(0, Long.MAX_VALUE)
                        .setFilter(
                            range(
                                "d0",
                                ColumnType.LONG,
                                2L,
                                null,
                                true,
                                false
                            )
                        )
                        .setProjectedColumns("a0", "d0")
                        .build()
                )
                .build()
        )
        .expectedResults(
            ImmutableList.of(
                new Object[] {3.0D, 1L},
                new Object[] {4.0D, 1L},
                new Object[] {5.0D, 1L},
                new Object[] {6.0D, 1L}
            )
        )
        .run();
  }

  @Test
  public void testInGroupByOrderByLimitOutGroupByOrderByLimit()
  {
    skipVectorize();
    cannotVectorize();
    String sql = "with t AS (SELECT m2 as mo, COUNT(m1) as trend_score\n"
        + "FROM \"foo\"\n"
        + "GROUP BY 1\n"
        + "ORDER BY trend_score DESC\n"
        + "LIMIT 10)\n"
        + "select mo, (MAX(trend_score)) from t\n"
        + "where mo > 2\n"
        + "GROUP BY 1 \n"
        + "ORDER BY 2 DESC  LIMIT 2 OFFSET 1\n";
    ImmutableList<Object[]> expectedResults = ImmutableList.of(
        new Object[] {4.0D, 1L},
        new Object[] {5.0D, 1L}
    );

    testBuilder()
        .sql(sql)
        .expectedQuery(
            WindowOperatorQueryBuilder.builder()
                .setDataSource(
                    new TopNQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Filtration.eternity()))
                        .dimension(new DefaultDimensionSpec("m2", "d0", ColumnType.DOUBLE))
                        .threshold(10)
                        .aggregators(
                            aggregators(
                                useDefault
                                    ? new CountAggregatorFactory("a0")
                                    : new FilteredAggregatorFactory(
                                        new CountAggregatorFactory("a0"),
                                        notNull("m1")
                                    )
                            )
                        )
                        .metric(new NumericTopNMetricSpec("a0"))
                        .context(OUTER_LIMIT_CONTEXT)
                        .build()
                )
                .setSignature(
                    RowSignature.builder()
                        .add("d0", ColumnType.DOUBLE)
                        .add("a0", ColumnType.LONG)
                        .build()
                )
                .setOperators(
                    OperatorFactoryBuilders.naiveSortOperator("a0", ColumnWithDirection.Direction.DESC),
                    OperatorFactoryBuilders.scanOperatorFactoryBuilder()
                        .setOffsetLimit(1, 2)
                        .build()
                )
                .setLeafOperators(
                    OperatorFactoryBuilders.scanOperatorFactoryBuilder()
                        .setOffsetLimit(0, Long.MAX_VALUE)
                        .setFilter(
                            range(
                                "d0",
                                ColumnType.LONG,
                                2L,
                                null,
                                true,
                                false
                            )
                        )
                        .setProjectedColumns("a0", "d0")
                        .build()
                )
                .build()
        )
        .expectedResults(expectedResults)
        .run();
  }


  @Test
  public void testScanAndSortCanGetSchemaFromScanQuery()
  {
    assumeTrue(NullHandling.sqlCompatible());
    msqIncompatible();
    String sql = "select * from (select * from \"wikipedia\" limit 3) order by \"user\"";
    ImmutableList<Object[]> expectedResults = ImmutableList.of(
        new Object[]{1442018825474L, "#en.wikipedia", "Auburn", "/* Status of peremptory norms under international law */ fixed spelling of 'Wimbledon'", "AU", "Australia", "true", "false", "false", "false", "false", null, "Main", "Peremptory norm", "NSW", "New South Wales", "60.225.66.142", 0L, 0L, 0L},
        new Object[]{1442018818771L, "#en.wikipedia", null, "added project", null, null, "false", "false", "false", "false", "false", null, "Talk", "Talk:Oswald Tilghman", null, null, "GELongstreet", 36L, 36L, 0L},
        new Object[]{1442018820496L, "#ca.wikipedia", null, "Robot inserta {{Commonscat}} que enlla\u00E7a amb [[commons:category:Rallicula]]", null, null, "false", "true", "false", "true", "false", null, "Main", "Rallicula", null, null, "PereBot", 17L, 17L, 0L}
    );

    testBuilder()
        .sql(sql)
        .expectedResults(expectedResults)
        .run();
  }


  @DecoupledTestConfig(nativeQueryIgnore = NativeQueryIgnore.SLIGHTLY_WORSE_PLAN)
  @Test
  public void testWindowingWithScanAndSort()
  {
    skipVectorize();
    cannotVectorize();
    msqIncompatible();
    String sql = "with t AS (\n"
        + "SELECT  \n"
        + "    RANK() OVER (PARTITION BY m2 ORDER BY m2 ASC) \n"
        + "      AS ranking,\n"
        + "    COUNT(m1) as trend_score\n"
        + "FROM foo\n"
        + "GROUP BY m2,m1 LIMIT 10\n"
        + ")\n"
        + "select ranking, trend_score from t ORDER BY trend_score";
    ImmutableList<Object[]> expectedResults = ImmutableList.of(
        new Object[] {1L, 1L},
        new Object[] {1L, 1L},
        new Object[] {1L, 1L},
        new Object[] {1L, 1L},
        new Object[] {1L, 1L},
        new Object[] {1L, 1L}
    );

    testBuilder()
        .sql(sql)
        .queryContext(ImmutableMap.of(PlannerContext.CTX_ENABLE_WINDOW_FNS, true))
        .expectedQuery(
            WindowOperatorQueryBuilder.builder()
                .setDataSource(
                    Druids.newScanQueryBuilder()
                        .dataSource(
                            new WindowOperatorQueryBuilder()
                                .setDataSource(
                                    GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(
                                            dimensions(
                                                new DefaultDimensionSpec("m2", "d0", ColumnType.DOUBLE),
                                                new DefaultDimensionSpec("m1", "d1", ColumnType.FLOAT)
                                            )
                                        )
                                        .setAggregatorSpecs(
                                            aggregators(
                                                useDefault
                                                    ? new CountAggregatorFactory("a0")
                                                    : new FilteredAggregatorFactory(
                                                        new CountAggregatorFactory("a0"),
                                                        notNull("m1")
                                                    )
                                            )
                                        )
                                        .build()
                                )
                                .setOperators(
                                    OperatorFactoryBuilders.naivePartitionOperator("d0"),
                                    OperatorFactoryBuilders.windowOperators(
                                        OperatorFactoryBuilders.rankProcessor("w0", "d0")
                                    )
                                )
                                .setSignature(
                                    RowSignature.builder()
                                        .add("w0", ColumnType.LONG)
                                        .add("a0", ColumnType.LONG)
                                        .build()
                                )
                                .build()
                        )
                        .intervals(querySegmentSpec(Filtration.eternity()))
                        .columns("a0", "w0")
                        .context(QUERY_CONTEXT_DEFAULT)
                        .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .legacy(false)
                        .limit(10)
                        .build()
                )
                .setSignature(
                    RowSignature.builder()
                        .add("w0", ColumnType.LONG)
                        .add("a0", ColumnType.LONG)
                        .build()
                )
                .setOperators(
                    OperatorFactoryBuilders.naiveSortOperator("a0", ColumnWithDirection.Direction.ASC)
                )
                .setLeafOperators(
                    OperatorFactoryBuilders.scanOperatorFactoryBuilder()
                        .setOffsetLimit(0, Long.MAX_VALUE)
                        .setProjectedColumns("a0", "w0")
                        .build()
                )
                .build()
        )
        .expectedResults(expectedResults)
        .run();
  }

  @Test
  public void testWindowingWithOrderBy()
  {
    skipVectorize();
    msqIncompatible();
    testBuilder()
        .sql(
            "SELECT\n"
                + "  FLOOR(__time TO DAY) t,\n"
                + "  SUM(cnt) c,\n"
                + "  SUM(SUM(cnt)) OVER (ORDER BY FLOOR(__time TO DAY)) cc\n"
                + "FROM foo\n"
                + "GROUP BY FLOOR(__time TO DAY)"
        )
        .queryContext(
            ImmutableMap.of(
                PlannerContext.CTX_ENABLE_WINDOW_FNS, true,
                QueryContexts.ENABLE_DEBUG, true
            )
        )
        .expectedQuery(
            WindowOperatorQueryBuilder.builder()
                .setDataSource(
                    Druids.newTimeseriesQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Filtration.eternity()))
                        .granularity(Granularities.DAY)
                        .aggregators(
                            new LongSumAggregatorFactory("a0", "cnt")
                        )
                        .context(OUTER_LIMIT_CONTEXT)
                        .build()
                )
                .setSignature(
                    RowSignature.builder()
                        .add("d0", ColumnType.LONG)
                        .add("a0", ColumnType.LONG)
                        .add("w0", ColumnType.LONG)
                        .build()
                )
                .setOperators(
                    OperatorFactoryBuilders.naivePartitionOperator(),
                    OperatorFactoryBuilders.windowOperators(
                        OperatorFactoryBuilders.framedAggregateProcessor(
                            WindowFrame.forOrderBy(ColumnWithDirection.ascending("d0")),
                            new LongSumAggregatorFactory("w0", "a0")
                        )
                    )
                )
                .build()
        )
        .expectedResults(
            ImmutableList.of(
                new Object[] {946684800000L, 1L, 1L},
                new Object[] {946771200000L, 1L, 2L},
                new Object[] {946857600000L, 1L, 3L},
                new Object[] {978307200000L, 1L, 4L},
                new Object[] {978393600000L, 1L, 5L},
                new Object[] {978480000000L, 1L, 6L}
            )
        )
        .run();
  }

  @Test
  public void testFilterParseLongNullable()
  {
    testQuery(
        "select count(*) from druid.foo where parse_long(dim1, 10) is null",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      expressionVirtualColumn(
                          "v0",
                          "parse_long(\"dim1\",10)",
                          ColumnType.LONG)
                  )
                  .filters(isNull("v0"))
                  .granularity(Granularities.ALL)
                  .aggregators(new CountAggregatorFactory("a0"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{NullHandling.sqlCompatible() ? 4L : 0L})
    );
  }
}
