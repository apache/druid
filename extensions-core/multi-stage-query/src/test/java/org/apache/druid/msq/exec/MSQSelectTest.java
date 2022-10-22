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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.indexing.ColumnMapping;
import org.apache.druid.msq.indexing.ColumnMappings;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MSQSelectTest extends MSQTestBase
{
  @Test
  public void testCalculator()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("EXPR$0", ColumnType.LONG)
                                               .build();

    testSelectQuery()
        .setSql("select 1 + 1")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(
                               InlineDataSource.fromIterable(
                                   ImmutableList.of(new Object[]{2L}),
                                   resultSignature
                               )
                           )
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("EXPR$0")
                           .context(defaultScanQueryContext(resultSignature))
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{2L})).verifyResults();
  }

  @Test
  public void testSelectOnFoo()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select cnt,dim1 from foo")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .context(defaultScanQueryContext(resultSignature))
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, !useDefault ? "" : null},
            new Object[]{1L, "10.1"},
            new Object[]{1L, "2"},
            new Object[]{1L, "1"},
            new Object[]{1L, "def"},
            new Object[]{1L, "abc"}
        )).verifyResults();
  }

  @Test
  public void testSelectOnFoo2()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("m1", ColumnType.LONG)
                                               .add("dim2", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select m1,dim2 from foo2")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(newScanQueryBuilder()
                              .dataSource(CalciteTests.DATASOURCE2)
                              .intervals(querySegmentSpec(Filtration.eternity()))
                              .columns("dim2", "m1")
                              .context(defaultScanQueryContext(
                                  RowSignature.builder()
                                              .add("dim2", ColumnType.STRING)
                                              .add("m1", ColumnType.LONG)
                                              .build()
                              ))
                              .build())
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, "en"},
            new Object[]{1L, "ru"},
            new Object[]{1L, "he"}
        )).verifyResults();
  }

  @Test
  public void testGroupByOnFoo()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo group by cnt")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(GroupByQuery.builder()
                                                      .setDataSource(CalciteTests.DATASOURCE1)
                                                      .setInterval(querySegmentSpec(Filtration
                                                                                        .eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setDimensions(dimensions(
                                                          new DefaultDimensionSpec(
                                                              "cnt",
                                                              "d0",
                                                              ColumnType.LONG
                                                          )
                                                      ))
                                                      .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                                          "a0")))
                                                      .setContext(DEFAULT_MSQ_CONTEXT)
                                                      .build())
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "cnt"),
                                           new ColumnMapping("a0", "cnt1")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{1L, 6L}))
        .verifyResults();
  }

  @Test
  public void testGroupByOrderByDimension()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
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
                            null
                        )
                    )
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("select m1, count(*) as cnt from foo group by m1 order by m1 desc")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(
                       new ColumnMappings(ImmutableList.of(
                           new ColumnMapping("d0", "m1"),
                           new ColumnMapping("a0", "cnt")
                       )
                       ))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{6f, 1L},
                new Object[]{5f, 1L},
                new Object[]{4f, 1L},
                new Object[]{3f, 1L},
                new Object[]{2f, 1L},
                new Object[]{1f, 1L}
            )
        )
        .verifyResults();
  }

  @Test
  public void testSubquery()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(
                        GroupByQuery.builder()
                                    .setDataSource("foo")
                                    .setInterval(querySegmentSpec(Filtration.eternity()))
                                    .setGranularity(Granularities.ALL)
                                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                                    .setContext(DEFAULT_MSQ_CONTEXT)
                                    .build()
                    )
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("select count(*) AS cnt from (select distinct m1 from foo)")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(new ColumnMappings(ImmutableList.of(new ColumnMapping("a0", "cnt"))))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{6L}))
        .verifyResults();
  }

  @Test
  public void testJoin()
  {
    final RowSignature resultSignature = RowSignature.builder()
                                                     .add("dim2", ColumnType.STRING)
                                                     .add("EXPR$1", ColumnType.DOUBLE)
                                                     .build();

    final ImmutableList<Object[]> expectedResults;

    if (NullHandling.sqlCompatible()) {
      expectedResults = ImmutableList.of(
          new Object[]{null, 4.0},
          new Object[]{"", 3.0},
          new Object[]{"a", 2.5},
          new Object[]{"abc", 5.0}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{null, 3.6666666666666665},
          new Object[]{"a", 2.5},
          new Object[]{"abc", 5.0}
      );
    }

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(
                        join(
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .columns("dim2", "m1", "m2")
                                    .context(
                                        defaultScanQueryContext(
                                            RowSignature.builder()
                                                        .add("dim2", ColumnType.STRING)
                                                        .add("m1", ColumnType.FLOAT)
                                                        .add("m2", ColumnType.DOUBLE)
                                                        .build()
                                        )
                                    )
                                    .limit(10)
                                    .build()
                            ),
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .columns(ImmutableList.of("m1"))
                                    .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                    .context(
                                        defaultScanQueryContext(
                                            RowSignature.builder().add("m1", ColumnType.FLOAT).build()
                                        )
                                    )
                                    .build()
                            ),
                            "j0.",
                            equalsCondition(
                                DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                                DruidExpression.ofColumn(ColumnType.FLOAT, "j0.m1")
                            ),
                            JoinType.INNER
                        )
                    )
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setDimensions(new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING))
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(
                        useDefault
                        ? aggregators(
                            new DoubleSumAggregatorFactory("a0:sum", "m2"),
                            new CountAggregatorFactory("a0:count")
                        )
                        : aggregators(
                            new DoubleSumAggregatorFactory("a0:sum", "m2"),
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("a0:count"),
                                not(selector("m2", null, null)),

                                // Not sure why the name is only set in SQL-compatible null mode. Seems strange.
                                // May be due to JSON serialization: name is set on the serialized aggregator even
                                // if it was originally created with no name.
                                NullHandling.sqlCompatible() ? "a0:count" : null
                            )
                        )
                    )
                    .setPostAggregatorSpecs(
                        ImmutableList.of(
                            new ArithmeticPostAggregator(
                                "a0",
                                "quotient",
                                ImmutableList.of(
                                    new FieldAccessPostAggregator(null, "a0:sum"),
                                    new FieldAccessPostAggregator(null, "a0:count")
                                )
                            )

                        )
                    )
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql(
            "SELECT t1.dim2, AVG(t1.m2) FROM "
            + "(SELECT * FROM foo LIMIT 10) AS t1 "
            + "INNER JOIN foo AS t2 "
            + "ON t1.m1 = t2.m1 "
            + "GROUP BY t1.dim2"
        )
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(new ColumnMappings(ImmutableList.of(
                       new ColumnMapping("d0", "dim2"),
                       new ColumnMapping("a0", "EXPR$1")
                   )))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(expectedResults)
        .verifyResults();
  }

  @Test
  public void testGroupByOrderByAggregation()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("sum_m1", ColumnType.DOUBLE)
                                            .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                    .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                    .setLimitSpec(
                        new DefaultLimitSpec(
                            ImmutableList.of(
                                new OrderByColumnSpec(
                                    "a0",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.NUMERIC
                                )
                            ),
                            null
                        )
                    )
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("select m1, sum(m1) as sum_m1 from foo group by m1 order by sum_m1 desc")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(
                       new ColumnMappings(
                           ImmutableList.of(
                               new ColumnMapping("d0", "m1"),
                               new ColumnMapping("a0", "sum_m1")
                           )
                       )
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{6f, 6d},
                new Object[]{5f, 5d},
                new Object[]{4f, 4d},
                new Object[]{3f, 3d},
                new Object[]{2f, 2d},
                new Object[]{1f, 1d}
            )
        ).verifyResults();
  }

  @Test
  public void testGroupByOrderByAggregationWithLimit()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("sum_m1", ColumnType.DOUBLE)
                                            .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                    .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                    .setLimitSpec(
                        new DefaultLimitSpec(
                            ImmutableList.of(
                                new OrderByColumnSpec(
                                    "a0",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.NUMERIC
                                )
                            ),
                            3
                        )
                    )
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("select m1, sum(m1) as sum_m1 from foo group by m1 order by sum_m1 desc limit 3")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(
                       new ColumnMappings(
                           ImmutableList.of(
                               new ColumnMapping("d0", "m1"),
                               new ColumnMapping("a0", "sum_m1")
                           )
                       )
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{6f, 6d},
                new Object[]{5f, 5d},
                new Object[]{4f, 4d}
            )
        ).verifyResults();
  }

  @Test
  public void testGroupByOrderByAggregationWithLimitAndOffset()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("sum_m1", ColumnType.DOUBLE)
                                            .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                    .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                    .setLimitSpec(
                        new DefaultLimitSpec(
                            ImmutableList.of(
                                new OrderByColumnSpec(
                                    "a0",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.NUMERIC
                                )
                            ),
                            1,
                            2
                        )
                    )
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("select m1, sum(m1) as sum_m1 from foo group by m1 order by sum_m1 desc limit 2 offset 1")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(
                       new ColumnMappings(
                           ImmutableList.of(
                               new ColumnMapping("d0", "m1"),
                               new ColumnMapping("a0", "sum_m1")
                           )
                       )
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{5f, 5d},
                new Object[]{4f, 4d}
            )
        ).verifyResults();
  }

  @Test
  public void testExternSelect1() throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    final GroupByQuery expectedQuery =
        GroupByQuery.builder()
                    .setDataSource(
                        new ExternalDataSource(
                            new LocalInputSource(null, null, ImmutableList.of(toRead.getAbsoluteFile())),
                            new JsonInputFormat(null, null, null, null, null),
                            RowSignature.builder()
                                        .add("timestamp", ColumnType.STRING)
                                        .add("page", ColumnType.STRING)
                                        .add("user", ColumnType.STRING)
                                        .build()
                        )
                    )
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setVirtualColumns(
                        new ExpressionVirtualColumn(
                            "v0",
                            "timestamp_floor(timestamp_parse(\"timestamp\",null,'UTC'),'P1D',null,'UTC')",
                            ColumnType.LONG,
                            CalciteTests.createExprMacroTable()
                        )
                    )
                    .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                    .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("SELECT\n"
                + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                + "  count(*) as cnt\n"
                + "FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [" + toReadAsJson + "],\"type\":\"local\"}',\n"
                + "    '{\"type\": \"json\"}',\n"
                + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                + "  )\n"
                + ") group by 1")
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
        .setExpectedMSQSpec(
            MSQSpec
                .builder()
                .query(expectedQuery)
                .columnMappings(new ColumnMappings(
                    ImmutableList.of(
                        new ColumnMapping("d0", "__time"),
                        new ColumnMapping("a0", "cnt")
                    )
                ))
                .tuningConfig(MSQTuningConfig.defaultConfig())
                .build())
        .verifyResults();
  }

  @Test
  public void testIncorrectSelectQuery()
  {
    testSelectQuery()
        .setSql("select a from ")
        .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(SqlPlanningException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Encountered \"from <EOF>\""))
        ))
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnInformationSchemaSource()
  {
    testSelectQuery()
        .setSql("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA")
        .setExpectedValidationErrorMatcher(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                    "Cannot query table INFORMATION_SCHEMA.SCHEMATA with SQL engine 'msq-task'."))
            )
        )
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnSysSource()
  {
    testSelectQuery()
        .setSql("SELECT * FROM sys.segments")
        .setExpectedValidationErrorMatcher(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                    "Cannot query table sys.segments with SQL engine 'msq-task'."))
            )
        )
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnSysSourceWithJoin()
  {
    testSelectQuery()
        .setSql("select s.segment_id, s.num_rows, f.dim1 from sys.segments as s, foo as f")
        .setExpectedValidationErrorMatcher(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                    "Cannot query table sys.segments with SQL engine 'msq-task'."))
            )
        )
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnSysSourceContainingWith()
  {
    testSelectQuery()
        .setSql("with segment_source as (SELECT * FROM sys.segments) "
                + "select segment_source.segment_id, segment_source.num_rows from segment_source")
        .setExpectedValidationErrorMatcher(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                    "Cannot query table sys.segments with SQL engine 'msq-task'."))
            )
        )
        .verifyPlanningErrors();
  }


  @Test
  public void testSelectOnUserDefinedSourceContainingWith()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("m1", ColumnType.LONG)
                                               .add("dim2", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("with sys as (SELECT * FROM foo2) "
                + "select m1, dim2 from sys")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE2)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("dim2", "m1")
                           .context(defaultScanQueryContext(
                               RowSignature.builder()
                                           .add("dim2", ColumnType.STRING)
                                           .add("m1", ColumnType.LONG)
                                           .build()
                           ))
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, "en"},
            new Object[]{1L, "ru"},
            new Object[]{1L, "he"}
        ))
        .verifyResults();
  }

  @Test
  public void testScanWithMultiValueSelectQuery()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("dim3", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select dim3 from foo")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(newScanQueryBuilder()
                                              .dataSource(CalciteTests.DATASOURCE1)
                                              .intervals(querySegmentSpec(Filtration.eternity()))
                                              .columns("dim3")
                                              .context(defaultScanQueryContext(resultSignature))
                                              .build())
                                   .columnMappings(ColumnMappings.identity(resultSignature))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{ImmutableList.of("a", "b")},
            new Object[]{ImmutableList.of("b", "c")},
            new Object[]{"d"},
            new Object[]{!useDefault ? "" : null},
            new Object[]{null},
            new Object[]{null}
        )).verifyResults();
  }

  @Test
  public void testGroupByWithMultiValue()
  {
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_MSQ_CONTEXT)
                                              .put("groupByEnableMultiValueUnnesting", true)
                                              .build();
    RowSignature rowSignature = RowSignature.builder()
                                            .add("dim3", ColumnType.STRING)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select dim3, count(*) as cnt1 from foo group by dim3")
        .setQueryContext(context)
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       GroupByQuery.builder()
                                   .setDataSource(CalciteTests.DATASOURCE1)
                                   .setInterval(querySegmentSpec(Filtration.eternity()))
                                   .setGranularity(Granularities.ALL)
                                   .setDimensions(
                                       dimensions(
                                           new DefaultDimensionSpec(
                                               "dim3",
                                               "d0"
                                           )
                                       )
                                   )
                                   .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                       "a0")))
                                   .setContext(context)
                                   .build()
                   )
                   .columnMappings(
                       new ColumnMappings(ImmutableList.of(
                           new ColumnMapping("d0", "dim3"),
                           new ColumnMapping("a0", "cnt1")
                       )
                       ))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            expectedMultiValueFooRowsGroup()
        )
        .verifyResults();
  }


  @Test
  public void testGroupByWithMultiValueWithoutGroupByEnable()
  {
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_MSQ_CONTEXT)
                                              .put("groupByEnableMultiValueUnnesting", false)
                                              .build();

    testSelectQuery()
        .setSql("select dim3, count(*) as cnt1 from foo group by dim3")
        .setQueryContext(context)
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(ISE.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                "Encountered multi-value dimension [dim3] that cannot be processed with 'groupByEnableMultiValueUnnesting' set to false."))
        ))
        .verifyExecutionError();
  }

  @Test
  public void testGroupByWithMultiValueMvToArray()
  {
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_MSQ_CONTEXT)
                                              .put("groupByEnableMultiValueUnnesting", true)
                                              .build();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("EXPR$0", ColumnType.STRING_ARRAY)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select MV_TO_ARRAY(dim3), count(*) as cnt1 from foo group by dim3")
        .setQueryContext(context)
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(GroupByQuery.builder()
                                                      .setDataSource(CalciteTests.DATASOURCE1)
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setDimensions(
                                                          dimensions(
                                                              new DefaultDimensionSpec(
                                                                  "dim3",
                                                                  "d0"
                                                              )
                                                          )
                                                      )
                                                      .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                                          "a0")))
                                                      .setPostAggregatorSpecs(
                                                          ImmutableList.of(new ExpressionPostAggregator(
                                                                               "p0",
                                                                               "mv_to_array(\"d0\")",
                                                                               null, ExprMacroTable.nil()
                                                                           )
                                                          )
                                                      )
                                                      .setContext(context)
                                                      .build()
                                   )
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("p0", "EXPR$0"),
                                           new ColumnMapping("a0", "cnt1")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            expectedMultiValueFooRowsGroupByList()
        )
        .verifyResults();
  }

  @Test
  public void testGroupByArrayWithMultiValueMvToArray()
  {
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_MSQ_CONTEXT)
                                              .put("groupByEnableMultiValueUnnesting", true)
                                              .build();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("EXPR$0", ColumnType.STRING_ARRAY)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    ArrayList<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{Collections.singletonList(null), !useDefault ? 2L : 3L});
    if (!useDefault) {
      expected.add(new Object[]{Collections.singletonList(""), 1L});
    }
    expected.addAll(ImmutableList.of(
        new Object[]{Arrays.asList("a", "b"), 1L},
        new Object[]{Arrays.asList("b", "c"), 1L},
        new Object[]{Collections.singletonList("d"), 1L}
    ));

    testSelectQuery()
        .setSql("select MV_TO_ARRAY(dim3), count(*) as cnt1 from foo group by MV_TO_ARRAY(dim3)")
        .setQueryContext(context)
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(GroupByQuery.builder()
                                                      .setDataSource(CalciteTests.DATASOURCE1)
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setDimensions(
                                                          dimensions(
                                                              new DefaultDimensionSpec(
                                                                  "v0",
                                                                  "d0",
                                                                  ColumnType.STRING_ARRAY
                                                              )
                                                          )
                                                      )
                                                      .setVirtualColumns(
                                                          new ExpressionVirtualColumn(
                                                              "v0",
                                                              "mv_to_array(\"dim3\")",
                                                              ColumnType.STRING_ARRAY,
                                                              TestExprMacroTable.INSTANCE
                                                          )
                                                      )
                                                      .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                                      .setContext(context)
                                                      .build()
                                   )
                                   .columnMappings(
                                       new ColumnMappings(
                                           ImmutableList.of(
                                               new ColumnMapping("d0", "EXPR$0"),
                                               new ColumnMapping("a0", "cnt1")
                                           )
                                       )
                                   )
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(expected)
        .verifyResults();
  }

  @Test
  public void testGroupByWithMultiValueMvToArrayWithoutGroupByEnable()
  {
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_MSQ_CONTEXT)
                                              .put("groupByEnableMultiValueUnnesting", false)
                                              .build();

    testSelectQuery()
        .setSql("select MV_TO_ARRAY(dim3), count(*) as cnt1 from foo group by dim3")
        .setQueryContext(context)
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(ISE.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                "Encountered multi-value dimension [dim3] that cannot be processed with 'groupByEnableMultiValueUnnesting' set to false."))
        ))
        .verifyExecutionError();
  }

  @Test
  public void testGroupByMultiValueMeasureQuery()
  {
    final RowSignature rowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("cnt1", ColumnType.LONG)
                                                  .build();

    final GroupByQuery expectedQuery =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)))
                    .setAggregatorSpecs(
                        aggregators(
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("a0"),
                                new NotDimFilter(new SelectorDimFilter("dim3", null, null)),
                                "a0"
                            )
                        )
                    )
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("select __time, count(dim3) as cnt1 from foo group by __time")
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(expectedQuery)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "__time"),
                                           new ColumnMapping("a0", "cnt1")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{946684800000L, 1L},
                new Object[]{946771200000L, 1L},
                new Object[]{946857600000L, 1L},
                new Object[]{978307200000L, !useDefault ? 1L : 0L},
                new Object[]{978393600000L, 0L},
                new Object[]{978480000000L, 0L}
            )
        )
        .verifyResults();
  }

  @Nonnull
  private List<Object[]> expectedMultiValueFooRowsGroup()
  {
    ArrayList<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{null, !useDefault ? 2L : 3L});
    if (!useDefault) {
      expected.add(new Object[]{"", 1L});
    }
    expected.addAll(ImmutableList.of(
        new Object[]{"a", 1L},
        new Object[]{"b", 2L},
        new Object[]{"c", 1L},
        new Object[]{"d", 1L}
    ));
    return expected;
  }

  @Nonnull
  private List<Object[]> expectedMultiValueFooRowsGroupByList()
  {
    ArrayList<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{Collections.singletonList(null), !useDefault ? 2L : 3L});
    if (!useDefault) {
      expected.add(new Object[]{Collections.singletonList(""), 1L});
    }
    expected.addAll(ImmutableList.of(
        new Object[]{Collections.singletonList("a"), 1L},
        new Object[]{Collections.singletonList("b"), 2L},
        new Object[]{Collections.singletonList("c"), 1L},
        new Object[]{Collections.singletonList("d"), 1L}
    ));
    return expected;
  }
}
