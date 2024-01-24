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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMaxAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.SingleValueAggregatoractory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

public class CalciteSingleValueAggregatorTest extends BaseCalciteQueryTest
{

  @Test
  public void testSingleValueFloatAgg()
  {
    msqIncompatible();
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT count(*) FROM foo where m1 <= (select min(m1) + 4 from foo)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(join(
                      new TableDataSource(CalciteTests.DATASOURCE1),
                      new QueryDataSource(GroupByQuery.builder()
                                                      .setDataSource(new QueryDataSource(
                                                          Druids.newTimeseriesQueryBuilder()
                                                                .dataSource(CalciteTests.DATASOURCE1)
                                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                                .granularity(Granularities.ALL)
                                                                .aggregators(new FloatMinAggregatorFactory("a0", "m1"))
                                                                .build()
                                                      ))
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setVirtualColumns(expressionVirtualColumn(
                                                                             "v0",
                                                                             "(\"a0\" + 4)",
                                                                             ColumnType.FLOAT
                                                                         )
                                                      )
                                                      .setAggregatorSpecs(
                                                          aggregators(
                                                              new SingleValueAggregatoractory(
                                                                  "_a0",
                                                                  "v0",
                                                                  ColumnType.FLOAT
                                                              )
                                                          )
                                                      )
                                                      .setLimitSpec(NoopLimitSpec.instance())
                                                      .setContext(QUERY_CONTEXT_DEFAULT)
                                                      .build()
                      ),
                      "j0.",
                      "1",
                      JoinType.INNER
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(expressionFilter("(\"m1\" <= \"j0._a0\")"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testSingleValueDoubleAgg()
  {
    msqIncompatible();
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT count(*) FROM foo where m1 >= (select max(m1) - 3.5 from foo)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(join(
                      new TableDataSource(CalciteTests.DATASOURCE1),
                      new QueryDataSource(GroupByQuery.builder()
                                                      .setDataSource(new QueryDataSource(
                                                          Druids.newTimeseriesQueryBuilder()
                                                                .dataSource(CalciteTests.DATASOURCE1)
                                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                                .granularity(Granularities.ALL)
                                                                .aggregators(new FloatMaxAggregatorFactory("a0", "m1"))
                                                                .build()
                                                      ))
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setVirtualColumns(expressionVirtualColumn(
                                                                             "v0",
                                                                             "(\"a0\" - 3.5)",
                                                                             ColumnType.DOUBLE
                                                                         )
                                                      )
                                                      .setAggregatorSpecs(
                                                          aggregators(
                                                              new SingleValueAggregatoractory(
                                                                  "_a0",
                                                                  "v0",
                                                                  ColumnType.DOUBLE
                                                              )
                                                          )
                                                      )
                                                      .setLimitSpec(NoopLimitSpec.instance())
                                                      .setContext(QUERY_CONTEXT_DEFAULT)
                                                      .build()
                      ),
                      "j0.",
                      "1",
                      JoinType.INNER
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(expressionFilter("(\"m1\" >= \"j0._a0\")"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{4L}
        )
    );
  }

  @Test
  public void testSingleValueLongAgg()
  {
    msqIncompatible();
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT count(*) FROM wikipedia where __time >= (select max(__time) - INTERVAL '10' MINUTE from wikipedia)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(join(
                      new TableDataSource(CalciteTests.WIKIPEDIA),
                      new QueryDataSource(GroupByQuery.builder()
                                                      .setDataSource(new QueryDataSource(
                                                          Druids.newTimeseriesQueryBuilder()
                                                                .dataSource(CalciteTests.WIKIPEDIA)
                                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                                .granularity(Granularities.ALL)
                                                                .aggregators(new LongMaxAggregatorFactory(
                                                                    "a0",
                                                                    "__time"
                                                                ))
                                                                .build()
                                                      ))
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setVirtualColumns(expressionVirtualColumn(
                                                                             "v0",
                                                                             "(\"a0\" - 600000)",
                                                                             ColumnType.LONG
                                                                         )
                                                      )
                                                      .setAggregatorSpecs(
                                                          aggregators(
                                                              new SingleValueAggregatoractory(
                                                                  "_a0",
                                                                  "v0",
                                                                  ColumnType.LONG
                                                              )
                                                          )
                                                      )
                                                      .setLimitSpec(NoopLimitSpec.instance())
                                                      .setContext(QUERY_CONTEXT_DEFAULT)
                                                      .build()
                      ),
                      "j0.",
                      "1",
                      JoinType.INNER
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(expressionFilter("(\"__time\" >= \"j0._a0\")"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{220L}
        )
    );
  }

  @Test
  public void testSingleValueStringAgg()
  {
    msqIncompatible();
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT  count(*) FROM wikipedia where channel = (select channel from wikipedia order by __time desc LIMIT 1 OFFSET 6)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(join(
                      new TableDataSource(CalciteTests.WIKIPEDIA),
                      new QueryDataSource(GroupByQuery.builder()
                                                      .setDataSource(new QueryDataSource(
                                                          Druids.newScanQueryBuilder()
                                                                .dataSource(CalciteTests.WIKIPEDIA)
                                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                                .offset(6L)
                                                                .limit(1L)
                                                                .order(ScanQuery.Order.DESCENDING)
                                                                .columns("__time", "channel")
                                                                .legacy(false)
                                                                .context(QUERY_CONTEXT_DEFAULT)
                                                                .build()
                                                      ))
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setVirtualColumns(expressionVirtualColumn(
                                                                             "v0",
                                                                             "\"channel\"",
                                                                             ColumnType.STRING
                                                                         )
                                                      )
                                                      .setAggregatorSpecs(
                                                          aggregators(
                                                              new SingleValueAggregatoractory(
                                                                  "a0",
                                                                  "v0",
                                                                  ColumnType.STRING
                                                              )
                                                          )
                                                      )
                                                      .setLimitSpec(NoopLimitSpec.instance())
                                                      .setContext(QUERY_CONTEXT_DEFAULT)
                                                      .build()
                      ),
                      "j0.",
                      "(\"channel\" == \"j0.a0\")",
                      JoinType.INNER
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1256L}
        )
    );
  }

  @Test
  public void testSingleValueStringMultipleRowsAgg()
  {
    msqIncompatible();
    skipVectorize();
    cannotVectorize();
    testQueryThrows(
        "SELECT  count(*) FROM wikipedia where channel = (select channel from wikipedia order by __time desc LIMIT 2 OFFSET 6)",
        exception -> exception.expectMessage("Single Value Aggregator would not be applied to more than one row")
    );
  }

  @Test
  public void testSingleValueEmptyInnerAgg()
  {
    msqIncompatible();
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT distinct countryName FROM wikipedia where countryName = ( select countryName from wikipedia where channel in ('abc', 'xyz'))",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(join(
                            new TableDataSource(CalciteTests.WIKIPEDIA),
                            new QueryDataSource(Druids.newTimeseriesQueryBuilder()
                                                      .dataSource(CalciteTests.WIKIPEDIA)
                                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                                      .granularity(Granularities.ALL)
                                                      .virtualColumns(expressionVirtualColumn(
                                                                          "v0",
                                                                          "\"countryName\"",
                                                                          ColumnType.STRING
                                                                      )
                                                      )
                                                      .aggregators(
                                                          new SingleValueAggregatoractory(
                                                              "a0",
                                                              "v0",
                                                              ColumnType.STRING
                                                          )
                                                      )
                                                      .filters(new InDimFilter(
                                                          "channel",
                                                          new HashSet<>(Arrays.asList(
                                                              "abc",
                                                              "xyz"
                                                          ))
                                                      ))
                                                      .context(QUERY_CONTEXT_DEFAULT)
                                                      .build()
                            ),
                            "j0.",
                            "(\"countryName\" == \"j0.a0\")",
                            JoinType.INNER
                        ))
                        .addDimension(new DefaultDimensionSpec("countryName", "d0", ColumnType.STRING))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of()
    );
  }

}
