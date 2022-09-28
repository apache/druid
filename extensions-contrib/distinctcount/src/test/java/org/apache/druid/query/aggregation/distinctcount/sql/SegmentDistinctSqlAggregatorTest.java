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

package org.apache.druid.query.aggregation.distinctcount.sql;

import com.google.common.collect.ImmutableList;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.distinctcount.DistinctCountAggregatorFactory;
import org.apache.druid.query.aggregation.distinctcount.DistinctCountDruidModule;
import org.apache.druid.query.aggregation.distinctcount.RoaringBitMapFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

public class SegmentDistinctSqlAggregatorTest extends BaseCalciteQueryTest
{


  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.add(new DistinctCountDruidModule());
  }

  @Test
  public void testDistinctCount() throws Exception
  {
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "SEGMENT_DISTINCT(dim1),"
        + " SEGMENT_DISTINCT(dim2) FILTER(WHERE dim2 <> ''),\n" // filtered
        + " SEGMENT_DISTINCT(SUBSTRING(dim2, 1, 1))" //  on extractionFn
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "substring(\"dim2\", 0, 1)",
                          ColumnType.STRING,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                                   new DistinctCountAggregatorFactory(
                                       "a0",
                                       "dim1",
                                       new RoaringBitMapFactory()
                                   ),
                                   new FilteredAggregatorFactory(
                                       new DistinctCountAggregatorFactory(
                                           "a1",
                                           "dim2",
                                           new RoaringBitMapFactory()
                                       ), BaseCalciteQueryTest.not(BaseCalciteQueryTest.selector("dim2", "", null))),
                                   new DistinctCountAggregatorFactory(
                                       "a2",
                                       "v0",
                                       new RoaringBitMapFactory()
                                   )
                               )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()),
        ImmutableList.of(new Long[]{6L, 2L, 3L})
    );
  }
}
