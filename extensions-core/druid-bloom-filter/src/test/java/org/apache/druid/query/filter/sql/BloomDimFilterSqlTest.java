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

package org.apache.druid.query.filter.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.calcite.avatica.SqlType;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.BloomFilterExtensionModule;
import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.expressions.BloomFilterExpressions;
import org.apache.druid.query.filter.BloomDimFilter;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.filter.BloomKFilterHolder;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.aggregation.ApproxCountDistinctSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.BuiltinApproxCountDistinctSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.CountSqlAggregator;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.SqlParameter;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BloomDimFilterSqlTest extends BaseCalciteQueryTest
{
  @Override
  public DruidOperatorTable createOperatorTable()
  {
    CalciteTests.getJsonMapper().registerModule(new BloomFilterSerializersModule());
    return new DruidOperatorTable(
        ImmutableSet.of(
            new CountSqlAggregator(new ApproxCountDistinctSqlAggregator(new BuiltinApproxCountDistinctSqlAggregator()))
        ),
        ImmutableSet.of(new BloomFilterOperatorConversion())
    );
  }

  @Override
  public ExprMacroTable createMacroTable()
  {
    final List<ExprMacroTable.ExprMacro> exprMacros = new ArrayList<>();
    for (Class<? extends ExprMacroTable.ExprMacro> clazz : ExpressionModule.EXPR_MACROS) {
      exprMacros.add(CalciteTests.INJECTOR.getInstance(clazz));
    }
    exprMacros.add(CalciteTests.INJECTOR.getInstance(LookupExprMacro.class));
    exprMacros.add(new BloomFilterExpressions.TestExprMacro());
    return new ExprMacroTable(exprMacros);
  }

  @Override
  public Iterable<? extends Module> getJacksonModules()
  {
    return Iterables.concat(super.getJacksonModules(), new BloomFilterExtensionModule().getJacksonModules());
  }

  @Test
  public void testBloomFilter() throws IOException
  {
    BloomKFilter filter = new BloomKFilter(1500);
    filter.addString("def");
    byte[] bytes = BloomFilterSerializersModule.bloomKFilterToBytes(filter);
    String base64 = StringUtils.encodeBase64String(bytes);

    testQuery(
        StringUtils.format("SELECT COUNT(*) FROM druid.foo WHERE bloom_filter_test(dim1, '%s')", base64),
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      new BloomDimFilter("dim1", BloomKFilterHolder.fromBloomKFilter(filter), null)
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
  public void testBloomFilterExprFilter() throws IOException
  {
    BloomKFilter filter = new BloomKFilter(1500);
    filter.addString("a-foo");
    filter.addString("-foo");
    if (!NullHandling.replaceWithDefault()) {
      filter.addBytes(null, 0, 0);
    }
    byte[] bytes = BloomFilterSerializersModule.bloomKFilterToBytes(filter);
    String base64 = StringUtils.encodeBase64String(bytes);

    // fool the planner to make an expression virtual column to test bloom filter Druid expression
    testQuery(
        StringUtils.format("SELECT COUNT(*) FROM druid.foo WHERE nullif(bloom_filter_test(concat(dim2, '-foo'), '%s'), 1) is null", base64),
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      new ExpressionDimFilter(
                          StringUtils.format(
                              "case_searched(bloom_filter_test(concat(\"dim2\",'-foo'),'%s'),1,isnull(bloom_filter_test(concat(\"dim2\",'-foo'),'%s')))",
                              base64,
                              base64
                          ),
                          null,
                          createMacroTable()
                      )
                  )
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
  public void testBloomFilterVirtualColumn() throws IOException
  {
    BloomKFilter filter = new BloomKFilter(1500);
    filter.addString("def-foo");
    byte[] bytes = BloomFilterSerializersModule.bloomKFilterToBytes(filter);
    String base64 = StringUtils.encodeBase64String(bytes);

    testQuery(
        StringUtils.format("SELECT COUNT(*) FROM druid.foo WHERE bloom_filter_test(concat(dim1, '-foo'), '%s')", base64),
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(expressionVirtualColumn("v0", "concat(\"dim1\",'-foo')", ColumnType.STRING))
                  .filters(
                      new BloomDimFilter("v0", BloomKFilterHolder.fromBloomKFilter(filter), null)
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
  public void testBloomFilterVirtualColumnNumber() throws IOException
  {
    BloomKFilter filter = new BloomKFilter(1500);
    filter.addFloat(20.2f);
    byte[] bytes = BloomFilterSerializersModule.bloomKFilterToBytes(filter);
    String base64 = StringUtils.encodeBase64String(bytes);

    testQuery(
        StringUtils.format("SELECT COUNT(*) FROM druid.foo WHERE bloom_filter_test(2 * CAST(dim1 AS float), '%s')", base64),
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(2 * CAST(\"dim1\", 'DOUBLE'))", ColumnType.FLOAT)
                  )
                  .filters(
                      new BloomDimFilter("v0", BloomKFilterHolder.fromBloomKFilter(filter), null)
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
  public void testBloomFilters() throws IOException
  {
    BloomKFilter filter = new BloomKFilter(1500);
    filter.addString("def");
    BloomKFilter filter2 = new BloomKFilter(1500);
    filter.addString("abc");
    byte[] bytes = BloomFilterSerializersModule.bloomKFilterToBytes(filter);
    byte[] bytes2 = BloomFilterSerializersModule.bloomKFilterToBytes(filter2);
    String base64 = StringUtils.encodeBase64String(bytes);
    String base642 = StringUtils.encodeBase64String(bytes2);

    testQuery(
        StringUtils.format("SELECT COUNT(*) FROM druid.foo WHERE bloom_filter_test(dim1, '%s') OR bloom_filter_test(dim2, '%s')", base64, base642),
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      new OrDimFilter(
                          new BloomDimFilter("dim1", BloomKFilterHolder.fromBloomKFilter(filter), null),
                          new BloomDimFilter("dim2", BloomKFilterHolder.fromBloomKFilter(filter2), null)
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

  @Ignore("this test is really slow and is intended to use for comparisons with testBloomFilterBigParameter")
  @Test
  public void testBloomFilterBigNoParam() throws IOException
  {
    BloomKFilter filter = new BloomKFilter(5_000_000);
    filter.addString("def");
    byte[] bytes = BloomFilterSerializersModule.bloomKFilterToBytes(filter);
    String base64 = StringUtils.encodeBase64String(bytes);
    testQuery(
        StringUtils.format("SELECT COUNT(*) FROM druid.foo WHERE bloom_filter_test(dim1, '%s')", base64),
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      new BloomDimFilter("dim1", BloomKFilterHolder.fromBloomKFilter(filter), null)
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

  @Ignore("this test is for comparison with testBloomFilterBigNoParam")
  @Test
  public void testBloomFilterBigParameter() throws IOException
  {
    BloomKFilter filter = new BloomKFilter(5_000_000);
    filter.addString("def");
    byte[] bytes = BloomFilterSerializersModule.bloomKFilterToBytes(filter);
    String base64 = StringUtils.encodeBase64String(bytes);
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE bloom_filter_test(dim1, ?)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      new BloomDimFilter("dim1", BloomKFilterHolder.fromBloomKFilter(filter), null)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        ),
        ImmutableList.of(new SqlParameter(SqlType.VARCHAR, base64))
    );
  }

  @Test
  public void testBloomFilterNullParameter() throws IOException
  {
    BloomKFilter filter = new BloomKFilter(1500);
    filter.addBytes(null, 0, 0);
    byte[] bytes = BloomFilterSerializersModule.bloomKFilterToBytes(filter);
    String base64 = StringUtils.encodeBase64String(bytes);

    // bloom filter expression is evaluated and optimized out at planning time since parameter is null and null matches
    // the supplied filter of the other parameter
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE bloom_filter_test(?, ?)",
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
        ),
        // there are no empty strings in the druid expression language since empty is coerced into a null when parsed
        ImmutableList.of(new SqlParameter(SqlType.VARCHAR, NullHandling.defaultStringValue()), new SqlParameter(SqlType.VARCHAR, base64))
    );
  }
}
