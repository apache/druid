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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.BloomFilterExtensionModule;
import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.expressions.BloomFilterExprMacro;
import org.apache.druid.query.filter.BloomDimFilter;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.filter.BloomKFilterHolder;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BloomDimFilterSqlTest extends BaseCalciteQueryTest
{
  private static final Injector INJECTOR = Guice.createInjector(
      binder -> {
        binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(TestHelper.makeJsonMapper());
        binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(
            LookupEnabledTestExprMacroTable.createTestLookupReferencesManager(
                ImmutableMap.of(
                    "a", "xa",
                    "abc", "xabc"
                )
            )
        );
      },
      new BloomFilterExtensionModule()
  );

  private static ObjectMapper jsonMapper =
      INJECTOR
          .getInstance(Key.get(ObjectMapper.class, Json.class))
          .registerModules(Collections.singletonList(new BloomFilterSerializersModule()));

  public static ExprMacroTable createExprMacroTable()
  {
    final List<ExprMacroTable.ExprMacro> exprMacros = new ArrayList<>();
    for (Class<? extends ExprMacroTable.ExprMacro> clazz : ExpressionModule.EXPR_MACROS) {
      exprMacros.add(INJECTOR.getInstance(clazz));
    }
    exprMacros.add(INJECTOR.getInstance(BloomFilterExprMacro.class));
    exprMacros.add(INJECTOR.getInstance(LookupExprMacro.class));
    return new ExprMacroTable(exprMacros);
  }

  @Rule
  @Override
  public QueryLogHook getQueryLogHook()
  {
    return queryLogHook = QueryLogHook.create(jsonMapper);
  }

  @Test
  public void testBloomFilter() throws Exception
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testBloomFilterExprFilter() throws Exception
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
        StringUtils.format("SELECT COUNT(*) FROM druid.foo WHERE bloom_filter_test(concat(dim2, '-foo'), '%s') = TRUE", base64),
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      new ExpressionDimFilter(
                          StringUtils.format("(bloom_filter_test(concat(\"dim2\",'-foo'),'%s') == 1)", base64),
                          createExprMacroTable()
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testBloomFilterVirtualColumn() throws Exception
  {
    // Cannot vectorize due to expression virtual columns.
    cannotVectorize();

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
                  .virtualColumns(expressionVirtualColumn("v0", "concat(\"dim1\",'-foo')", ValueType.STRING))
                  .filters(
                      new BloomDimFilter("v0", BloomKFilterHolder.fromBloomKFilter(filter), null)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }


  @Test
  public void testBloomFilterVirtualColumnNumber() throws Exception
  {
    // Cannot vectorize due to expression virtual columns.
    cannotVectorize();

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
                      expressionVirtualColumn("v0", "(2 * CAST(\"dim1\", 'DOUBLE'))", ValueType.FLOAT)
                  )
                  .filters(
                      new BloomDimFilter("v0", BloomKFilterHolder.fromBloomKFilter(filter), null)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testBloomFilters() throws Exception
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Override
  public List<Object[]> getResults(
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final String sql,
      final AuthenticationResult authenticationResult
  ) throws Exception
  {
    final DruidOperatorTable operatorTable = new DruidOperatorTable(
        ImmutableSet.of(),
        ImmutableSet.of(INJECTOR.getInstance(BloomFilterOperatorConversion.class))
    );
    return getResults(
        plannerConfig,
        queryContext,
        sql,
        authenticationResult,
        operatorTable,
        createExprMacroTable(),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        jsonMapper
    );
  }
}
