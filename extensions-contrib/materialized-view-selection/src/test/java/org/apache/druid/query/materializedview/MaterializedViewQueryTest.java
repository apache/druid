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

package org.apache.druid.query.materializedview;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MaterializedViewQueryTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();
  private DataSourceOptimizer optimizer;

  @Before
  public void setUp()
  {
    JSON_MAPPER.registerSubtypes(new NamedType(MaterializedViewQuery.class, MaterializedViewQuery.TYPE));
    optimizer = EasyMock.createMock(DataSourceOptimizer.class);
    JSON_MAPPER.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), LookupEnabledTestExprMacroTable.INSTANCE)
            .addValue(DataSourceOptimizer.class, optimizer)
    );
  }

  @Test
  public void testQuerySerialization() throws IOException
  {
    TopNQuery topNQuery = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();
    MaterializedViewQuery query = new MaterializedViewQuery(topNQuery, optimizer);
    String json = JSON_MAPPER.writeValueAsString(query);
    Query serdeQuery = JSON_MAPPER.readValue(json, Query.class);
    Assert.assertEquals(query, serdeQuery);
    Assert.assertEquals(new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE), query.getDataSource());
    Assert.assertEquals(QueryRunnerTestHelper.ALL_GRAN, query.getGranularity());
    Assert.assertEquals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC.getIntervals(), query.getIntervals());
  }
}
