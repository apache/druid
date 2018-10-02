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
import java.util.Collections;

import static org.apache.druid.query.QueryRunnerTestHelper.addRowsIndexConstant;
import static org.apache.druid.query.QueryRunnerTestHelper.allGran;
import static org.apache.druid.query.QueryRunnerTestHelper.commonDoubleAggregators;
import static org.apache.druid.query.QueryRunnerTestHelper.dataSource;
import static org.apache.druid.query.QueryRunnerTestHelper.fullOnIntervalSpec;
import static org.apache.druid.query.QueryRunnerTestHelper.indexMetric;
import static org.apache.druid.query.QueryRunnerTestHelper.marketDimension;

public class MaterializedViewQueryTest 
{
  private static final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
  private DataSourceOptimizer optimizer;

  @Before
  public void setUp() 
  {
    jsonMapper.registerSubtypes(new NamedType(MaterializedViewQuery.class, MaterializedViewQuery.TYPE));
    optimizer = EasyMock.createMock(DataSourceOptimizer.class);
    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), LookupEnabledTestExprMacroTable.INSTANCE)
            .addValue(DataSourceOptimizer.class, optimizer)
    );
  }
  
  @Test
  public void testQuerySerialization() throws IOException
  {
    TopNQuery topNQuery = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(marketDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(fullOnIntervalSpec)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    commonDoubleAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Collections.singletonList(addRowsIndexConstant))
        .build();
    MaterializedViewQuery query = new MaterializedViewQuery(topNQuery, optimizer);
    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);
    Assert.assertEquals(query, serdeQuery);
    Assert.assertEquals(new TableDataSource(dataSource), query.getDataSource());
    Assert.assertEquals(allGran, query.getGranularity());
    Assert.assertEquals(fullOnIntervalSpec.getIntervals(), query.getIntervals());
  }
}
