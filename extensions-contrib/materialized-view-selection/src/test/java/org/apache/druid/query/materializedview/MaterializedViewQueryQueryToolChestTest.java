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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Druids;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class MaterializedViewQueryQueryToolChestTest
{
  @Test
  public void testMakePostComputeManipulatorFn()
  {
    TimeseriesQuery realQuery = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                        .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                        .aggregators(QueryRunnerTestHelper.ROWS_COUNT)
                                        .descending(true)
                                        .build();
    MaterializedViewQuery materializedViewQuery = new MaterializedViewQuery(realQuery, null);

    QueryToolChest materializedViewQueryQueryToolChest =
        new MaterializedViewQueryQueryToolChest(new MapQueryToolChestWarehouse(
            ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
                .put(
                    TimeseriesQuery.class,
                    new TimeseriesQueryQueryToolChest(
                        QueryRunnerTestHelper.noopIntervalChunkingQueryRunnerDecorator()
                    )
                )
                .build()
        ));

    Function postFn =
        materializedViewQueryQueryToolChest.makePostComputeManipulatorFn(
          materializedViewQuery,
          new MetricManipulationFn() {
            @Override
            public Object manipulate(AggregatorFactory factory, Object object)
            {
              return "metricvalue1";
            }
          });



    Result<TimeseriesResultValue> result = new Result<>(
        DateTimes.nowUtc(),
        new TimeseriesResultValue(ImmutableMap.of("dim1", "dimvalue1"))
    );

    Result<TimeseriesResultValue> postResult = (Result<TimeseriesResultValue>) postFn.apply(result);
    Map<String, Object> postResultMap = postResult.getValue().getBaseObject();

    Assert.assertEquals(postResult.getTimestamp(), result.getTimestamp());
    Assert.assertEquals(postResultMap.size(), 2);
    Assert.assertEquals(postResultMap.get(QueryRunnerTestHelper.ROWS_COUNT.getName()), "metricvalue1");
    Assert.assertEquals(postResultMap.get("dim1"), "dimvalue1");
  }
}
