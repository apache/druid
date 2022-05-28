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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Druids;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.GroupByQueryRunnerTestHelper;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MaterializedViewQueryQueryToolChestTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

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
                .put(TimeseriesQuery.class, new TimeseriesQueryQueryToolChest())
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

  @Test
  public void testDecorateObjectMapper() throws IOException
  {
    GroupByQuery realQuery = GroupByQuery.builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, false))
        .build();

    QueryToolChest queryToolChest =
        new MaterializedViewQueryQueryToolChest(new MapQueryToolChestWarehouse(
            ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
                .put(GroupByQuery.class, new GroupByQueryQueryToolChest(null))
                .build()
        ));

    ObjectMapper objectMapper = queryToolChest.decorateObjectMapper(JSON_MAPPER, realQuery);

    List<ResultRow> results = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            realQuery,
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx",
            135L

        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            realQuery,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            118L
        )
    );
    List<MapBasedRow> expectedResults = results.stream()
        .map(resultRow -> resultRow.toMapBasedRow(realQuery))
        .collect(Collectors.toList());

    Assert.assertEquals(
        "decorate-object-mapper",
        JSON_MAPPER.writerFor(new TypeReference<List<MapBasedRow>>(){}).writeValueAsString(expectedResults),
        objectMapper.writeValueAsString(results)
    );
  }

  @Test
  public void testDecorateObjectMapperMaterializedViewQuery() throws IOException
  {
    GroupByQuery realQuery = GroupByQuery.builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, false))
        .build();
    MaterializedViewQuery materializedViewQuery = new MaterializedViewQuery(realQuery, null);

    QueryToolChest materializedViewQueryQueryToolChest =
        new MaterializedViewQueryQueryToolChest(new MapQueryToolChestWarehouse(
            ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
                .put(GroupByQuery.class, new GroupByQueryQueryToolChest(null))
                .build()
        ));

    ObjectMapper objectMapper = materializedViewQueryQueryToolChest.decorateObjectMapper(JSON_MAPPER, materializedViewQuery);

    List<ResultRow> results = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            realQuery,
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx",
            135L

        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            realQuery,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            118L
        )
    );
    List<MapBasedRow> expectedResults = results.stream()
        .map(resultRow -> resultRow.toMapBasedRow(realQuery))
        .collect(Collectors.toList());

    Assert.assertEquals(
        "decorate-object-mapper",
        JSON_MAPPER.writerFor(new TypeReference<List<MapBasedRow>>(){}).writeValueAsString(expectedResults),
        objectMapper.writeValueAsString(results)
    );
  }

  @Test
  public void testGetRealQuery()
  {
    GroupByQuery realQuery = GroupByQuery.builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, false))
        .build();
    MaterializedViewQuery materializedViewQuery = new MaterializedViewQuery(realQuery, null);

    MaterializedViewQueryQueryToolChest materializedViewQueryQueryToolChest =
        new MaterializedViewQueryQueryToolChest(new MapQueryToolChestWarehouse(
            ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
                .put(GroupByQuery.class, new GroupByQueryQueryToolChest(null))
                .build()
        ));

    Assert.assertEquals(realQuery, materializedViewQueryQueryToolChest.getRealQuery(materializedViewQuery));
    
  }
  
}
