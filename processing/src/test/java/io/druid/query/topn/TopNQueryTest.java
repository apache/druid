/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.topn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.dimension.LegacyDimensionSpec;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.ordering.StringComparators;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static io.druid.query.QueryRunnerTestHelper.addRowsIndexConstant;
import static io.druid.query.QueryRunnerTestHelper.allGran;
import static io.druid.query.QueryRunnerTestHelper.commonAggregators;
import static io.druid.query.QueryRunnerTestHelper.dataSource;
import static io.druid.query.QueryRunnerTestHelper.fullOnInterval;
import static io.druid.query.QueryRunnerTestHelper.indexMetric;
import static io.druid.query.QueryRunnerTestHelper.marketDimension;
import static io.druid.query.QueryRunnerTestHelper.rowsCount;

public class TopNQueryTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(marketDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }


  @Test
  public void testQuerySerdeWithLookupExtractionFn() throws IOException
  {
    final TopNQuery expectedQuery = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(
            new ExtractionDimensionSpec(
                marketDimension,
                marketDimension,
                new LookupExtractionFn(new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false), true, null, false, false),
                null
            )
        )
        .metric(new NumericTopNMetricSpec(indexMetric))
        .threshold(2)
        .intervals(fullOnInterval.getIntervals())
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .build();
    final String str = jsonMapper.writeValueAsString(expectedQuery);
    Assert.assertEquals(expectedQuery, jsonMapper.readValue(str, TopNQuery.class));
  }

  @Test
  public void testQuerySerdeWithAlphaNumericTopNMetricSpec() throws IOException
  {
    TopNQuery expectedQuery = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(new LegacyDimensionSpec(marketDimension))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.ALPHANUMERIC))
        .threshold(2)
        .intervals(fullOnInterval.getIntervals())
        .aggregators(Lists.<AggregatorFactory>newArrayList(rowsCount))
        .build();
    String jsonQuery = "{\n"
                       + "  \"queryType\": \"topN\",\n"
                       + "  \"dataSource\": \"testing\",\n"
                       + "  \"dimension\": \"market\",\n"
                       + "  \"threshold\": 2,\n"
                       + "  \"metric\": {\n"
                       + "    \"type\": \"dimension\",\n"
                       + "    \"ordering\": \"alphanumeric\"\n"
                       + "   },\n"
                       + "  \"granularity\": \"all\",\n"
                       + "  \"aggregations\": [\n"
                       + "    {\n"
                       + "      \"type\": \"count\",\n"
                       + "      \"name\": \"rows\"\n"
                       + "    }\n"
                       + "  ],\n"
                       + "  \"intervals\": [\n"
                       + "    \"1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z\"\n"
                       + "  ]\n"
                       + "}";
    TopNQuery actualQuery = jsonMapper.readValue(
        jsonMapper.writeValueAsString(
            jsonMapper.readValue(
                jsonQuery,
                TopNQuery.class
            )
        ), TopNQuery.class
    );
    Assert.assertEquals(expectedQuery, actualQuery);
  }

}
