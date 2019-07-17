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

package org.apache.druid.query.topn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.dimension.LegacyDimensionSpec;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TopNQueryTest
{
  private static final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.commonDoubleAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .build();

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }


  @Test
  public void testQuerySerdeWithLookupExtractionFn() throws IOException
  {
    final TopNQuery expectedQuery = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new LookupExtractionFn(
                    new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false),
                    true,
                    null,
                    false,
                    false
                )
            )
        )
        .metric(new NumericTopNMetricSpec(QueryRunnerTestHelper.indexMetric))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.fullOnIntervalSpec.getIntervals())
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.commonDoubleAggregators,
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new LegacyDimensionSpec(QueryRunnerTestHelper.marketDimension))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.ALPHANUMERIC))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.fullOnIntervalSpec.getIntervals())
        .aggregators(QueryRunnerTestHelper.rowsCount)
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
