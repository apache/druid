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

package org.apache.druid.query.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.LegacyDimensionSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SearchQueryTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query query = Druids.newSearchQueryBuilder()
                        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                        .granularity(QueryRunnerTestHelper.ALL_GRAN)
                        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                        .query("a")
                        .build();

    String json = JSON_MAPPER.writeValueAsString(query);
    Query serdeQuery = JSON_MAPPER.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

  @Test
  public void testEquals()
  {
    Query query1 = Druids.newSearchQueryBuilder()
                         .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                         .granularity(QueryRunnerTestHelper.ALL_GRAN)
                         .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                         .dimensions(
                             new DefaultDimensionSpec(
                                 QueryRunnerTestHelper.QUALITY_DIMENSION,
                                 QueryRunnerTestHelper.QUALITY_DIMENSION
                             )
                         )
                         .query("a")
                         .build();
    Query query2 = Druids.newSearchQueryBuilder()
                         .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                         .granularity(QueryRunnerTestHelper.ALL_GRAN)
                         .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                         .dimensions(
                             new DefaultDimensionSpec(
                                 QueryRunnerTestHelper.QUALITY_DIMENSION,
                                 QueryRunnerTestHelper.QUALITY_DIMENSION
                             )
                         )
                         .query("a")
                         .build();

    Assert.assertEquals(query1, query2);
  }

  @Test
  public void testSerDe() throws IOException
  {
    Query query = Druids.newSearchQueryBuilder()
                        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                        .granularity(QueryRunnerTestHelper.ALL_GRAN)
                        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                        .dimensions(new LegacyDimensionSpec(QueryRunnerTestHelper.QUALITY_DIMENSION))
                        .query("a")
                        .build();
    final String json =
        "{\"queryType\":\"search\",\"dataSource\":{\"type\":\"table\",\"name\":\"testing\"},\"filter\":null,\"granularity\":{\"type\":\"all\"},\"limit\":1000,\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z\"]},\"searchDimensions\":[\""
        + QueryRunnerTestHelper.QUALITY_DIMENSION
        + "\"],\"query\":{\"type\":\"insensitive_contains\",\"value\":\"a\"},\"sort\":{\"type\":\"lexicographic\"},\"context\":null}";
    final Query serdeQuery = JSON_MAPPER.readValue(json, Query.class);
    Assert.assertEquals(query.toString(), serdeQuery.toString());
    Assert.assertEquals(query, serdeQuery);

    final String json2 =
        "{\"queryType\":\"search\",\"dataSource\":{\"type\":\"table\",\"name\":\"testing\"},\"filter\":null,\"granularity\":{\"type\":\"all\"},\"limit\":1000,\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z\"]},\"searchDimensions\":[\"quality\"],\"query\":{\"type\":\"insensitive_contains\",\"value\":\"a\"},\"sort\":{\"type\":\"lexicographic\"},\"context\":null}";
    final Query serdeQuery2 = JSON_MAPPER.readValue(json2, Query.class);

    Assert.assertEquals(query.toString(), serdeQuery2.toString());
    Assert.assertEquals(query, serdeQuery2);
  }
}
