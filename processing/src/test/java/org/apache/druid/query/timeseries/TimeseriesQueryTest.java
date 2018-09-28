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

package org.apache.druid.query.timeseries;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class TimeseriesQueryTest
{
  private static final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  @Parameterized.Parameters(name = "descending={0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.cartesian(Arrays.asList(false, true));
  }

  private final boolean descending;

  public TimeseriesQueryTest(boolean descending)
  {
    this.descending = descending;
  }

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query query = Druids.newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.dayGran)
        .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
        .aggregators(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexDoubleSum)
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

}
