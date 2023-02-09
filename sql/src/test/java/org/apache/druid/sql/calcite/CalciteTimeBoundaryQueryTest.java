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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.junit.Test;

import java.util.HashMap;

public class CalciteTimeBoundaryQueryTest extends BaseCalciteQueryTest
{
  // __time for foo is [2000-01-01, 2000-01-02, 2000-01-03, 2001-01-01, 2001-01-02, 2001-01-03]
  @Test
  public void testMaxTimeQuery()
  {
    HashMap<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    HashMap<String, Object> expectedContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    expectedContext.put(TimeBoundaryQuery.MAX_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT MAX(__time) AS maxTime FROM foo",
        queryContext,
        ImmutableList.of(
            Druids.newTimeBoundaryQueryBuilder()
                  .dataSource("foo")
                  .bound(TimeBoundaryQuery.MAX_TIME)
                  .context(expectedContext)
                  .build()
        ),
        ImmutableList.of(new Object[]{DateTimes.of("2001-01-03").getMillis()})
    );
  }

  @Test
  public void testMinTimeQuery()
  {
    HashMap<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    HashMap<String, Object> expectedContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    expectedContext.put(TimeBoundaryQuery.MIN_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT MIN(__time) AS minTime FROM foo",
        queryContext,
        ImmutableList.of(
            Druids.newTimeBoundaryQueryBuilder()
                  .dataSource("foo")
                  .bound(TimeBoundaryQuery.MIN_TIME)
                  .context(expectedContext)
                  .build()
        ),
        ImmutableList.of(new Object[]{DateTimes.of("2000-01-01").getMillis()})
    );
  }

  @Test
  public void testMinTimeQueryWithFilters()
  {
    HashMap<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    HashMap<String, Object> expectedContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    expectedContext.put(TimeBoundaryQuery.MIN_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT MIN(__time) AS minTime FROM foo where __time >= '2001-01-01' and __time < '2003-01-01'",
        queryContext,
        ImmutableList.of(
            Druids.newTimeBoundaryQueryBuilder()
                  .dataSource("foo")
                  .intervals(
                      new MultipleIntervalSegmentSpec(
                          ImmutableList.of(Intervals.of("2001-01-01T00:00:00.000Z/2003-01-01T00:00:00.000Z"))
                      )
                  )
                  .bound(TimeBoundaryQuery.MIN_TIME)
                  .context(expectedContext)
                  .build()
        ),
        ImmutableList.of(new Object[]{DateTimes.of("2001-01-01").getMillis()})
    );
  }

  // Currently, if both min(__time) and max(__time) are present, we don't convert it
  // to a timeBoundary query. (ref : https://github.com/apache/druid/issues/12479)
  @Test
  public void testMinMaxTimeQuery()
  {
    HashMap<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    context.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    testQuery(
        "SELECT MIN(__time) AS minTime, MAX(__time) as maxTime FROM foo",
        context,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource("foo")
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .aggregators(
                      new LongMinAggregatorFactory("a0", "__time"),
                      new LongMaxAggregatorFactory("a1", "__time")
                  )
                  .context(context)
                  .build()
        ),
        ImmutableList.of(new Object[]{
            DateTimes.of("2000-01-01").getMillis(),
            DateTimes.of("2001-01-03").getMillis()
        })
    );
  }
}
