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

package org.apache.druid.server.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.junit.Assert;
import org.junit.Test;

public class DefaultRequestLogEventTest
{
  private ObjectMapper objectMapper = new DefaultObjectMapper();

  @Test
  public void testDefaultRequestLogEventSerde() throws Exception
  {
    RequestLogLine nativeLine = RequestLogLine.forNative(
            new TimeseriesQuery(
                    new TableDataSource("dummy"),
                    new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
                    true,
                    VirtualColumns.EMPTY,
                    null,
                    Granularities.ALL,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    5,
                    ImmutableMap.of("key", "value")),
            DateTimes.of(2019, 12, 12, 3, 1),
            "127.0.0.1",
            new QueryStats(ImmutableMap.of("query/time", 13L, "query/bytes", 10L, "success", true, "identity", "allowAll"))
    );

    DefaultRequestLogEvent defaultRequestLogEvent = new DefaultRequestLogEvent(
            ImmutableMap.of("service", "druid-service", "host", "127.0.0.1"),
            "feed",
            nativeLine);

    String logEventJson = objectMapper.writeValueAsString(defaultRequestLogEvent);
    String expected = "{\"feed\":\"feed\",\"query\":{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"dummy\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z\"]},\"descending\":true,\"virtualColumns\":[],\"filter\":null,\"granularity\":{\"type\":\"all\"},\"aggregations\":[],\"postAggregations\":[],\"limit\":5,\"context\":{\"key\":\"value\"}},\"host\":\"127.0.0.1\",\"timestamp\":\"2019-12-12T03:01:00.000Z\",\"service\":\"druid-service\",\"sql\":null,\"sqlQueryContext\":{},\"remoteAddr\":\"127.0.0.1\",\"queryStats\":{\"query/time\":13,\"query/bytes\":10,\"success\":true,\"identity\":\"allowAll\"}}";
    Assert.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(logEventJson));
  }
}
