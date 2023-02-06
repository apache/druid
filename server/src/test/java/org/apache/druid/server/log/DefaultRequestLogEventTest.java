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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    String expected = "{\"feed\":\"feed\",\"query\":{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"dummy\"},"
        + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z\"]},"
        + "\"descending\":true,\"granularity\":{\"type\":\"all\"},\"limit\":5,"
        + "\"context\":{\"key\":\"value\"}},\"host\":\"127.0.0.1\",\"timestamp\":\"2019-12-12T03:01:00.000Z\","
        + "\"service\":\"druid-service\",\"sql\":null,\"sqlQueryContext\":{},\"remoteAddr\":\"127.0.0.1\","
        + "\"queryStats\":{\"query/time\":13,\"query/bytes\":10,\"success\":true,\"identity\":\"allowAll\"}}";
    Assert.assertEquals(objectMapper.readTree(expected), objectMapper.readTree(logEventJson));
  }

  @Test
  public void testDefaultRequestLogEventToMap()
  {
    final String feed = "test";
    final DateTime timestamp = DateTimes.of(2019, 12, 12, 3, 1);
    final String service = "druid-service";
    final String host = "127.0.0.1";
    final Query query = new TimeseriesQuery(
        new TableDataSource("dummy"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
        true,
        VirtualColumns.EMPTY,
        null,
        Granularities.ALL,
        ImmutableList.of(),
        ImmutableList.of(),
        5,
        ImmutableMap.of("key", "value"));
    final QueryStats queryStats = new QueryStats(
        ImmutableMap.of("query/time", 13L, "query/bytes", 10L, "success", true, "identity", "allowAll"));
    RequestLogLine nativeLine = RequestLogLine.forNative(
        query,
        timestamp,
        host,
        queryStats
    );

    DefaultRequestLogEvent defaultRequestLogEvent = new DefaultRequestLogEvent(
        ImmutableMap.of("service", service, "host", host), feed, nativeLine
    );
    final Map<String, Object> expected = new HashMap<>();
    expected.put("feed", feed);
    expected.put("timestamp", timestamp);
    expected.put("service", service);
    expected.put("host", host);
    expected.put("query", query);
    expected.put("remoteAddr", host);
    expected.put("queryStats", queryStats);

    Assert.assertEquals(expected, defaultRequestLogEvent.toMap());
  }

  @Test
  public void testDefaultRequestLogEventToMapSQL()
  {
    final String feed = "test";
    final DateTime timestamp = DateTimes.of(2019, 12, 12, 3, 1);
    final String service = "druid-service";
    final String host = "127.0.0.1";
    final String sql = "select * from 1337";
    final QueryStats queryStats = new QueryStats(
        ImmutableMap.of(
            "sqlQuery/time", 13L,
            "sqlQuery/planningTimeMs", 1L,
            "sqlQuery/bytes", 10L,
            "success", true,
            "identity", "allowAll"
        )
    );

    RequestLogLine nativeLine = RequestLogLine.forSql(
        sql,
        ImmutableMap.of(),
        timestamp,
        host,
        queryStats
    );

    DefaultRequestLogEvent defaultRequestLogEvent = new DefaultRequestLogEvent(
        ImmutableMap.of("service", service, "host", host), feed, nativeLine
    );
    final Map<String, Object> expected = new HashMap<>();
    expected.put("feed", feed);
    expected.put("timestamp", timestamp);
    expected.put("service", service);
    expected.put("host", host);
    expected.put("sql", sql);
    expected.put("sqlQueryContext", ImmutableMap.of());
    expected.put("remoteAddr", host);
    expected.put("queryStats", queryStats);

    Assert.assertEquals(expected, defaultRequestLogEvent.toMap());
  }

  @Test
  public void testSerializeSqlLogRequestMap() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    String timestamp = "2022-08-17T18:51:00.000Z";

    Event event = DefaultRequestLogEventBuilderFactory.instance()
                                                      .createRequestLogEventBuilder(
                                                          "requests",
                                                          RequestLogLine.forSql(
                                                              "SELECT * FROM dummy",
                                                              Collections.emptyMap(),
                                                              DateTimes.of(timestamp),
                                                              "127.0.0.1",
                                                              new QueryStats(ImmutableMap.of())
                                                          )
                                                      )
                                                      .build("my-service", "my-host");

    String actual = mapper.writeValueAsString(event.toMap());
    String expected = "{"
                      + "\"feed\":\"requests\","
                      + "\"timestamp\":\""
                      + timestamp
                      + "\","
                      + "\"service\":\"my-service\","
                      + "\"host\":\"my-host\","
                      + "\"sql\":\"SELECT * FROM dummy\","
                      + "\"sqlQueryContext\":{},"
                      + "\"queryStats\":{},"
                      + "\"remoteAddr\":\"127.0.0.1\""
                      + "}";

    Assert.assertEquals(mapper.readTree(expected), mapper.readTree(actual));
  }

  @Test
  public void testSerializeNativeLogRequestMap() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    RequestLogLine nativeLine = RequestLogLine.forNative(
        new TimeseriesQuery(
            new TableDataSource("dummy"),
            new MultipleIntervalSegmentSpec(
                ImmutableList.of(Intervals.of(
                    "2015-01-01/2015-01-02"))),
            true,
            VirtualColumns.EMPTY,
            null,
            Granularities.ALL,
            ImmutableList.of(),
            ImmutableList.of(),
            5,
            ImmutableMap.of("key", "value")
        ),
        DateTimes.of(2019, 12, 12, 3, 1),
        "127.0.0.1",
        new QueryStats(ImmutableMap.of(
            "query/time",
            13L,
            "query/bytes",
            10L,
            "success",
            true,
            "identity",
            "allowAll"
        ))
    );

    Event event = DefaultRequestLogEventBuilderFactory.instance()
                                                      .createRequestLogEventBuilder("my-feed", nativeLine)
                                                      .build("my-service", "my-host");

    String actual = mapper.writeValueAsString(event.toMap());
    String queryString = "{"
                         + "\"queryType\":\"timeseries\","
                         + "\"dataSource\":{\"type\":\"table\",\"name\":\"dummy\"},"
                         + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z\"]},"
                         + "\"descending\":true,"
                         + "\"granularity\":{\"type\":\"all\"},"
                         + "\"limit\":5,"
                         + "\"context\":{\"key\":\"value\"}"
                         + "}";

    String expected = "{"
                      + "\"feed\":\"my-feed\","
                      + "\"host\":\"my-host\","
                      + "\"service\":\"my-service\","
                      + "\"timestamp\":\"2019-12-12T03:01:00.000Z\","
                      + "\"query\":"
                      + queryString
                      + ","
                      + "\"remoteAddr\":\"127.0.0.1\","
                      + "\"queryStats\":{\"query/time\":13,\"query/bytes\":10,\"success\":true,\"identity\":\"allowAll\"}}";

    Assert.assertEquals(mapper.readTree(expected), mapper.readTree(actual));
  }

  @Test
  public void testSerializeNativeLogRequestMapWithAdditionalParameters() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();


    RequestLogLine nativeLine = RequestLogLine.forNative(
        new TimeseriesQuery(
            new TableDataSource("dummy"),
            new MultipleIntervalSegmentSpec(
                ImmutableList.of(Intervals.of(
                    "2015-01-01/2015-01-02"))),
            true,
            VirtualColumns.EMPTY,
            null,
            Granularities.ALL,
            ImmutableList.of(),
            ImmutableList.of(),
            5,
            ImmutableMap.of("key", "value")
        ),
        DateTimes.of(2019, 12, 12, 3, 1),
        "127.0.0.1",
        new QueryStats(ImmutableMap.of(
            "query/time",
            13L,
            "query/bytes",
            10L,
            "success",
            true,
            "identity",
            "allowAll"
        ))
    );

    Event event = DefaultRequestLogEventBuilderFactory.instance()
                                                      .createRequestLogEventBuilder("my-feed", nativeLine)
                                                      .build("my-service", "my-host");

    EventMap map = EventMap.builder()
                           .putNonNull("number", 1)
                           .putNonNull("text", "some text")
                           .putNonNull("null", null)
                           .putAll(event.toMap())
                           .build();

    String actual = mapper.writeValueAsString(map);
    String queryString = "{"
                         + "\"queryType\":\"timeseries\","
                         + "\"dataSource\":{\"type\":\"table\",\"name\":\"dummy\"},"
                         + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z\"]},"
                         + "\"descending\":true,"
                         + "\"granularity\":{\"type\":\"all\"},"
                         + "\"limit\":5,"
                         + "\"context\":{\"key\":\"value\"}"
                         + "}";

    String expected = "{"
                      + "\"feed\":\"my-feed\","
                      + "\"host\":\"my-host\","
                      + "\"service\":\"my-service\","
                      + "\"timestamp\":\"2019-12-12T03:01:00.000Z\","
                      + "\"query\":"
                      + queryString
                      + ","
                      + "\"remoteAddr\":\"127.0.0.1\","
                      + "\"number\":1,"
                      + "\"text\":\"some text\","
                      + "\"queryStats\":{\"query/time\":13,\"query/bytes\":10,\"success\":true,\"identity\":\"allowAll\"}}";

    Assert.assertEquals(mapper.readTree(expected), mapper.readTree(actual));
  }

}
