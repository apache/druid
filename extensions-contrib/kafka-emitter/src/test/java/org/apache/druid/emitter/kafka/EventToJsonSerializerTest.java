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

package org.apache.druid.emitter.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import junit.framework.TestCase;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.log.DefaultRequestLogEventBuilderFactory;
import org.joda.time.DateTime;

import java.util.Collections;

public class EventToJsonSerializerTest extends TestCase
{

  public void testSerializeServiceMetricEvent() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    EventToJsonSerializer serializer = EventToJsonSerializer.of(mapper);
    DateTime timestamp = DateTimes.nowUtc();
    Event event = ServiceMetricEvent.builder()
                                    .setFeed("my-feed")
                                    .build(timestamp, "m1", 1)
                                    .build("my-service", "my-host");

    String actual = serializer.serialize(event);
    String expected = "{"
                      + "\"feed\":\"my-feed\","
                      + "\"timestamp\":\""
                      + timestamp
                      + "\","
                      + "\"metric\":\"m1\","
                      + "\"value\":1,"
                      + "\"service\":\"my-service\","
                      + "\"host\":\"my-host\""
                      + "}";
    assertEquals(mapper.readTree(expected), mapper.readTree(actual));
  }

  public void testSerializeAlertEvent() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    EventToJsonSerializer serializer = EventToJsonSerializer.of(mapper);
    DateTime timestamp = DateTimes.nowUtc();
    Event event = new AlertEvent(
        timestamp,
        "my-service",
        "my-host",
        AlertEvent.Severity.DEFAULT,
        "my-description",
        Collections.emptyMap()
    );

    String actual = serializer.serialize(event);
    String expected = "{"
                      + "\"feed\":\"alerts\","
                      + "\"timestamp\":\""
                      + timestamp
                      + "\","
                      + "\"severity\":\""
                      + AlertEvent.Severity.DEFAULT
                      + "\","
                      + "\"service\":\"my-service\","
                      + "\"host\":\"my-host\","
                      + "\"description\":\"my-description\","
                      + "\"data\":{}"
                      + "}";
    assertEquals(mapper.readTree(expected), mapper.readTree(actual));
  }

  public void testSerializeSqlLogRequest() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    EventToJsonSerializer serializer = EventToJsonSerializer.of(mapper);
    DateTime timestamp = DateTimes.nowUtc();

    Event event = DefaultRequestLogEventBuilderFactory.instance()
                                                      .createRequestLogEventBuilder(
                                                          "requests",
                                                          RequestLogLine.forSql(
                                                              "SELECT * FROM dummy",
                                                              Collections.emptyMap(),
                                                              timestamp,
                                                              "127.0.0.1",
                                                              new QueryStats(ImmutableMap.of())
                                                          )
                                                      )
                                                      .build("my-service", "my-host");

    String actual = serializer.serialize(event);
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

    assertEquals(mapper.readTree(expected), mapper.readTree(actual));
  }

  public void testSerializeNativeLogRequest() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    EventToJsonSerializer serializer = EventToJsonSerializer.of(mapper);

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

    String actual = serializer.serialize(event);
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

    assertEquals(mapper.readTree(expected), mapper.readTree(actual));
  }

  public void testSerializeNativeLogRequestWithAdditionalParameters() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    EventToJsonSerializer serializer = EventToJsonSerializer.of(mapper)
                                                            .withProperty("number", 1)
                                                            .withProperty("text", "some text")
                                                            .withProperty("null", null);

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

    String actual = serializer.serialize(event);
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

    assertEquals(mapper.readTree(expected), mapper.readTree(actual));
  }
}
