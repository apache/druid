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

package org.apache.druid.emitter.statsd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.timgroup.statsd.Event;
import com.timgroup.statsd.StatsDClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.service.AlertBuilder;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class StatsDEmitterTest
{
  @Test
  public void testConvertRange()
  {
    StatsDClient client = mock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, null, null, null, null, null, null, null, null),
        new ObjectMapper(),
        client
    );
    client.gauge("broker.query.cache.total.hitRate", 54);
    emitter.emit(new ServiceMetricEvent.Builder()
                     .setDimension("dataSource", "data-source")
                     .build(DateTimes.nowUtc(), "query/cache/total/hitRate", 0.54)
                     .build("broker", "brokerHost1")
    );
  }

  @Test
  public void testConvertRangeWithDogstatsd()
  {
    StatsDClient client = mock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, null, null, null, null, true, null, null, null),
        new ObjectMapper(),
        client
    );
    client.gauge("broker.query.cache.total.hitRate", 0.54);
    emitter.emit(new ServiceMetricEvent.Builder()
                     .setDimension("dataSource", "data-source")
                     .build(DateTimes.nowUtc(), "query/cache/total/hitRate", 0.54)
                     .build("broker", "brokerHost1")
    );
  }

  @Test
  public void testNoConvertRange()
  {
    StatsDClient client = mock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, null, null, null, null, null, null, null, null),
        new ObjectMapper(),
        client
    );
    client.time("broker.query.time.data-source.groupBy", 10);
    emitter.emit(new ServiceMetricEvent.Builder()
                     .setDimension("dataSource", "data-source")
                     .setDimension("type", "groupBy")
                     .setDimension("interval", "2013/2015")
                     .setDimension("some_random_dim1", "random_dim_value1")
                     .setDimension("some_random_dim2", "random_dim_value2")
                     .setDimension("hasFilters", "no")
                     .setDimension("duration", "P1D")
                     .setDimension("remoteAddress", "194.0.90.2")
                     .setDimension("id", "ID")
                     .setDimension("context", "{context}")
                     .build(DateTimes.nowUtc(), "query/time", 10)
                     .build("broker", "brokerHost1")
    );
  }

  @Test
  public void testConfigOptions()
  {
    StatsDClient client = mock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, "#", true, null, null, null, null, null, null),
        new ObjectMapper(),
        client
    );
    client.time("brokerHost1#broker#query#time#data-source#groupBy", 10);
    emitter.emit(new ServiceMetricEvent.Builder()
                     .setDimension("dataSource", "data-source")
                     .setDimension("type", "groupBy")
                     .setDimension("interval", "2013/2015")
                     .setDimension("some_random_dim1", "random_dim_value1")
                     .setDimension("some_random_dim2", "random_dim_value2")
                     .setDimension("hasFilters", "no")
                     .setDimension("duration", "P1D")
                     .setDimension("remoteAddress", "194.0.90.2")
                     .setDimension("id", "ID")
                     .setDimension("context", "{context}")
                     .build(DateTimes.nowUtc(), "query/time", 10)
                     .build("broker", "brokerHost1")
    );
  }

  @Test
  public void testDogstatsdEnabled()
  {
    StatsDClient client = mock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, "#", true, null, null, true, null, null, null),
        new ObjectMapper(),
        client
    );
    client.time("broker#query#time", 10,
                "dataSource:data-source", "type:groupBy", "hostname:brokerHost1"
    );
    emitter.emit(new ServiceMetricEvent.Builder()
                     .setDimension("dataSource", "data-source")
                     .setDimension("type", "groupBy")
                     .setDimension("interval", "2013/2015")
                     .setDimension("some_random_dim1", "random_dim_value1")
                     .setDimension("some_random_dim2", "random_dim_value2")
                     .setDimension("hasFilters", "no")
                     .setDimension("duration", "P1D")
                     .setDimension("remoteAddress", "194.0.90.2")
                     .setDimension("id", "ID")
                     .setDimension("context", "{context}")
                     .build(DateTimes.nowUtc(), "query/time", 10)
                     .build("broker", "brokerHost1")
    );
  }

  @Test
  public void testBlankHolderOptions()
  {
    StatsDClient client = mock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, null, true, null, null, null, null, null, null),
        new ObjectMapper(),
        client
    );
    client.count("brokerHost1.broker.jvm.gc.count.G1-GC", 1);
    emitter.emit(new ServiceMetricEvent.Builder()
                     .setDimension("gcName", "G1 GC")
                     .build(DateTimes.nowUtc(), "jvm/gc/count", 1)
                     .build("broker", "brokerHost1")
    );
  }

  @Test
  public void testServiceAsTagOption()
  {
    StatsDClient client = mock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
            new StatsDEmitterConfig("localhost", 8888, null, null, true, null, null, true, null, true, null),
            new ObjectMapper(),
            client
    );
    client.time("druid.query.time", 10,
            "druid_service:druid/broker", "dataSource:data-source", "type:groupBy", "hostname:brokerHost1"
    );
    emitter.emit(new ServiceMetricEvent.Builder()
            .setDimension("dataSource", "data-source")
            .setDimension("type", "groupBy")
            .build(DateTimes.nowUtc(), "query/time", 10)
            .build("druid/broker", "brokerHost1")
    );
  }

  @Test
  public void testAlertEvent()
  {
    StatsDClient client = mock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, null, true, null, null, true, null, true, true),
        new ObjectMapper(),
        client
    );
    Event expectedEvent = Event
        .builder()
        .withPriority(Event.Priority.NORMAL)
        .withAlertType(Event.AlertType.WARNING)
        .withTitle("something bad happened [exception]")
        .withText("{\"exception\":\"NPE\"}")
        .build();

    emitter.emit(AlertBuilder.create("something bad happened [%s]", "exception")
                             .severity(AlertEvent.Severity.ANOMALY)
                             .addData(ImmutableMap.of("exception", "NPE"))
                             .build("druid/broker", "brokerHost1")
    );

    final ArgumentCaptor<Event> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
    verify(client).recordEvent(
        eventArgumentCaptor.capture(),
        eq("feed:alerts"), eq("druid_service:druid/broker"),
        eq("severity:anomaly"), eq("hostname:brokerHost1")
    );

    Event actualEvent = eventArgumentCaptor.getValue();
    Assert.assertTrue(actualEvent.getMillisSinceEpoch() > 0);
    Assert.assertEquals(expectedEvent.getPriority(), actualEvent.getPriority());
    Assert.assertEquals(expectedEvent.getAlertType(), actualEvent.getAlertType());
    Assert.assertEquals(expectedEvent.getTitle(), actualEvent.getTitle());
    Assert.assertEquals(expectedEvent.getText(), actualEvent.getText());
  }

  @Test
  public void testInitialization()
  {
    final StatsDEmitterConfig config = new StatsDEmitterConfig(
        "localhost",
        8888,
        "druid",
        "-",
        true,
        null,
        null,
        true,
        ImmutableList.of("tag1", "value1"),
        true,
        true
    );
    try (StatsDEmitter emitter = StatsDEmitter.of(config, new ObjectMapper())) {

    }
  }

  @Test
  public void testJacksonModules()
  {
    Assert.assertTrue(new StatsDEmitterModule().getJacksonModules().isEmpty());
  }
}
