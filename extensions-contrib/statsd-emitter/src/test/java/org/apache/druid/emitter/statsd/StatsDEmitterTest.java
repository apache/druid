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
import com.timgroup.statsd.StatsDClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.Test;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 */
public class StatsDEmitterTest
{
  @Test
  public void testConvertRange()
  {
    StatsDClient client = createMock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, null, null, null, null, null, null),
        new ObjectMapper(),
        client
    );
    client.gauge("broker.query.cache.total.hitRate", 54, new String[0]);
    replay(client);
    emitter.emit(new ServiceMetricEvent.Builder()
                     .setDimension("dataSource", "data-source")
                     .build(DateTimes.nowUtc(), "query/cache/total/hitRate", 0.54)
                     .build("broker", "brokerHost1")
    );
    verify(client);
  }

  @Test
  public void testConvertRangeWithDogstatsd()
  {
    StatsDClient client = createMock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, null, null, null, null, true, null),
        new ObjectMapper(),
        client
    );
    client.gauge("broker.query.cache.total.hitRate", 0.54, new String[0]);
    replay(client);
    emitter.emit(new ServiceMetricEvent.Builder()
                     .setDimension("dataSource", "data-source")
                     .build(DateTimes.nowUtc(), "query/cache/total/hitRate", 0.54)
                     .build("broker", "brokerHost1")
    );
    verify(client);
  }

  @Test
  public void testNoConvertRange()
  {
    StatsDClient client = createMock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, null, null, null, null, null, null),
        new ObjectMapper(),
        client
    );
    client.time("broker.query.time.data-source.groupBy", 10, new String[0]);
    replay(client);
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
    verify(client);
  }

  @Test
  public void testConfigOptions()
  {
    StatsDClient client = createMock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, "#", true, null, null, null, null),
        new ObjectMapper(),
        client
    );
    client.time("brokerHost1#broker#query#time#data-source#groupBy", 10, new String[0]);
    replay(client);
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
    verify(client);
  }

  @Test
  public void testDogstatsdEnabled()
  {
    StatsDClient client = createMock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, "#", true, null, null, true, null),
        new ObjectMapper(),
        client
    );
    client.time("broker#query#time", 10,
        new String[] {"dataSource:data-source", "type:groupBy", "hostname:brokerHost1"});
    replay(client);
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
    verify(client);
  }

  @Test
  public void testBlankHolderOptions()
  {
    StatsDClient client = createMock(StatsDClient.class);
    StatsDEmitter emitter = new StatsDEmitter(
        new StatsDEmitterConfig("localhost", 8888, null, null, true, null, null, null, null),
        new ObjectMapper(),
        client
    );
    client.count("brokerHost1.broker.jvm.gc.count.G1-GC", 1, new String[0]);
    replay(client);
    emitter.emit(new ServiceMetricEvent.Builder()
                     .setDimension("gcName", "G1 GC")
                     .build(DateTimes.nowUtc(), "jvm/gc/count", 1)
                     .build("broker", "brokerHost1")
    );
    verify(client);
  }
}
