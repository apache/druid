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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.timgroup.statsd.StatsDClient;
import io.druid.emitter.statsd.StatsDEmitter;
import io.druid.emitter.statsd.StatsDEmitterConfig;
import io.druid.java.util.common.DateTimes;
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
        new StatsDEmitterConfig("localhost", 8888, null, null, null, null),
        new ObjectMapper(),
        client
    );
    client.gauge("broker.query.cache.total.hitRate", 54);
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
        new StatsDEmitterConfig("localhost", 8888, null, null, null, null),
        new ObjectMapper(),
        client
    );
    client.time("broker.query.time.data-source.groupBy", 10);
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
        new StatsDEmitterConfig("localhost", 8888, null, "#", true, null),
        new ObjectMapper(),
        client
    );
    client.time("brokerHost1#broker#query#time#data-source#groupBy", 10);
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
}
