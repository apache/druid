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

package org.apache.druid.emitter.influxdb;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InfluxdbEmitterTest
{

  private ServiceMetricEvent event;

  @Before
  public void setUp()
  {
    DateTime date = new DateTime(2017,
                                 10,
                                 30,
                                 10,
                                 00,
                                 DateTimeZone.UTC); // 10:00am on 30/10/2017 = 1509357600000000000 in epoch nanoseconds
    String metric = "metric/te/st/value";
    Number value = 1234;
    ImmutableMap<String, String> serviceDims = ImmutableMap.of(
        "service",
        "druid/historical",
        "host",
        "localhost",
        "version",
        "0.10.0"
    );
    ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
    builder.setDimension("nonWhiteListedDim", "test");
    builder.setDimension("dataSource", "test_datasource");
    ServiceEventBuilder eventBuilder = builder.build(date, metric, value);
    event = (ServiceMetricEvent) eventBuilder.build(serviceDims);
  }

  @Test
  public void testTransformForInfluxWithLongMetric()
  {
    InfluxdbEmitterConfig config = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    InfluxdbEmitter influxdbEmitter = new InfluxdbEmitter(config);
    String expected =
        "druid_metric,service=druid/historical,metric=druid_te_st,hostname=localhost,dataSource=test_datasource druid_value=1234 1509357600000000000"
        + "\n";
    String actual = influxdbEmitter.transformForInfluxSystems(event);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testTransformForInfluxWithShortMetric()
  {
    DateTime date = new DateTime(2017,
                                 10,
                                 30,
                                 10,
                                 00,
                                 DateTimeZone.UTC); // 10:00am on 30/10/2017 = 1509357600000000000 in epoch nanoseconds
    String metric = "metric/time";
    Number value = 1234;
    ImmutableMap<String, String> serviceDims = ImmutableMap.of(
        "service",
        "druid/historical",
        "host",
        "localhost",
        "version",
        "0.10.0"
    );
    ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
    ServiceEventBuilder eventBuilder = builder.build(date, metric, value);
    ServiceMetricEvent event = (ServiceMetricEvent) eventBuilder.build(serviceDims);
    InfluxdbEmitterConfig config = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    InfluxdbEmitter influxdbEmitter = new InfluxdbEmitter(config);
    String expected = "druid_metric,service=druid/historical,hostname=localhost druid_time=1234 1509357600000000000"
                      + "\n";
    String actual = influxdbEmitter.transformForInfluxSystems(event);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMetricIsInDimensionWhitelist()
  {
    DateTime date = new DateTime(2017,
                                 10,
                                 30,
                                 10,
                                 00,
                                 DateTimeZone.UTC); // 10:00am on 30/10/2017 = 1509357600000000000 in epoch nanoseconds
    String metric = "metric/time";
    Number value = 1234;
    ImmutableMap<String, String> serviceDims = ImmutableMap.of(
        "service",
        "druid/historical",
        "host",
        "localhost",
        "version",
        "0.10.0"
    );
    ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
    ServiceEventBuilder eventBuilder = builder.build(date, metric, value);
    builder.setDimension("dataSource", "wikipedia");
    builder.setDimension("taskType", "index");
    ServiceMetricEvent event = (ServiceMetricEvent) eventBuilder.build(serviceDims);
    InfluxdbEmitterConfig config = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        ImmutableSet.of("dataSource")
    );
    InfluxdbEmitter influxdbEmitter = new InfluxdbEmitter(config);
    String expected = "druid_metric,service=druid/historical,hostname=localhost,dataSource=wikipedia druid_time=1234 1509357600000000000"
                      + "\n";
    String actual = influxdbEmitter.transformForInfluxSystems(event);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMetricIsInDefaultDimensionWhitelist()
  {
    DateTime date = new DateTime(2017,
                                 10,
                                 30,
                                 10,
                                 00,
                                 DateTimeZone.UTC); // 10:00am on 30/10/2017 = 1509357600000000000 in epoch nanoseconds
    String metric = "metric/time";
    Number value = 1234;
    ImmutableMap<String, String> serviceDims = ImmutableMap.of(
        "service",
        "druid/historical",
        "host",
        "localhost",
        "version",
        "0.10.0"
    );
    ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
    ServiceEventBuilder eventBuilder = builder.build(date, metric, value);
    builder.setDimension("dataSource", "wikipedia");
    builder.setDimension("taskType", "index");
    ServiceMetricEvent event = (ServiceMetricEvent) eventBuilder.build(serviceDims);
    InfluxdbEmitterConfig config = new InfluxdbEmitterConfig(
        "localhost",
        8086,
        "dbname",
        10000,
        15000,
        30000,
        "adam",
        "password",
        null
    );
    InfluxdbEmitter influxdbEmitter = new InfluxdbEmitter(config);
    String expected = "druid_metric,service=druid/historical,hostname=localhost,dataSource=wikipedia,taskType=index druid_time=1234 1509357600000000000"
                      + "\n";
    String actual = influxdbEmitter.transformForInfluxSystems(event);
    Assert.assertEquals(expected, actual);
  }
}
