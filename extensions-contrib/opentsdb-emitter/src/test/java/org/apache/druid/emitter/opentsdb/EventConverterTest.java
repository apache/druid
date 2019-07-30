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

package org.apache.druid.emitter.opentsdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class EventConverterTest
{
  private EventConverter converter;

  @Before
  public void setUp()
  {
    converter = new EventConverter(new ObjectMapper(), null);
  }

  @Test
  public void testSanitize()
  {
    String metric = " foo bar/baz";
    Assert.assertEquals("foo_bar.baz", converter.sanitize(metric));
  }

  @Test
  public void testConvert()
  {
    DateTime dateTime = DateTimes.nowUtc();
    ServiceMetricEvent configuredEvent = new ServiceMetricEvent.Builder()
        .setDimension("dataSource", "foo:bar")
        .setDimension("type", "groupBy")
        .build(dateTime, "query/time", 10)
        .build("druid:broker", "127.0.0.1:8080");

    Map<String, Object> expectedTags = new HashMap<>();
    expectedTags.put("service", "druid_broker");
    expectedTags.put("host", "127.0.0.1_8080");
    expectedTags.put("dataSource", "foo_bar");
    expectedTags.put("type", "groupBy");

    OpentsdbEvent opentsdbEvent = converter.convert(configuredEvent);
    Assert.assertEquals("query.time", opentsdbEvent.getMetric());
    Assert.assertEquals(dateTime.getMillis() / 1000L, opentsdbEvent.getTimestamp());
    Assert.assertEquals(10, opentsdbEvent.getValue());
    Assert.assertEquals(expectedTags, opentsdbEvent.getTags());

    ServiceMetricEvent notConfiguredEvent = new ServiceMetricEvent.Builder()
        .setDimension("dataSource", "data-source")
        .setDimension("type", "groupBy")
        .build(dateTime, "foo/bar", 10)
        .build("broker", "brokerHost1");
    Assert.assertNull(converter.convert(notConfiguredEvent));
  }
}
