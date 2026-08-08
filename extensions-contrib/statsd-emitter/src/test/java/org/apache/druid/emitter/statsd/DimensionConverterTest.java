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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DimensionConverterTest
{
  @Test
  public void testConvert()
  {
    DimensionConverter dimensionConverter = new DimensionConverter(new ObjectMapper(), null);
    ServiceMetricEvent event = new ServiceMetricEvent.Builder()
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
        .setMetric("query/time", 10)
        .build("broker", "brokerHost1");

    ImmutableMap.Builder<String, String> actual = new ImmutableMap.Builder<>();
    StatsDMetric statsDMetric = dimensionConverter.addFilteredUserDims(
        event.getService(),
        event.getMetric(),
        event.getUserDims(),
        actual
    );
    Assertions.assertEquals(StatsDMetric.Type.timer, statsDMetric.type, "correct StatsDMetric.Type");
    ImmutableMap.Builder<String, String> expected = new ImmutableMap.Builder<>();
    expected.put("dataSource", "data-source");
    expected.put("type", "groupBy");
    Assertions.assertEquals(expected.build(), actual.build(), "correct Dimensions");
  }
}
