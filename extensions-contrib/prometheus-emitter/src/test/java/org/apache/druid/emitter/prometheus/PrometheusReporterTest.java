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

package org.apache.druid.emitter.prometheus;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

public class PrometheusReporterTest
{
  private ObjectMapper mapper = new DefaultObjectMapper();
  private PrometheusReporter reporter;

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper()));
    reporter = new PrometheusReporter(mapper, null, "10.100.201.218", 9091, "druid");
  }

  @Test
  public void testPush() throws Exception
  {
    DateTime createdTime = DateTimes.nowUtc();
    ImmutableMap<String, String> serviceDims = new ImmutableMap.Builder<String, String>()
        .put("service", "druid/broker")
        .put("host", "localhost")
        .build();
    ServiceMetricEvent metricEvent = ServiceMetricEvent.builder().setDimension("dataSource", "test").build(createdTime, "query/time", 10).build(serviceDims);

    reporter.emitMetric(metricEvent);
    reporter.push("push_to_gateway_test");
  }

}
