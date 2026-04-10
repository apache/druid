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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class MetricAllowlistParsersTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testParseMetricNameObject()
      throws Exception
  {
    final JsonNode jsonNode = MAPPER.readTree("{\"query/time\":[],\"jvm/gc/cpu\":[]}");

    final Set<String> metricNames = MetricAllowlistParsers.parseMetricNameObject(jsonNode, "test-source");

    Assert.assertEquals(2, metricNames.size());
    Assert.assertTrue(metricNames.contains("query/time"));
    Assert.assertTrue(metricNames.contains("jvm/gc/cpu"));
  }

  @Test
  public void testParseMetricNameObjectRequiresJsonObject()
      throws Exception
  {
    final JsonNode jsonNode = MAPPER.readTree("[\"query/time\"]");

    final DruidException exception = Assert.assertThrows(
        DruidException.class,
        () -> MetricAllowlistParsers.parseMetricNameObject(jsonNode, "test-source")
    );

    Assert.assertEquals(
        "Metric allowlist file [test-source] must be a JSON object of metric names.",
        exception.getMessage()
    );
  }
}
