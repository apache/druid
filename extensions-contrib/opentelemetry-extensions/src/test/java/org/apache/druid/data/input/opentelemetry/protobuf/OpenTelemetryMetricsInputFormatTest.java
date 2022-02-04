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

package org.apache.druid.data.input.opentelemetry.protobuf;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputFormat;
import org.junit.Assert;
import org.junit.Test;

public class OpenTelemetryMetricsInputFormatTest
{
  @Test
  public void testSerde() throws Exception
  {
    OpenTelemetryMetricsProtobufInputFormat inputFormat = new OpenTelemetryMetricsProtobufInputFormat(
            "metric.name",
            "raw.value",
            "descriptor.",
            "custom."
    );

    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.registerModules(new OpenTelemetryProtobufExtensionsModule().getJacksonModules());

    final OpenTelemetryMetricsProtobufInputFormat actual = (OpenTelemetryMetricsProtobufInputFormat) jsonMapper.readValue(
            jsonMapper.writeValueAsString(inputFormat),
            InputFormat.class
    );
    Assert.assertEquals(inputFormat, actual);
    Assert.assertEquals("metric.name", actual.getMetricDimension());
    Assert.assertEquals("raw.value", actual.getValueDimension());
    Assert.assertEquals("descriptor.", actual.getMetricAttributePrefix());
    Assert.assertEquals("custom.", actual.getResourceAttributePrefix());
  }

  @Test
  public void testDefaults()
  {
    OpenTelemetryMetricsProtobufInputFormat inputFormat = new OpenTelemetryMetricsProtobufInputFormat(
            null,
            null,
            null,
            null
    );

    Assert.assertEquals("metric", inputFormat.getMetricDimension());
    Assert.assertEquals("value", inputFormat.getValueDimension());
    Assert.assertEquals("", inputFormat.getMetricAttributePrefix());
    Assert.assertEquals("resource.", inputFormat.getResourceAttributePrefix());
  }
}
