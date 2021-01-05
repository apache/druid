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

package org.apache.druid.data.input.opencensus.protobuf;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputFormat;
import org.junit.Assert;
import org.junit.Test;

public class OpenCensusInputFormatTest
{
  @Test
  public void testSerde() throws Exception
  {
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat("metric.name", "descriptor.", "custom.");

    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.registerModules(new OpenCensusProtobufExtensionsModule().getJacksonModules());

    final OpenCensusProtobufInputFormat actual = (OpenCensusProtobufInputFormat) jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        InputFormat.class
    );
    Assert.assertEquals(inputFormat, actual);
    Assert.assertEquals("metric.name", actual.getMetricDimension());
    Assert.assertEquals("descriptor.", actual.getMetricLabelPrefix());
    Assert.assertEquals("custom.", actual.getResourceLabelPrefix());
  }

  @Test
  public void testDefaults()
  {
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat(null, null, null);

    Assert.assertEquals("name", inputFormat.getMetricDimension());
    Assert.assertEquals("", inputFormat.getMetricLabelPrefix());
    Assert.assertEquals("resource.", inputFormat.getResourceLabelPrefix());
  }
}
