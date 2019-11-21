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

package org.apache.druid.indexing.overlord.sampler;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;

public class IndexTaskSamplerSpecTest extends EasyMockSupport
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  private final InputSourceSampler inputSourceSampler = createMock(InputSourceSampler.class);

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public IndexTaskSamplerSpecTest()
  {
    MAPPER.setInjectableValues(
        new InjectableValues.Std()
            .addValue(InputSourceSampler.class, inputSourceSampler)
            .addValue(ObjectMapper.class, MAPPER)
    );
    MAPPER.registerModules((Iterable<Module>) new SamplerModule().getJacksonModules());
    MAPPER.registerModules((Iterable<Module>) new FirehoseModule().getJacksonModules());
  }

  @Test
  public void testSerde() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"index\",\n"
                  + "  \"samplerConfig\": {\n"
                  + "    \"numRows\": 123,\n"
                  + "    \"timeoutMs\": 2345\n"
                  + "  },\n"
                  + "  \"spec\": {\n"
                  + "    \"dataSchema\": {\n"
                  + "      \"dataSource\": \"sampler\",\n"
                  + "      \"dimensionsSpec\": {},\n"
                  + "      \"timestampSpec\": {\n"
                  + "        \"missingValue\": \"1970\"\n"
                  + "      }\n"
                  + "    },\n"
                  + "    \"ioConfig\": {\n"
                  + "      \"type\": \"index\",\n"
                  + "      \"inputSource\": {\n"
                  + "        \"type\": \"local\",\n"
                  + "        \"baseDir\": \"/tmp\",\n"
                  + "        \"filter\": \"wikiticker-2015-09-12-sampled.json\"\n"
                  + "      },\n"
                  + "      \"inputFormat\": {\n"
                  + "        \"type\": \"json\"\n"
                  + "      }\n"
                  + "    }\n"
                  + "  }\n"
                  + "}";

    Capture<InputSource> capturedInputSource = EasyMock.newCapture();
    Capture<InputFormat> capturedInputFormat = EasyMock.newCapture();
    Capture<DataSchema> capturedDataSchema = EasyMock.newCapture();
    Capture<SamplerConfig> capturedSamplerConfig = EasyMock.newCapture();

    IndexTaskSamplerSpec spec = MAPPER.readValue(json, IndexTaskSamplerSpec.class);

    EasyMock.expect(inputSourceSampler.sample(
        EasyMock.capture(capturedInputSource),
        EasyMock.capture(capturedInputFormat),
        EasyMock.capture(capturedDataSchema),
        EasyMock.capture(capturedSamplerConfig)
    )).andReturn(new SamplerResponse(0, 0, null));

    replayAll();

    spec.sample();
    verifyAll();

    InputSource inputSource = capturedInputSource.getValue();
    Assert.assertEquals(new File("/tmp"), ((LocalInputSource) inputSource).getBaseDir());
    Assert.assertEquals("wikiticker-2015-09-12-sampled.json", ((LocalInputSource) inputSource).getFilter());

    DataSchema dataSchema = capturedDataSchema.getValue();
    Assert.assertEquals("sampler", dataSchema.getDataSource());
    Assert.assertEquals(JsonInputFormat.class, capturedInputFormat.getValue().getClass());

    SamplerConfig samplerConfig = capturedSamplerConfig.getValue();
    Assert.assertEquals(123, samplerConfig.getNumRows());
    Assert.assertEquals(2345, samplerConfig.getTimeoutMs());
  }
}
