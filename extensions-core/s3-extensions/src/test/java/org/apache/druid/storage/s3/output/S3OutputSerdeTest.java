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

package org.apache.druid.storage.s3.output;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;

public class S3OutputSerdeTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();


  @Test
  public void sanity() throws IOException
  {
    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"bucket\": \"TEST\",\n"
                                           + "  \"prefix\": \"abc\",\n"
                                           + "  \"tempDir\": \"/tmp\",\n"
                                           + "  \"chunkSize\":104857600,\n"
                                           + "  \"maxResultsSize\":209715200,\n"
                                           + "  \"maxRetry\": 2\n"
                                           + "}\n");

    S3OutputConfig s3OutputConfig = new S3OutputConfig(
        "TEST",
        "abc",
        new File("/tmp"),
        HumanReadableBytes.valueOf(HumanReadableBytes.parse("100Mib")),
        HumanReadableBytes.valueOf(HumanReadableBytes.parse("200Mib")),
        2
    );

    Assert.assertEquals(
        json,
        MAPPER.writeValueAsString(s3OutputConfig)
    );

    Assert.assertEquals(s3OutputConfig, MAPPER.readValue(json, S3OutputConfig.class));
  }

  @Test
  public void noPrefix() throws JsonProcessingException
  {
    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"bucket\": \"TEST\",\n"
                                           + "  \"tempDir\": \"/tmp\",\n"
                                           + "  \"chunkSize\":104857600,\n"
                                           + "  \"maxResultsSize\":209715200,\n"
                                           + "  \"maxRetry\": 2\n"
                                           + "}\n");
    expectedException.expect(MismatchedInputException.class);
    expectedException.expectMessage("Missing required creator property 'prefix'");
    MAPPER.readValue(json, S3OutputConfig.class);
  }

  @Test
  public void noBucket() throws JsonProcessingException
  {
    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"prefix\": \"abc\",\n"
                                           + "  \"tempDir\": \"/tmp\",\n"
                                           + "  \"chunkSize\":104857600,\n"
                                           + "  \"maxResultsSize\":209715200,\n"
                                           + "  \"maxRetry\": 2\n"
                                           + "}\n");
    expectedException.expect(MismatchedInputException.class);
    expectedException.expectMessage("Missing required creator property 'bucket'");
    MAPPER.readValue(json, S3OutputConfig.class);
  }

  @Test
  public void noTempDir() throws JsonProcessingException
  {
    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"prefix\": \"abc\",\n"
                                           + "  \"bucket\": \"TEST\",\n"
                                           + "  \"chunkSize\":104857600,\n"
                                           + "  \"maxResultsSize\":209715200,\n"
                                           + "  \"maxRetry\": 2\n"
                                           + "}\n");
    expectedException.expect(MismatchedInputException.class);
    expectedException.expectMessage("Missing required creator property 'tempDir'");
    MAPPER.readValue(json, S3OutputConfig.class);
  }

  @Test
  public void leastArguments() throws JsonProcessingException
  {
    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"tempDir\": \"/tmp\",\n"
                                           + "  \"prefix\": \"abc\",\n"
                                           + "  \"bucket\": \"TEST\"\n"
                                           + "}\n");

    S3OutputConfig s3OutputConfig = new S3OutputConfig(
        "TEST",
        "abc",
        new File("/tmp"),
        null,
        null,
        null
    );
    Assert.assertEquals(s3OutputConfig, MAPPER.readValue(json, S3OutputConfig.class));
  }


  @Test
  public void testChunkValidation() throws JsonProcessingException
  {

    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"prefix\": \"abc\",\n"
                                           + "  \"bucket\": \"TEST\",\n"
                                           + "  \"tempDir\": \"/tmp\",\n"
                                           + "  \"chunkSize\":104,\n"
                                           + "  \"maxResultsSize\":209715200,\n"
                                           + "  \"maxRetry\": 2\n"
                                           + "}\n");
    expectedException.expect(ValueInstantiationException.class);
    expectedException.expectMessage("chunkSize[104] should be >=");
    MAPPER.readValue(json, S3OutputConfig.class);
  }

  private static String jsonStringReadyForAssert(String input)
  {
    return StringUtils.removeChar(StringUtils.removeChar(input, '\n'), ' ');
  }
}
