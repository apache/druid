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

package org.apache.druid.storage.azure.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AzureOutputSerdeTest
{

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void sanity() throws IOException
  {
    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"container\": \"TEST\",\n"
                                           + "  \"prefix\": \"abc\",\n"
                                           + "  \"tempDir\": \"/tmp\",\n"
                                           + "  \"chunkSize\":104857600,\n"
                                           + "  \"maxRetry\": 2\n"
                                           + "}\n");

    AzureOutputConfig azureOutputConfig = new AzureOutputConfig(
        "TEST",
        "abc",
        new File("/tmp"),
        HumanReadableBytes.valueOf(HumanReadableBytes.parse("100Mib")),
        2
    );

    Assert.assertEquals(
        json,
        MAPPER.writeValueAsString(azureOutputConfig)
    );

    Assert.assertEquals(azureOutputConfig, MAPPER.readValue(json, AzureOutputConfig.class));
  }

  @Test
  public void noPrefix()
  {
    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"container\": \"TEST\",\n"
                                           + "  \"tempDir\": \"/tmp\",\n"
                                           + "  \"chunkSize\":104857600,\n"
                                           + "  \"maxRetry\": 2\n"
                                           + "}\n");
    Assert.assertThrows(MismatchedInputException.class, () -> MAPPER.readValue(json, AzureOutputConfig.class));
  }

  @Test
  public void noContainer()
  {
    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"prefix\": \"abc\",\n"
                                           + "  \"tempDir\": \"/tmp\",\n"
                                           + "  \"chunkSize\":104857600,\n"
                                           + "  \"maxRetry\": 2\n"
                                           + "}\n");
    Assert.assertThrows(MismatchedInputException.class, () -> MAPPER.readValue(json, AzureOutputConfig.class));
  }

  @Test
  public void noTempDir()
  {
    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"prefix\": \"abc\",\n"
                                           + "  \"container\": \"TEST\",\n"
                                           + "  \"chunkSize\":104857600,\n"
                                           + "  \"maxRetry\": 2\n"
                                           + "}\n");
    Assert.assertThrows(MismatchedInputException.class, () -> MAPPER.readValue(json, AzureOutputConfig.class));
  }

  @Test
  public void leastArguments() throws JsonProcessingException
  {
    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"tempDir\": \"/tmp\",\n"
                                           + "  \"prefix\": \"abc\",\n"
                                           + "  \"container\": \"TEST\"\n"
                                           + "}\n");

    AzureOutputConfig azureOutputConfig = new AzureOutputConfig(
        "TEST",
        "abc",
        new File("/tmp"),
        null,
        null
    );
    Assert.assertEquals(azureOutputConfig, MAPPER.readValue(json, AzureOutputConfig.class));
  }


  @Test
  public void testChunkValidation()
  {

    String json = jsonStringReadyForAssert("{\n"
                                           + "  \"prefix\": \"abc\",\n"
                                           + "  \"container\": \"TEST\",\n"
                                           + "  \"tempDir\": \"/tmp\",\n"
                                           + "  \"chunkSize\":104,\n"
                                           + "  \"maxRetry\": 2\n"
                                           + "}\n");
    Assert.assertThrows(ValueInstantiationException.class, () -> MAPPER.readValue(json, AzureOutputConfig.class));
  }

  private static String jsonStringReadyForAssert(String input)
  {
    return StringUtils.removeChar(StringUtils.removeChar(input, '\n'), ' ');
  }
}
