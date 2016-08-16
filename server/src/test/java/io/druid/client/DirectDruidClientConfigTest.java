/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class DirectDruidClientConfigTest
{
  @Test
  public void testSerdeDefault() throws Exception
  {
    String json = "{}";

    ObjectMapper mapper = TestHelper.JSON_MAPPER;

    DirectDruidClientConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                json,
                DirectDruidClientConfig.class
            )
        ), DirectDruidClientConfig.class
    );

    Assert.assertTrue(config.getQueryResponseIteratorFactory() instanceof V2QueryResponseIteratorFactory);
  }

  @Test
  public void testSerdeWithV3Iterator() throws Exception
  {
    String json = "{\n"
                  + "  \"queryResponseIterator\": { \"type\": \"v3\" }\n"
                  + "}";

    ObjectMapper mapper = TestHelper.JSON_MAPPER;
    DirectDruidClientConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                json,
                DirectDruidClientConfig.class
            )
        ), DirectDruidClientConfig.class
    );

    Assert.assertTrue(config.getQueryResponseIteratorFactory() instanceof V3QueryResponseIteratorFactory);
  }
}
