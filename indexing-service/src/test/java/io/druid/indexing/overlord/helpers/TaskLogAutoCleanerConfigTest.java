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

package io.druid.indexing.overlord.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class TaskLogAutoCleanerConfigTest
{
  @Test
  public void testSerde() throws Exception
  {
    String json = "{\n"
                  + "  \"enabled\": true,\n"
                  + "  \"initialDelay\": 10,\n"
                  + "  \"delay\": 40,\n"
                  + "  \"durationToRetain\": 30\n"
                  + "}";

    ObjectMapper mapper = TestHelper.makeJsonMapper();

    TaskLogAutoCleanerConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                json,
                TaskLogAutoCleanerConfig.class
            )
        ), TaskLogAutoCleanerConfig.class
    );

    Assert.assertTrue(config.isEnabled());
    Assert.assertEquals(10, config.getInitialDelay());
    Assert.assertEquals(40, config.getDelay());
    Assert.assertEquals(30, config.getDurationToRetain());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String json = "{}";

    ObjectMapper mapper = TestHelper.makeJsonMapper();

    TaskLogAutoCleanerConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                json,
                TaskLogAutoCleanerConfig.class
            )
        ), TaskLogAutoCleanerConfig.class
    );

    Assert.assertFalse(config.isEnabled());
    Assert.assertTrue(config.getInitialDelay() >= 60000 && config.getInitialDelay() <= 300000);
    Assert.assertEquals(6 * 60 * 60 * 1000, config.getDelay());
    Assert.assertEquals(Long.MAX_VALUE, config.getDurationToRetain());
  }
}
