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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.TestHelper;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class HttpServerInventoryViewConfigTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testDeserializationWithDefaults() throws Exception
  {
    String json = "{}";

    HttpServerInventoryViewConfig config = MAPPER.readValue(json, HttpServerInventoryViewConfig.class);

    Assert.assertEquals(Duration.standardMinutes(4), config.getRequestTimeout());
    Assert.assertEquals(Duration.standardMinutes(1), config.getUnstableAlertTimeout());
    Assert.assertEquals(5, config.getNumThreads());
  }

  @Test
  public void testDeserializationWithNonDefaults() throws Exception
  {
    String json = "{\n"
                  + "  \"serverTimeout\": \"PT2M\",\n"
                  + "  \"serverUnstabilityTimeout\": \"PT3M\",\n"
                  + "  \"numThreads\": 7\n"
                  + "}";

    HttpServerInventoryViewConfig config = MAPPER.readValue(json, HttpServerInventoryViewConfig.class);

    Assert.assertEquals(Duration.standardMinutes(2), config.getRequestTimeout());
    Assert.assertEquals(Duration.standardMinutes(3), config.getUnstableAlertTimeout());
    Assert.assertEquals(7, config.getNumThreads());
  }
}
