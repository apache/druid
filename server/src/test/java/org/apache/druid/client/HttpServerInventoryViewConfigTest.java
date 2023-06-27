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

import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 */
class HttpServerInventoryViewConfigTest
{
  @Test
  void testDeserializationWithDefaults() throws Exception
  {
    String json = "{}";

    HttpServerInventoryViewConfig config = TestHelper.makeJsonMapper().readValue(json, HttpServerInventoryViewConfig.class);

    assertEquals(TimeUnit.MINUTES.toMillis(4), config.getServerTimeout());
    assertEquals(TimeUnit.MINUTES.toMillis(1), config.getServerUnstabilityTimeout());
    assertEquals(5, config.getNumThreads());
  }

  @Test
  void testDeserializationWithNonDefaults() throws Exception
  {
    String json = "{\n"
                  + "  \"serverTimeout\": \"PT2M\",\n"
                  + "  \"serverUnstabilityTimeout\": \"PT3M\",\n"
                  + "  \"numThreads\": 7\n"
                  + "}";

    HttpServerInventoryViewConfig config = TestHelper.makeJsonMapper().readValue(json, HttpServerInventoryViewConfig.class);

    assertEquals(TimeUnit.MINUTES.toMillis(2), config.getServerTimeout());
    assertEquals(TimeUnit.MINUTES.toMillis(3), config.getServerUnstabilityTimeout());
    assertEquals(7, config.getNumThreads());
  }
}
