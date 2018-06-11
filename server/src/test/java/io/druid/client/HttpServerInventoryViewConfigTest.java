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

import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 */
public class HttpServerInventoryViewConfigTest
{
  @Test
  public void testDeserializationWithDefaults() throws Exception
  {
    String json = "{}";

    HttpServerInventoryViewConfig config = TestHelper.makeJsonMapper().readValue(json, HttpServerInventoryViewConfig.class);

    Assert.assertEquals(TimeUnit.MINUTES.toMillis(4), config.getServerTimeout());
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(1), config.getServerUnstabilityTimeout());
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

    HttpServerInventoryViewConfig config = TestHelper.makeJsonMapper().readValue(json, HttpServerInventoryViewConfig.class);

    Assert.assertEquals(TimeUnit.MINUTES.toMillis(2), config.getServerTimeout());
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(3), config.getServerUnstabilityTimeout());
    Assert.assertEquals(7, config.getNumThreads());
  }
}
