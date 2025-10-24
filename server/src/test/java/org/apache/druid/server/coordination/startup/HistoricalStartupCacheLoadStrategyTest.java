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

package org.apache.druid.server.coordination.startup;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class HistoricalStartupCacheLoadStrategyTest
{
  private final ObjectMapper mapper = TestHelper.JSON_MAPPER;

  @Test
  public void testDeserializeLoadAllEagerly() throws Exception
  {
    final String json = "{\"type\":\"" + LoadAllEagerlyStrategy.STRATEGY_NAME + "\"}";
    final HistoricalStartupCacheLoadStrategy obj =
        mapper.readValue(json, HistoricalStartupCacheLoadStrategy.class);
    Assert.assertTrue(obj instanceof LoadAllEagerlyStrategy);
  }

  @Test
  public void testDeserializeLoadAllLazily() throws Exception
  {
    final String json = "{\"type\":\"" + LoadAllLazilyStrategy.STRATEGY_NAME + "\"}";
    final HistoricalStartupCacheLoadStrategy obj =
        mapper.readValue(json, HistoricalStartupCacheLoadStrategy.class);
    Assert.assertTrue(obj instanceof LoadAllLazilyStrategy);
  }

  @Test
  public void testDeserializeLoadEagerlyBeforePeriod() throws Exception
  {
    final String json = "{\"type\":\"" + LoadEagerlyBeforePeriod.STRATEGY_NAME + "\",\"period\":\"P3D\"}";
    final HistoricalStartupCacheLoadStrategy obj =
        mapper.readValue(json, HistoricalStartupCacheLoadStrategy.class);
    Assert.assertTrue(obj instanceof LoadEagerlyBeforePeriod);
  }
}
