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

package org.apache.druid.server.coordinator.balancer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BalancerStrategyFactoryTest
{
  private final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  private ListeningExecutorService executorService;

  @Before
  public void setup()
  {
    executorService = MoreExecutors.listeningDecorator(
        new BlockingExecutorService("StrategyFactoryTest-%s")
    );
  }

  @After
  public void tearDown()
  {
    executorService.shutdownNow();
  }

  @Test
  public void testCachingCostStrategyFallsBackToCost() throws JsonProcessingException
  {
    final String json = "{\"strategy\":\"cachingCost\"}";
    BalancerStrategyFactory factory = MAPPER.readValue(json, BalancerStrategyFactory.class);
    BalancerStrategy strategy = factory.createBalancerStrategy(executorService);

    Assert.assertTrue(strategy instanceof CostBalancerStrategy);
    Assert.assertFalse(strategy instanceof CachingCostBalancerStrategy);
  }
}
