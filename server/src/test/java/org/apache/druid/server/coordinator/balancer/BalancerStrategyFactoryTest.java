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
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class BalancerStrategyFactoryTest
{
  private final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testCachingCostStrategyFallsBackToCost() throws JsonProcessingException
  {
    final String json = "{\"strategy\":\"cachingCost\"}";
    BalancerStrategyFactory factory = MAPPER.readValue(json, BalancerStrategyFactory.class);
    BalancerStrategy strategy = factory.createBalancerStrategy(1);

    Assert.assertTrue(strategy instanceof CostBalancerStrategy);
    Assert.assertFalse(strategy instanceof CachingCostBalancerStrategy);

    factory.stopExecutor();
  }

  @Test
  public void testBalancerFactoryCreatesNewExecutorIfNumThreadsChanges()
  {
    BalancerStrategyFactory factory = new CostBalancerStrategyFactory();
    ListeningExecutorService exec1 = factory.getOrCreateBalancerExecutor(1);
    ListeningExecutorService exec2 = factory.getOrCreateBalancerExecutor(2);

    Assert.assertTrue(exec1.isShutdown());
    Assert.assertNotSame(exec1, exec2);

    ListeningExecutorService exec3 = factory.getOrCreateBalancerExecutor(3);
    Assert.assertTrue(exec2.isShutdown());
    Assert.assertNotSame(exec2, exec3);

    ListeningExecutorService exec4 = factory.getOrCreateBalancerExecutor(3);
    Assert.assertFalse(exec3.isShutdown());
    Assert.assertSame(exec3, exec4);

    factory.stopExecutor();
  }
}
