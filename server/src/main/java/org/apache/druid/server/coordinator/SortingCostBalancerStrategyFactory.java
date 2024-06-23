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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JacksonInject;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;

public class SortingCostBalancerStrategyFactory extends CostBalancerStrategyFactory
{
  private final ServerInventoryView serverInventoryView;
  private final LoadQueueTaskMaster loadQueueTaskMaster;


  public SortingCostBalancerStrategyFactory(
      @JacksonInject ServerInventoryView serverInventoryView,
      @JacksonInject LoadQueueTaskMaster loadQueueTaskMaster
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.loadQueueTaskMaster = loadQueueTaskMaster;
  }

  @Override
  public CostBalancerStrategy createBalancerStrategy(final int numBalancerThreads)
  {
    return new SortingCostBalancerStrategy(
        serverInventoryView,
        loadQueueTaskMaster,
        getOrCreateBalancerExecutor(1)
    );
  }
}
