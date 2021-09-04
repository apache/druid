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

package org.apache.druid.server.scheduling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.ResourceGroupScheduler;
import org.apache.druid.collections.SemaphoreResourceGroupScheduler;
import org.apache.druid.offheap.OffheapBufferGenerator;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.server.QueryLaningStrategy;
import org.apache.druid.server.QuerySchedulerConfig;
import org.apache.druid.server.ResourceScheduleStrategy;
import org.apache.druid.server.initialization.ServerConfig;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class LaningResourceScheduleStrategy implements ResourceScheduleStrategy
{
  private final boolean ignoreUnknownLane;

  @JsonCreator
  public LaningResourceScheduleStrategy(@JsonProperty("ignoreUnknownLane") Boolean ignoreUnknownLane)
  {
    this.ignoreUnknownLane = !Boolean.FALSE.equals(ignoreUnknownLane);
  }

  @Override
  public BlockingPool<ByteBuffer> getMergeBufferPool(
      ServerConfig serverConfig,
      QuerySchedulerConfig querySchedulerConfig,
      DruidProcessingConfig config
  )
  {
    QueryLaningStrategy laningStrategy = querySchedulerConfig.getLaningStrategy();
    int totalLimit = config.getNumMergeBuffers();
    ResourceGroupScheduler laneCapacityScheduler = getResourceGroupScheduler(
        serverConfig,
        laningStrategy,
        querySchedulerConfig,
        totalLimit
    );
    OffheapBufferGenerator bufferGenerator = new OffheapBufferGenerator(
        "result merging",
        config.intermediateComputeSizeBytes()
    );
    return new DefaultBlockingPool<>(
        laneCapacityScheduler,
        bufferGenerator
    );
  }

  private ResourceGroupScheduler getResourceGroupScheduler(
      ServerConfig serverConfig,
      QueryLaningStrategy laningStrategy,
      QuerySchedulerConfig querySchedulerConfig,
      int totalLimit
  )
  {
    int serverQueryThreadCapacity;
    if (querySchedulerConfig.getNumThreads() > 0
        && querySchedulerConfig.getNumThreads() < serverConfig.getNumThreads()) {
      serverQueryThreadCapacity = querySchedulerConfig.getNumThreads();
    } else {
      serverQueryThreadCapacity = serverConfig.getNumThreads();
    }
    Object2IntMap<String> serverQueryThreadPermitMap = laningStrategy.getLaneLimits(serverQueryThreadCapacity);
    Map<String, Integer> newResourceGroupPermits = new HashMap<>();

    serverQueryThreadPermitMap.forEach((group, serverQueryGroupCapacity) -> {
      double doubleCapacityValue = (double) serverQueryGroupCapacity * totalLimit / serverQueryThreadCapacity;
      int intCapacityValue = Math.max((int) Math.ceil(doubleCapacityValue), 1);
      newResourceGroupPermits.put(group, intCapacityValue);
    });
    return new SemaphoreResourceGroupScheduler(
        totalLimit,
        newResourceGroupPermits,
        ignoreUnknownLane
    );
  }
}
