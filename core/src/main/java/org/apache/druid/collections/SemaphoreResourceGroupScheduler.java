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

package org.apache.druid.collections;

import org.apache.druid.java.util.common.IAE;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class SemaphoreResourceGroupScheduler implements ResourceGroupScheduler
{
  private final int capacity;
  private final Map<String, Integer> configMap;
  private final Map<String, Semaphore> groupSemaphoreRegistry;
  private final boolean ignoreUnknownGroup;

  public SemaphoreResourceGroupScheduler(int capacity, Map<String, Integer> config, boolean ignoreUnknownGroup)
  {
    this.capacity = capacity;
    this.configMap = config;
    this.ignoreUnknownGroup = ignoreUnknownGroup;
    this.groupSemaphoreRegistry = new HashMap<>();
    configMap.forEach((group, permits) -> {
      Semaphore semaphore = new Semaphore(permits, true);
      groupSemaphoreRegistry.put(group, semaphore);
    });
  }

  @Override
  public int getGroupAvailableCapacity(String resourceGroup)
  {
    if (ignoreResourceGroup(resourceGroup)) {
      return capacity;
    } else {
      return getGroupSemaphore(resourceGroup).availablePermits();
    }
  }

  @Override
  public int getGroupCapacity(String resourceGroup)
  {
    if (ignoreResourceGroup(resourceGroup)) {
      return capacity;
    } else {
      return configMap.getOrDefault(resourceGroup, -1);
    }
  }

  @Override
  public int getTotalCapacity()
  {
    return capacity;
  }

  @Override
  public Set<String> getAllGroup()
  {
    return configMap.keySet();
  }

  @Override
  public void accquire(String resourceGroup, int permits) throws InterruptedException
  {
    if (!ignoreResourceGroup(resourceGroup)) {
      getGroupSemaphore(resourceGroup).acquire(permits);
    }
  }

  @Override
  public boolean tryAcquire(String resourceGroup, int permits)
  {
    if (ignoreResourceGroup(resourceGroup)) {
      return true;
    }
    return getGroupSemaphore(resourceGroup).tryAcquire(permits);
  }

  @Override
  public boolean tryAcquire(String resourceGroup, int permits, long timeout, TimeUnit unit) throws InterruptedException
  {
    if (ignoreResourceGroup(resourceGroup)) {
      return true;
    }
    return getGroupSemaphore(resourceGroup).tryAcquire(permits, timeout, unit);
  }

  @Override
  public void release(String resourceGroup, int permits)
  {
    if (!ignoreResourceGroup(resourceGroup)) {
      getGroupSemaphore(resourceGroup).release(permits);
    }
  }

  private boolean ignoreResourceGroup(String resourceGroup)
  {
    return resourceGroup == null || (ignoreUnknownGroup && configMap.get(resourceGroup) == null);
  }

  private Semaphore getGroupSemaphore(String group)
  {
    Semaphore semaphore = groupSemaphoreRegistry.get(group);
    if (semaphore == null) {
      throw new IAE("resource group %s not exists", group);
    }
    return semaphore;
  }
}
