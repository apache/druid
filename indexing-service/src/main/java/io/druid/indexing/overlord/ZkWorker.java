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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.indexing.worker.TaskAnnouncement;
import io.druid.indexing.worker.Worker;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds information about a worker and a listener for task status changes associated with the worker.
 */
public class ZkWorker implements Closeable
{
  private final PathChildrenCache statusCache;
  private final Function<ChildData, TaskAnnouncement> cacheConverter;

  private AtomicReference<Worker> worker;
  private AtomicReference<DateTime> lastCompletedTaskTime = new AtomicReference<DateTime>(new DateTime());
  private AtomicInteger countinouslyFailedTasksCount = new AtomicInteger(0);

  public ZkWorker(Worker worker, PathChildrenCache statusCache, final ObjectMapper jsonMapper)
  {
    this.worker = new AtomicReference<>(worker);
    this.statusCache = statusCache;
    this.cacheConverter = new Function<ChildData, TaskAnnouncement>()
    {
      @Override
      public TaskAnnouncement apply(ChildData input)
      {
        try {
          return jsonMapper.readValue(input.getData(), TaskAnnouncement.class);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  public void start() throws Exception
  {
    statusCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
  }

  public void addListener(PathChildrenCacheListener listener)
  {
    statusCache.getListenable().addListener(listener);
  }

  @JsonProperty("worker")
  public Worker getWorker()
  {
    return worker.get();
  }

  @JsonProperty("runningTasks")
  public Collection<String> getRunningTaskIds()
  {
    return getRunningTasks().keySet();
  }

  public Map<String, TaskAnnouncement> getRunningTasks()
  {
    Map<String, TaskAnnouncement> retVal = Maps.newHashMap();
    for (TaskAnnouncement taskAnnouncement : Lists.transform(
        statusCache.getCurrentData(),
        cacheConverter
    )) {
      retVal.put(taskAnnouncement.getTaskStatus().getId(), taskAnnouncement);
    }

    return retVal;
  }

  @JsonProperty("currCapacityUsed")
  public int getCurrCapacityUsed()
  {
    int currCapacity = 0;
    for (TaskAnnouncement taskAnnouncement : getRunningTasks().values()) {
      currCapacity += taskAnnouncement.getTaskResource().getRequiredCapacity();
    }
    return currCapacity;
  }

  @JsonProperty("availabilityGroups")
  public Set<String> getAvailabilityGroups()
  {
    Set<String> retVal = Sets.newHashSet();
    for (TaskAnnouncement taskAnnouncement : getRunningTasks().values()) {
      retVal.add(taskAnnouncement.getTaskResource().getAvailabilityGroup());
    }
    return retVal;
  }

  @JsonProperty
  public DateTime getLastCompletedTaskTime()
  {
    return lastCompletedTaskTime.get();
  }

  public boolean isRunningTask(String taskId)
  {
    return getRunningTasks().containsKey(taskId);
  }

  public boolean isValidVersion(String minVersion)
  {
    return worker.get().getVersion().compareTo(minVersion) >= 0;
  }

  public void setWorker(Worker newWorker)
  {
    final Worker oldWorker = worker.get();
    Preconditions.checkArgument(newWorker.getHost().equals(oldWorker.getHost()), "Cannot change Worker host");
    Preconditions.checkArgument(newWorker.getIp().equals(oldWorker.getIp()), "Cannot change Worker ip");

    worker.set(newWorker);
  }

  public void setLastCompletedTaskTime(DateTime completedTaskTime)
  {
    lastCompletedTaskTime.set(completedTaskTime);
  }

  public ImmutableWorkerInfo toImmutable()
  {

    return new ImmutableWorkerInfo(worker.get(), getCurrCapacityUsed(), getAvailabilityGroups(), getRunningTaskIds(), lastCompletedTaskTime.get());
  }

  @Override
  public void close() throws IOException
  {
    statusCache.close();
  }

  public int getCountinouslyFailedTasksCount()
  {
    return countinouslyFailedTasksCount.get();
  }

  public void resetCountinouslyFailedTasksCount()
  {
    this.countinouslyFailedTasksCount.set(0);
  }

  public void incrementCountinouslyFailedTasksCount()
  {
    this.countinouslyFailedTasksCount.incrementAndGet();
  }

  @Override
  public String toString()
  {
    return "ZkWorker{" +
           "worker=" + worker +
           ", lastCompletedTaskTime=" + lastCompletedTaskTime +
           '}';
  }
}
