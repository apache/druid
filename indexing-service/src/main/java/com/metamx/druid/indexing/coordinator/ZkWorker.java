/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.indexing.coordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.worker.Worker;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Holds information about a worker and a listener for task status changes associated with the worker.
 */
public class ZkWorker implements Closeable
{
  private final Worker worker;
  private final PathChildrenCache statusCache;
  private final Function<ChildData, TaskStatus> cacheConverter;

  private volatile DateTime lastCompletedTaskTime = new DateTime();

  public ZkWorker(Worker worker, PathChildrenCache statusCache, final ObjectMapper jsonMapper)
  {
    this.worker = worker;
    this.statusCache = statusCache;
    this.cacheConverter = new Function<ChildData, TaskStatus>()
    {
      @Override
      public TaskStatus apply(ChildData input)
      {
        try {
          return jsonMapper.readValue(input.getData(), TaskStatus.class);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  public void start(PathChildrenCache.StartMode startMode) throws Exception
  {
    statusCache.start(startMode);
  }

  public void addListener(PathChildrenCacheListener listener)
  {
    statusCache.getListenable().addListener(listener);
  }

  @JsonProperty
  public Worker getWorker()
  {
    return worker;
  }

  @JsonProperty
  public Map<String, TaskStatus> getRunningTasks()
  {
    Map<String, TaskStatus> retVal = Maps.newHashMap();
    for (TaskStatus taskStatus : Lists.transform(
        statusCache.getCurrentData(),
        cacheConverter
    )) {
      retVal.put(taskStatus.getId(), taskStatus);
    }

    return retVal;
  }

  @JsonProperty("currCapacity")
  public int getCurrCapacity()
  {
    int currCapacity = 0;
    for (TaskStatus taskStatus : getRunningTasks().values()) {
      currCapacity += taskStatus.getResource().getRequiredCapacity();
    }
    return currCapacity;
  }

  @JsonProperty("availabilityGroups")
  public Set<String> getAvailabilityGroups()
  {
    Set<String> retVal = Sets.newHashSet();
    for (TaskStatus taskStatus : getRunningTasks().values()) {
      retVal.add(taskStatus.getResource().getAvailabilityGroup());
    }
    return retVal;
  }

  @JsonProperty
  public DateTime getLastCompletedTaskTime()
  {
    return lastCompletedTaskTime;
  }

  public boolean isRunningTask(String taskId)
  {
    return getRunningTasks().containsKey(taskId);
  }

  public boolean isAtCapacity()
  {
    return getCurrCapacity() >= worker.getCapacity();
  }

  public boolean canRunTask(Task task)
  {
    return (worker.getCapacity() - getCurrCapacity() >= task.getTaskResource().getRequiredCapacity()
            && !getAvailabilityGroups().contains(task.getTaskResource().getAvailabilityGroup()));
  }

  public void setLastCompletedTaskTime(DateTime completedTaskTime)
  {
    lastCompletedTaskTime = completedTaskTime;
  }

  @Override
  public void close() throws IOException
  {
    statusCache.close();
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
