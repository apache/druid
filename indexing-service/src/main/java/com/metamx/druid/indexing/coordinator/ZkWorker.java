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

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Holds information about a worker and a listener for task status changes associated with the worker.
 */
public class ZkWorker implements Closeable
{
  private final Worker worker;
  private final PathChildrenCache statusCache;

  private final Object lock = new Object();
  private final Map<String, Task> runningTasks = Maps.newHashMap();
  private final Set<String> availabilityGroups = Sets.newHashSet();

  private volatile int currCapacity = 0;
  private volatile DateTime lastCompletedTaskTime = new DateTime();

  public ZkWorker(Worker worker, PathChildrenCache statusCache)
  {
    this.worker = worker;
    this.statusCache = statusCache;
  }

  public void start() throws Exception
  {
    statusCache.start();
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
  public Collection<Task> getRunningTasks()
  {
    return runningTasks.values();
  }

  @JsonProperty
  public int getCurrCapacity()
  {
    return currCapacity;
  }

  @JsonProperty
  public DateTime getLastCompletedTaskTime()
  {
    return lastCompletedTaskTime;
  }

  public void addRunningTask(Task task)
  {
    synchronized (lock) {
      runningTasks.put(task.getId(), task);
      availabilityGroups.add(task.getTaskResource().getAvailabilityGroup());
      currCapacity += task.getTaskResource().getCapacity();
    }
  }

  public void addRunningTasks(Collection<Task> tasks)
  {
    for (Task task : tasks) {
      addRunningTask(task);
    }
  }

  public Task removeRunningTask(Task task)
  {
    synchronized (lock) {
      currCapacity -= task.getTaskResource().getCapacity();
      availabilityGroups.remove(task.getTaskResource().getAvailabilityGroup());
      return runningTasks.remove(task.getId());
    }
  }

  public boolean isRunningTask(String taskId)
  {
    return runningTasks.containsKey(taskId);
  }

  public boolean isAtCapacity()
  {
    return currCapacity >= worker.getCapacity();
  }

  public boolean canRunTask(Task task)
  {
    return (worker.getCapacity() - currCapacity >= task.getTaskResource().getCapacity() && !availabilityGroups.contains(task.getTaskResource().getAvailabilityGroup()));
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
           "runningTasks=" + runningTasks +
           ", lastCompletedTaskTime=" + lastCompletedTaskTime +
           ", currCapacity=" + currCapacity +
           ", worker=" + worker +
           '}';
  }
}
