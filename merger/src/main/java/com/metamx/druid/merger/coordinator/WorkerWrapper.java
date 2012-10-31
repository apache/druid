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

package com.metamx.druid.merger.coordinator;

import com.metamx.druid.merger.worker.Worker;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import org.joda.time.DateTime;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 */
public class WorkerWrapper
{
  private final Worker worker;
  private final ConcurrentSkipListSet<String> runningTasks;
  private final PathChildrenCache watcher;

  private volatile DateTime lastCompletedTaskTime;

  public WorkerWrapper(Worker worker, ConcurrentSkipListSet<String> runningTasks, PathChildrenCache watcher)
  {
    this.worker = worker;
    this.runningTasks = runningTasks;
    this.watcher = watcher;
  }

  public Worker getWorker()
  {
    return worker;
  }

  public Set<String> getRunningTasks()
  {
    return runningTasks;
  }

  public PathChildrenCache getWatcher()
  {
    return watcher;
  }

  public DateTime getLastCompletedTaskTime()
  {
    return lastCompletedTaskTime;
  }

  public boolean isAtCapacity()
  {
    return runningTasks.size() >= worker.getCapacity();
  }

  public void setLastCompletedTaskTime(DateTime completedTaskTime)
  {
    lastCompletedTaskTime = completedTaskTime;
  }

  public void removeTask(String taskId)
  {
    runningTasks.remove(taskId);
  }
}
