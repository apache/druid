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

import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.Task;

import java.util.Collection;

/**
 * Interface for handing off tasks. Used by a {@link com.metamx.druid.merger.coordinator.exec.TaskConsumer} to
 * run tasks that have been locked.
 */
public interface TaskRunner
{
  /**
   * Run a task. The returned status should be some kind of completed status.
   *
   * @param task task to run
   * @return task status, eventually
   */
  public ListenableFuture<TaskStatus> run(Task task);

  public Collection<TaskRunnerWorkItem> getRunningTasks();

  public Collection<TaskRunnerWorkItem> getPendingTasks();

  public Collection<ZkWorker> getWorkers();
}
