/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexing.overlord;

import com.google.common.util.concurrent.ListenableFuture;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;

import java.util.Collection;

/**
 * Interface for handing off tasks. Managed by a {@link io.druid.indexing.overlord.TaskQueue}.
 */
public interface TaskRunner
{
  /**
   * Run a task. The returned status should be some kind of completed status.
   *
   * @param task task to run
   *
   * @return task status, eventually
   */
  public ListenableFuture<TaskStatus> run(Task task);

  /**
   * Inform the task runner it can clean up any resources associated with a task. This implies shutdown of any
   * currently-running tasks.
   *
   * @param taskid task ID to clean up resources for
   */
  public void shutdown(String taskid);

  public Collection<? extends TaskRunnerWorkItem> getRunningTasks();

  public Collection<? extends TaskRunnerWorkItem> getPendingTasks();

  public Collection<? extends TaskRunnerWorkItem> getKnownTasks();

  public Collection<ZkWorker> getWorkers();
}
