/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
