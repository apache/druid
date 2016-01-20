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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.Pair;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.autoscaling.ScalingStats;

import java.util.Collection;
import java.util.List;

/**
 * Interface for handing off tasks. Managed by a {@link io.druid.indexing.overlord.TaskQueue}.
 */
public interface TaskRunner
{
  /**
   * Some task runners can restart previously-running tasks after being bounced. This method does that, and returns
   * the list of tasks (and status futures).
   */
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore();

  /**
   * Run a task. The returned status should be some kind of completed status.
   *
   * @param task task to run
   *
   * @return task status, eventually
   */
  ListenableFuture<TaskStatus> run(Task task);

  /**
   * Inform the task runner it can clean up any resources associated with a task. This implies shutdown of any
   * currently-running tasks.
   *
   * @param taskid task ID to clean up resources for
   */
  void shutdown(String taskid);

  /**
   * Stop this task runner. This may block until currently-running tasks can be gracefully stopped. After calling
   * stopping, "run" will not accept further tasks.
   */
  void stop();

  Collection<? extends TaskRunnerWorkItem> getRunningTasks();

  Collection<? extends TaskRunnerWorkItem> getPendingTasks();

  Collection<? extends TaskRunnerWorkItem> getKnownTasks();

  /**
   * Some runners are able to scale up and down their capacity in a dynamic manner. This returns stats on those activities
   *
   * @return ScalingStats if the runner has an underlying resource which can scale, Optional.absent() otherwise
   */
  Optional<ScalingStats> getScalingStats();
}
