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

package org.apache.druid.k8s.overlord;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.java.util.common.ISE;

import java.io.InputStream;

public class KubernetesWorkItem extends TaskRunnerWorkItem
{
  private final Task task;
  private final SettableFuture<KubernetesPeonLifecycle> kubernetesPeonLifecycle = SettableFuture.create();

  public KubernetesWorkItem(Task task, ListenableFuture<TaskStatus> statusFuture)
  {
    super(task.getId(), statusFuture);
    this.task = task;
  }

  protected synchronized void setKubernetesPeonLifecycle(KubernetesPeonLifecycle kubernetesPeonLifecycle)
  {
    if (!this.kubernetesPeonLifecycle.set(kubernetesPeonLifecycle)) {
      throw DruidException.defensive("Unexpected call to setKubernetesPeonLifecycle, can only call this once.");
    }
  }

  protected synchronized void shutdown()
  {
    if (kubernetesPeonLifecycle.isDone()) {
      final KubernetesPeonLifecycle lifecycle = FutureUtils.getUncheckedImmediately(kubernetesPeonLifecycle);
      lifecycle.startWatchingLogs();
      lifecycle.shutdown();
    }
  }

  protected boolean isPending()
  {
    return RunnerTaskState.PENDING.equals(getRunnerTaskState());
  }

  protected boolean isRunning()
  {
    return RunnerTaskState.RUNNING.equals(getRunnerTaskState());
  }

  protected RunnerTaskState getRunnerTaskState()
  {
    if (!kubernetesPeonLifecycle.isDone()) {
      return RunnerTaskState.PENDING;
    }

    switch (FutureUtils.getUncheckedImmediately(kubernetesPeonLifecycle).getState()) {
      case NOT_STARTED:
      case PENDING:
        return RunnerTaskState.PENDING;
      case RUNNING:
        return RunnerTaskState.RUNNING;
      case STOPPED:
        return RunnerTaskState.NONE;
      default:
        throw new ISE("Peon lifecycle in unknown state");
    }
  }

  protected Optional<InputStream> streamTaskLogs()
  {
    if (!kubernetesPeonLifecycle.isDone()) {
      return Optional.absent();
    }
    return FutureUtils.getUncheckedImmediately(kubernetesPeonLifecycle).streamLogs();
  }

  @Override
  public TaskLocation getLocation()
  {
    if (!kubernetesPeonLifecycle.isDone()) {
      return TaskLocation.unknown();
    }
    return FutureUtils.getUncheckedImmediately(kubernetesPeonLifecycle).getTaskLocation();
  }

  @Override
  public String getTaskType()
  {
    return task.getType();
  }

  @Override
  public String getDataSource()
  {
    return task.getDataSource();
  }

  public Task getTask()
  {
    return task;
  }

  /**
   * Future that resolves when the work item's lifecycle has been initialized.
   */
  public ListenableFuture<TaskLocation> getTaskLocationAsync()
  {
    return FutureUtils.transformAsync(kubernetesPeonLifecycle, KubernetesPeonLifecycle::getTaskLocationAsync);
  }
}
