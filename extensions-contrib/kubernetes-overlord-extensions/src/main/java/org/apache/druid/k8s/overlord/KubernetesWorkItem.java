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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
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
  private KubernetesPeonLifecycle kubernetesPeonLifecycle = null;

  public KubernetesWorkItem(Task task, ListenableFuture<TaskStatus> statusFuture)
  {
    super(task.getId(), statusFuture);
    this.task = task;
  }

  protected synchronized void setKubernetesPeonLifecycle(KubernetesPeonLifecycle kubernetesPeonLifecycle)
  {
    Preconditions.checkState(this.kubernetesPeonLifecycle == null);
    this.kubernetesPeonLifecycle = kubernetesPeonLifecycle;
  }

  protected synchronized void shutdown()
  {

    if (this.kubernetesPeonLifecycle != null) {
      this.kubernetesPeonLifecycle.startWatchingLogs();
      this.kubernetesPeonLifecycle.shutdown();
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
    if (kubernetesPeonLifecycle == null) {
      return RunnerTaskState.PENDING;
    }

    switch (kubernetesPeonLifecycle.getState()) {
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
    if (kubernetesPeonLifecycle == null) {
      return Optional.absent();
    }
    return kubernetesPeonLifecycle.streamLogs();
  }

  @Override
  public TaskLocation getLocation()
  {
    if (kubernetesPeonLifecycle == null) {
      return TaskLocation.unknown();
    }
    return kubernetesPeonLifecycle.getTaskLocation();
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
}
