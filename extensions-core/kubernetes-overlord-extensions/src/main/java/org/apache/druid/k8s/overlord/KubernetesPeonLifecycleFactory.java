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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.AbstractKubernetesPeonClient;
import org.apache.druid.tasklogs.TaskLogs;

public class KubernetesPeonLifecycleFactory implements PeonLifecycleFactory
{
  private final AbstractKubernetesPeonClient client;
  private final TaskLogs taskLogs;
  private final ObjectMapper mapper;
  private final long logSaveTimeoutMs;

  public KubernetesPeonLifecycleFactory(
      AbstractKubernetesPeonClient client,
      TaskLogs taskLogs,
      ObjectMapper mapper,
      long logSaveTimeoutMs
  )
  {
    this.client = client;
    this.taskLogs = taskLogs;
    this.mapper = mapper;
    this.logSaveTimeoutMs = logSaveTimeoutMs;
  }

  @Override
  public KubernetesPeonLifecycle build(Task task, K8sTaskId k8sTaskId, KubernetesPeonLifecycle.TaskStateListener stateListener)
  {
    return new KubernetesPeonLifecycle(
        task,
        k8sTaskId,
        client,
        taskLogs,
        mapper,
        stateListener,
        logSaveTimeoutMs
    );
  }
}
