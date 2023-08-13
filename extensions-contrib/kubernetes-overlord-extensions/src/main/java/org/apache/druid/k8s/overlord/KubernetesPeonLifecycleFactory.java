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
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.tasklogs.TaskLogs;

public class KubernetesPeonLifecycleFactory implements PeonLifecycleFactory
{
  private final KubernetesPeonClient client;
  private final TaskLogs taskLogs;
  private final ObjectMapper mapper;

  public KubernetesPeonLifecycleFactory(
      KubernetesPeonClient client,
      TaskLogs taskLogs,
      ObjectMapper mapper
  )
  {
    this.client = client;
    this.taskLogs = taskLogs;
    this.mapper = mapper;
  }

  @Override
  public KubernetesPeonLifecycle build(Task task, KubernetesPeonLifecycle.TaskStateListener stateListener)
  {
    return new KubernetesPeonLifecycle(
        task,
        client,
        taskLogs,
        mapper,
        stateListener
    );
  }
}
