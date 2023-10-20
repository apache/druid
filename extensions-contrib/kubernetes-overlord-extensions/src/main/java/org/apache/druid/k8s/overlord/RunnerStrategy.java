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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.indexing.common.task.Task;

/**
 * A strategy for selecting a runner type to run a task, it could be k8s or worker.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = KubernetesRunnerStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "k8s", value = KubernetesRunnerStrategy.class),
    @JsonSubTypes.Type(name = "worker", value = WorkerRunnerStrategy.class),
    @JsonSubTypes.Type(name = "taskSpec", value = MixRunnerStrategy.class)
})
public interface RunnerStrategy
{
  enum RunnerType
  {
    KUBERNETES_RUNNER_TYPE("k8s"),
    WORKER_REMOTE_RUNNER_TYPE("remote"),
    WORKER_HTTPREMOTE_RUNNER_TYPE("httpRemote");

    private final String type;

    RunnerType(String type)
    {
      this.type = type;
    }

    public String getType()
    {
      return type;
    }
  }

  RunnerType getRunnerTypeForTask(Task task);
}
