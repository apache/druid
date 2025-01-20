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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.k8s.overlord.runnerstrategy.RunnerStrategy;


public class KubernetesAndWorkerTaskRunnerFactory implements TaskRunnerFactory<KubernetesAndWorkerTaskRunner>
{
  public static final String TYPE_NAME = "k8sAndWorker";

  private final KubernetesTaskRunnerFactory kubernetesTaskRunnerFactory;
  private final TaskRunnerFactory<? extends WorkerTaskRunner> taskRunnerFactory;
  private final RunnerStrategy runnerStrategy;

  private KubernetesAndWorkerTaskRunner runner;

  @Inject
  public KubernetesAndWorkerTaskRunnerFactory(
      KubernetesTaskRunnerFactory kubernetesTaskRunnerFactory,
      @Named("taskRunnerFactory") TaskRunnerFactory<? extends WorkerTaskRunner> taskRunnerFactory,
      RunnerStrategy runnerStrategy
  )
  {
    this.kubernetesTaskRunnerFactory = kubernetesTaskRunnerFactory;
    this.taskRunnerFactory = taskRunnerFactory;
    this.runnerStrategy = runnerStrategy;
  }

  @Override
  public KubernetesAndWorkerTaskRunner build()
  {
    runner = new KubernetesAndWorkerTaskRunner(
        kubernetesTaskRunnerFactory.build(),
        taskRunnerFactory.build(),
        runnerStrategy
    );
    return runner;
  }

  @Override
  public KubernetesAndWorkerTaskRunner get()
  {
    return runner;
  }
}
