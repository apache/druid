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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.ObjectUtils;

import javax.annotation.Nullable;

public class KubernetesAndWorkerTaskRunnerConfig
{
  /**
   * Select which runner type a task would run on, options are k8s or worker.
   */
  @JsonProperty
  private RunnerStrategy runnerStrategy;

  @JsonCreator
  public KubernetesAndWorkerTaskRunnerConfig(
      @JsonProperty("runnerStrategy") @Nullable RunnerStrategy runnerStrategy
  )
  {
    this.runnerStrategy = ObjectUtils.defaultIfNull(runnerStrategy, new KubernetesRunnerStrategy());
  }

  public RunnerStrategy getRunnerStrategy()
  {
    return runnerStrategy;
  }

}
