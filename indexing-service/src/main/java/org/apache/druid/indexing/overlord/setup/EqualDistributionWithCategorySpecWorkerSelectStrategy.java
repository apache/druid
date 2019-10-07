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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.config.WorkerTaskRunnerConfig;

import javax.annotation.Nullable;
import java.util.Objects;

public class EqualDistributionWithCategorySpecWorkerSelectStrategy implements WorkerSelectStrategy
{
  private final WorkerCategorySpec workerCategorySpec;

  @JsonCreator
  public EqualDistributionWithCategorySpecWorkerSelectStrategy(
      @JsonProperty("workerCategorySpec") WorkerCategorySpec workerCategorySpec
  )
  {
    this.workerCategorySpec = workerCategorySpec;
  }

  @JsonProperty
  public WorkerCategorySpec getWorkerCategorySpec()
  {
    return workerCategorySpec;
  }

  @Nullable
  @Override
  public ImmutableWorkerInfo findWorkerForTask(
      final WorkerTaskRunnerConfig config,
      final ImmutableMap<String, ImmutableWorkerInfo> zkWorkers,
      final Task task
  )
  {
    return WorkerSelectUtils.selectWorker(
        task,
        zkWorkers,
        config,
        workerCategorySpec,
        EqualDistributionWorkerSelectStrategy::selectFromEligibleWorkers
    );
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final EqualDistributionWithCategorySpecWorkerSelectStrategy that = (EqualDistributionWithCategorySpecWorkerSelectStrategy) o;
    return Objects.equals(workerCategorySpec, that.workerCategorySpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(workerCategorySpec);
  }

  @Override
  public String toString()
  {
    return "EqualDistributionWithCategorySpecWorkerSelectStrategy{" +
           "workerCategorySpec=" + workerCategorySpec +
           '}';
  }
}
