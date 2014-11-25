/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableZkWorker;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;

/**
 * The {@link io.druid.indexing.overlord.RemoteTaskRunner} uses this class to select a worker to assign tasks to.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = FillCapacityWorkerSelectStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "fillCapacity", value = FillCapacityWorkerSelectStrategy.class),
    @JsonSubTypes.Type(name = "fillCapacityWithAffinity", value = FillCapacityWithAffinityWorkerSelectStrategy.class)
})
public interface WorkerSelectStrategy
{
  /**
   * Customizable logic for selecting a worker to run a task.
   *
   * @param config    A config for running remote tasks
   * @param zkWorkers An immutable map of workers to choose from.
   * @param task      The task to assign.
   *
   * @return A {@link io.druid.indexing.overlord.ImmutableZkWorker} to run the task if one is available.
   */
  public Optional<ImmutableZkWorker> findWorkerForTask(
      final RemoteTaskRunnerConfig config,
      final ImmutableMap<String, ImmutableZkWorker> zkWorkers,
      final Task task
  );
}
