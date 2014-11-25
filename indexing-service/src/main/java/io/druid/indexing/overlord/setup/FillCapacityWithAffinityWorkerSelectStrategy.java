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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableZkWorker;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;

import java.util.List;
import java.util.Set;

/**
 */
public class FillCapacityWithAffinityWorkerSelectStrategy extends FillCapacityWorkerSelectStrategy
{
  private final FillCapacityWithAffinityConfig affinityConfig;
  private final Set<String> affinityWorkerHosts = Sets.newHashSet();

  @JsonCreator
  public FillCapacityWithAffinityWorkerSelectStrategy(
      @JsonProperty("affinityConfig") FillCapacityWithAffinityConfig affinityConfig
  )
  {
    this.affinityConfig = affinityConfig;
    for (List<String> affinityWorkers : affinityConfig.getAffinity().values()) {
      for (String affinityWorker : affinityWorkers) {
        this.affinityWorkerHosts.add(affinityWorker);
      }
    }
  }

  @JsonProperty
  public FillCapacityWithAffinityConfig getAffinityConfig()
  {
    return affinityConfig;
  }

  @Override
  public Optional<ImmutableZkWorker> findWorkerForTask(
      final RemoteTaskRunnerConfig config,
      final ImmutableMap<String, ImmutableZkWorker> zkWorkers,
      final Task task
  )
  {
    // don't run other datasources on affinity workers; we only want our configured datasources to run on them
    ImmutableMap.Builder<String, ImmutableZkWorker> builder = new ImmutableMap.Builder<>();
    for (String workerHost : zkWorkers.keySet()) {
      if (!affinityWorkerHosts.contains(workerHost)) {
        builder.put(workerHost, zkWorkers.get(workerHost));
      }
    }
    ImmutableMap<String, ImmutableZkWorker> eligibleWorkers = builder.build();

    List<String> workerHosts = affinityConfig.getAffinity().get(task.getDataSource());
    if (workerHosts == null) {
      return super.findWorkerForTask(config, eligibleWorkers, task);
    }

    ImmutableMap.Builder<String, ImmutableZkWorker> affinityBuilder = new ImmutableMap.Builder<>();
    for (String workerHost : workerHosts) {
      ImmutableZkWorker zkWorker = zkWorkers.get(workerHost);
      if (zkWorker != null) {
        affinityBuilder.put(workerHost, zkWorker);
      }
    }
    ImmutableMap<String, ImmutableZkWorker> affinityWorkers = affinityBuilder.build();

    if (!affinityWorkers.isEmpty()) {
      Optional<ImmutableZkWorker> retVal = super.findWorkerForTask(config, affinityWorkers, task);
      if (retVal.isPresent()) {
        return retVal;
      }
    }

    return super.findWorkerForTask(config, eligibleWorkers, task);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FillCapacityWithAffinityWorkerSelectStrategy that = (FillCapacityWithAffinityWorkerSelectStrategy) o;

    if (affinityConfig != null ? !affinityConfig.equals(that.affinityConfig) : that.affinityConfig != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return affinityConfig != null ? affinityConfig.hashCode() : 0;
  }
}

