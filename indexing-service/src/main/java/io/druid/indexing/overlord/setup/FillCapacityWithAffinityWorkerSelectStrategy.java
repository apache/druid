package io.druid.indexing.overlord.setup;

import com.google.api.client.util.Sets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
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


  @Inject
  public FillCapacityWithAffinityWorkerSelectStrategy(
      FillCapacityWithAffinityConfig affinityConfig,
      RemoteTaskRunnerConfig config
  )
  {
    super(config);
    this.affinityConfig = affinityConfig;
    for (List<String> affinityWorkers : affinityConfig.getPreferences().values()) {
      for (String affinityWorker : affinityWorkers) {
        this.affinityWorkerHosts.add(affinityWorker);
      }
    }
  }

  @Override
  public Optional<ImmutableZkWorker> findWorkerForTask(
      ImmutableMap<String, ImmutableZkWorker> zkWorkers, Task task
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

    List<String> workerHosts = affinityConfig.getPreferences().get(task.getDataSource());
    if (workerHosts == null) {
      return super.findWorkerForTask(eligibleWorkers, task);
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
      Optional<ImmutableZkWorker> retVal = super.findWorkerForTask(affinityWorkers, task);
      if (retVal.isPresent()) {
        return retVal;
      }
    }

    return super.findWorkerForTask(eligibleWorkers, task);

  }
}

