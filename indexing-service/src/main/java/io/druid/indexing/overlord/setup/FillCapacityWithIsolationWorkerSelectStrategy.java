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
public class FillCapacityWithIsolationWorkerSelectStrategy extends FillCapacityWorkerSelectStrategy
{
  private final FillCapacityWithIsolationConfig isolationConfig;
  private final Set<String> isolatedWorkers = Sets.newHashSet();


  @Inject
  public FillCapacityWithIsolationWorkerSelectStrategy(
      FillCapacityWithIsolationConfig isolationConfig,
      RemoteTaskRunnerConfig config
  )
  {
    super(config);
    this.isolationConfig = isolationConfig;
    for (List<String> isolated : isolationConfig.getPreferences().values()) {
      for (String isolatedWorker : isolated) {
        isolatedWorkers.add(isolatedWorker);
      }
    }
  }

  @Override
  public Optional<ImmutableZkWorker> findWorkerForTask(
      ImmutableMap<String, ImmutableZkWorker> zkWorkers, Task task
  )
  {
    // remove isolatedWorkers
    ImmutableMap.Builder<String, ImmutableZkWorker> builder = new ImmutableMap.Builder<>();
    for (String workerHost : zkWorkers.keySet()) {
      if (!isolatedWorkers.contains(workerHost)) {
        builder.put(workerHost, zkWorkers.get(workerHost));
      }
    }
    ImmutableMap<String, ImmutableZkWorker> eligibleWorkers = builder.build();

    List<String> workerHosts = isolationConfig.getPreferences().get(task.getDataSource());
    if (workerHosts == null) {
      return super.findWorkerForTask(eligibleWorkers, task);
    }

    ImmutableMap.Builder<String, ImmutableZkWorker> isolatedBuilder = new ImmutableMap.Builder<>();
    for (String workerHost : workerHosts) {
      ImmutableZkWorker zkWorker = zkWorkers.get(workerHost);
      if (zkWorker != null) {
        isolatedBuilder.put(workerHost, zkWorker);
      }
    }
    ImmutableMap<String, ImmutableZkWorker> isolatedWorkers = isolatedBuilder.build();

    if (isolatedWorkers.isEmpty()) {
      return super.findWorkerForTask(eligibleWorkers, task);
    } else {
      return super.findWorkerForTask(isolatedWorkers, task);
    }
  }
}

