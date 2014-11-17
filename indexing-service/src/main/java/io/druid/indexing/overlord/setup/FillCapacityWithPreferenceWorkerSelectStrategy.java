package io.druid.indexing.overlord.setup;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableZkWorker;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;

/**
 */
public class FillCapacityWithPreferenceWorkerSelectStrategy extends FillCapacityWorkerSelectStrategy
{
  private final FillCapacityWithPreferenceConfig preferenceConfig;
  private final RemoteTaskRunnerConfig config;

  @Inject
  public FillCapacityWithPreferenceWorkerSelectStrategy(
      FillCapacityWithPreferenceConfig preferenceConfig,
      RemoteTaskRunnerConfig config
  )
  {
    super(config);
    this.preferenceConfig = preferenceConfig;
    this.config = config;
  }

  @Override
  public Optional<ImmutableZkWorker> findWorkerForTask(
      ImmutableMap<String, ImmutableZkWorker> zkWorkers, Task task
  )
  {
    String preferredZkWorker = preferenceConfig.getPreferences().get(task.getDataSource());

    ImmutableZkWorker zkWorker = zkWorkers.get(preferredZkWorker);

    final String minWorkerVer = config.getMinWorkerVersion();
    if (zkWorker != null && zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)) {
      return Optional.of(zkWorker);
    }

    return super.findWorkerForTask(zkWorkers, task);
  }
}
