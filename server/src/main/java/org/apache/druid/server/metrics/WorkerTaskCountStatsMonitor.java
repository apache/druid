package org.apache.druid.server.metrics;

import com.google.inject.Inject;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

public class WorkerTaskCountStatsMonitor extends AbstractMonitor
{
  private static final EmittingLogger LOGGER = new EmittingLogger(WorkerTaskCountStatsMonitor.class);

  private final WorkerTaskCountStatsProvider statsProvider;

  @Inject
  public WorkerTaskCountStatsMonitor(
      WorkerTaskCountStatsProvider statsProvider
  )
  {
    this.statsProvider = statsProvider;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    emit(emitter, "middleManager/task/failed/count", statsProvider.getSuccessfulTaskCount());
    emit(emitter, "middleManager/task/running/count", statsProvider.getFailedTaskCount());
    emit(emitter, "middleManager/task/success/count", statsProvider.getRunningTaskCount());
    return true;
  }

  private void emit(ServiceEmitter emitter, String key, Long count)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    if (count != null) {
      LOGGER.info("%s: [%d]", key, count);
      emitter.emit(builder.build(key, count));
    }
  }
}
