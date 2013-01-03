package com.metamx.druid.query;

import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MetricsEmittingExecutorService extends AbstractExecutorService
{
  private final ExecutorService base;
  private final ServiceEmitter emitter;
  private final ServiceMetricEvent.Builder metricBuilder;

  public MetricsEmittingExecutorService(
      ExecutorService base,
      ServiceEmitter emitter,
      ServiceMetricEvent.Builder metricBuilder
  )
  {
    this.base = base;
    this.emitter = emitter;
    this.metricBuilder = metricBuilder;
  }

  @Override
  public void shutdown()
  {
    base.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    return base.shutdownNow();
  }

  @Override
  public boolean isShutdown()
  {
    return base.isShutdown();
  }

  @Override
  public boolean isTerminated()
  {
    return base.isTerminated();
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException
  {
    return base.awaitTermination(l, timeUnit);
  }

  @Override
  public void execute(Runnable runnable)
  {
    emitMetrics();
    base.execute(runnable);
  }

  private void emitMetrics()
  {
    if (base instanceof ThreadPoolExecutor) {
      emitter.emit(metricBuilder.build("exec/backlog", ((ThreadPoolExecutor) base).getQueue().size()));
    }
  }
}
