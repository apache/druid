package org.apache.druid.server;

import org.apache.druid.guice.LazySingleton;
import org.apache.druid.server.metrics.QueryCountStatsProvider;

import java.util.concurrent.atomic.AtomicLong;

@LazySingleton
public class BaseQueryCountResource implements QueryCountStatsProvider, QueryMetricCounter
{
  private final AtomicLong successfulQueryCount = new AtomicLong();
  private final AtomicLong failedQueryCount = new AtomicLong();
  private final AtomicLong interruptedQueryCount = new AtomicLong();
  private final AtomicLong timedOutQueryCount = new AtomicLong();

  @Override
  public long getSuccessfulQueryCount()
  {
    return successfulQueryCount.get();
  }

  @Override
  public long getFailedQueryCount()
  {
    return failedQueryCount.get();
  }

  @Override
  public long getInterruptedQueryCount()
  {
    return interruptedQueryCount.get();
  }

  @Override
  public long getTimedOutQueryCount()
  {
    return timedOutQueryCount.get();
  }

  @Override
  public void incrementSuccess()
  {
    successfulQueryCount.incrementAndGet();
  }

  @Override
  public void incrementFailed()
  {
    failedQueryCount.incrementAndGet();
  }

  @Override
  public void incrementInterrupted()
  {
    interruptedQueryCount.incrementAndGet();
  }

  @Override
  public void incrementTimedOut()
  {
    timedOutQueryCount.incrementAndGet();
  }
}
