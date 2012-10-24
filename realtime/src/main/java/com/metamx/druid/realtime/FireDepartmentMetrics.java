package com.metamx.druid.realtime;

import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class FireDepartmentMetrics
{
  private final AtomicLong processedCount = new AtomicLong(0);
  private final AtomicLong thrownAwayCount = new AtomicLong(0);
  private final AtomicLong unparseableCount = new AtomicLong(0);
  private final AtomicLong rowOutputCount = new AtomicLong(0);

  public void incrementProcessed()
  {
    processedCount.incrementAndGet();
  }

  public void incrementThrownAway()
  {
    thrownAwayCount.incrementAndGet();
  }

  public void incrementUnparseable()
  {
    unparseableCount.incrementAndGet();
  }

  public void incrementRowOutputCount(long numRows)
  {
    rowOutputCount.addAndGet(numRows);
  }

  public long processed()
  {
    return processedCount.get();
  }

  public long thrownAway()
  {
    return thrownAwayCount.get();
  }

  public long unparseable()
  {
    return unparseableCount.get();
  }

  public long rowOutput()
  {
    return rowOutputCount.get();
  }

  public FireDepartmentMetrics snapshot()
  {
    final FireDepartmentMetrics retVal = new FireDepartmentMetrics();
    retVal.processedCount.set(processedCount.get());
    retVal.thrownAwayCount.set(thrownAwayCount.get());
    retVal.unparseableCount.set(unparseableCount.get());
    retVal.rowOutputCount.set(rowOutputCount.get());
    return retVal;
  }
}
