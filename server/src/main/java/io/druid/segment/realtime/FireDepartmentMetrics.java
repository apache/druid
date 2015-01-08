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

package io.druid.segment.realtime;

import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class FireDepartmentMetrics
{
  private final AtomicLong processedCount = new AtomicLong(0);
  private final AtomicLong thrownAwayCount = new AtomicLong(0);
  private final AtomicLong unparseableCount = new AtomicLong(0);
  private final AtomicLong rowOutputCount = new AtomicLong(0);
  private final AtomicLong numPersists = new AtomicLong(0);
  private final AtomicLong persistTimeMillis = new AtomicLong(0);
  private final AtomicLong persistBackPressureMillis = new AtomicLong(0);

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

  public void incrementNumPersists()
  {
    numPersists.incrementAndGet();
  }

  public void incrementPersistTimeMillis(long millis)
  {
    persistTimeMillis.addAndGet(millis);
  }

  public void incrementPersistBackPressureMillis(long millis)
  {
    persistBackPressureMillis.addAndGet(millis);
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

  public long numPersists()
  {
    return numPersists.get();
  }

  public long persistTimeMillis()
  {
    return persistTimeMillis.get();
  }

  public long persistBackPressureMillis()
  {
    return persistBackPressureMillis.get();
  }

  public FireDepartmentMetrics snapshot()
  {
    final FireDepartmentMetrics retVal = new FireDepartmentMetrics();
    retVal.processedCount.set(processedCount.get());
    retVal.thrownAwayCount.set(thrownAwayCount.get());
    retVal.unparseableCount.set(unparseableCount.get());
    retVal.rowOutputCount.set(rowOutputCount.get());
    retVal.numPersists.set(numPersists.get());
    retVal.persistTimeMillis.set(persistTimeMillis.get());
    retVal.persistBackPressureMillis.set(persistBackPressureMillis.get());
    return retVal;
  }

  /**
   * merge other FireDepartmentMetrics, will modify this object's data
   *
   * @return this object
   */
  public FireDepartmentMetrics merge(FireDepartmentMetrics other)
  {
    Preconditions.checkNotNull(other, "Cannot merge a null FireDepartmentMetrics");
    FireDepartmentMetrics otherSnapshot = other.snapshot();
    processedCount.addAndGet(otherSnapshot.processed());
    thrownAwayCount.addAndGet(otherSnapshot.thrownAway());
    rowOutputCount.addAndGet(otherSnapshot.rowOutput());
    unparseableCount.addAndGet(otherSnapshot.unparseable());
    numPersists.addAndGet(otherSnapshot.numPersists());
    persistTimeMillis.addAndGet(otherSnapshot.persistTimeMillis());
    persistBackPressureMillis.addAndGet(otherSnapshot.persistBackPressureMillis());
    return this;
  }
}
