/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.java.util.emitter.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.UnsignedInts;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class to accumulate simple stats of some time points. All methods are safe to use from multiple threads.
 */
public class ConcurrentTimeCounter
{
  /** Lower 32 bits for sum of {@link #add}ed times, higher 32 bits for the count */
  private final AtomicLong timeSumAndCount = new AtomicLong(0L);
  /** Lower 32 bits for the max {@link #add}ed time, 63th bit for indication if any value is added. */
  private final AtomicLong max = new AtomicLong(-1);
  /** Similar to {@link #max} */
  private final AtomicLong min = new AtomicLong(-1);

  public void add(int time)
  {
    long x = (1L << 32) | time;
    timeSumAndCount.addAndGet(x);
    updateMax(time);
    updateMin(time);
  }

  private void updateMax(int time)
  {
    long max;
    do {
      max = this.max.get();
      if (max >= 0 && ((int) max) >= time) {
        return;
      }
    } while (!this.max.compareAndSet(max, UnsignedInts.toLong(time)));
  }

  private void updateMin(int time)
  {
    long min;
    do {
      min = this.min.get();
      if (min >= 0 && ((int) min) <= time) {
        return;
      }
    } while (!this.min.compareAndSet(min, UnsignedInts.toLong(time)));
  }

  @VisibleForTesting
  long getTimeSumAndCount()
  {
    return timeSumAndCount.get();
  }

  public long getTimeSumAndCountAndReset()
  {
    return timeSumAndCount.getAndSet(0L);
  }

  /**
   * Returns the max time {@link #add}ed since the previous call to this method or since the creation of this object,
   * or null if no times were added.
   */
  @Nullable
  public Integer getAndResetMaxTime()
  {
    long max = this.max.getAndSet(-1);
    if (max >= 0) {
      return (int) max;
    } else {
      return null;
    }
  }

  /**
   * Returns the min time {@link #add}ed since the previous call to this method or since the creation of this object,
   * or null if no times were added.
   */
  @Nullable
  public Integer getAndResetMinTime()
  {
    long min = this.min.getAndSet(-1);
    if (min >= 0) {
      return (int) min;
    } else {
      return null;
    }
  }

  public static int timeSum(long timeSumAndCount)
  {
    return (int) timeSumAndCount;
  }

  public static int count(long timeSumAndCount)
  {
    return (int) (timeSumAndCount >> 32);
  }
}
