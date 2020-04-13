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

package org.apache.druid.java.util.metrics;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.apache.druid.java.util.common.logger.Logger;

import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;

class AllocationMetricCollector
{
  private static final Logger log = new Logger(AllocationMetricCollector.class);

  private static final int NO_DATA = -1;

  private final Method getThreadAllocatedBytes;
  private final ThreadMXBean threadMXBean;

  private Long2LongMap previousResults;

  AllocationMetricCollector(Method method, ThreadMXBean threadMXBean)
  {
    this.getThreadAllocatedBytes = method;
    this.threadMXBean = threadMXBean;

    previousResults = new Long2LongOpenHashMap();
    previousResults.defaultReturnValue(NO_DATA);
  }

  /**
   * Uses getThreadAllocatedBytes internally {@link com.sun.management.ThreadMXBean#getThreadAllocatedBytes}.
   *
   * Tests show the call to getThreadAllocatedBytes for a single thread ID out of 500 threads running takes around
   * 9000 ns (in the worst case), which for 500 IDs should take 500*9000/1000/1000 = 4.5 ms to the max.
   * AllocationMetricCollector takes linear time to calculate delta, for 500 threads it's negligible.
   * See the default emitting period {@link MonitorSchedulerConfig#getEmitterPeriod}.
   *
   * @return all threads summed allocated bytes delta
   */
  long calculateDelta()
  {
    try {
      long[] allThreadIds = threadMXBean.getAllThreadIds();
      // the call time depends on number of threads, for 500 threads the estimated time is 4 ms
      long[] bytes = (long[]) getThreadAllocatedBytes.invoke(threadMXBean, (Object) allThreadIds);
      long sum = 0;
      Long2LongMap newResults = new Long2LongOpenHashMap();
      newResults.defaultReturnValue(NO_DATA);
      for (int i = 0; i < allThreadIds.length; i++) {
        long threadId = allThreadIds[i];
        long previous = previousResults.get(threadId);
        long current = bytes[i];
        newResults.put(threadId, current);
        // a) some threads can be terminated and their ids won't be present
        // b) if new threads ids can collide with terminated threads ids then the current allocation can be lesser than
        // before
        if (previous == NO_DATA || previous > current) {
          sum += current;
        } else {
          sum += current - previous;
        }
      }
      previousResults = newResults;
      return sum;
    }
    catch (ReflectiveOperationException e) {
      log.error(e, "Cannot calculate delta"); // it doesn't make sense after initialization is complete
    }
    return 0;
  }
}
