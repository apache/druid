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

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.apache.druid.java.util.common.logger.Logger;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

class AllocationMetricCollector
{
  private static final Logger log = new Logger(AllocationMetricCollector.class);
  private static Method getThreadAllocatedBytes;
  private static ThreadMXBean threadMXBean;

  private Map<Long, Long> previousResults;
  private static boolean initialized = false;

  static {
    try {
      // classes in the sun.* packages are not part of the public/supported Java API and should not be used directly.
      threadMXBean = ManagementFactory.getThreadMXBean();
      getThreadAllocatedBytes = threadMXBean.getClass().getMethod("getThreadAllocatedBytes", long[].class);
      getThreadAllocatedBytes.setAccessible(true);
      getThreadAllocatedBytes.invoke(threadMXBean, (Object) threadMXBean.getAllThreadIds());
      initialized = true;
    }
    catch (Exception e) {
      log.warn(e, "Cannot initialize %s", AllocationMetricCollector.class.getName());
    }
  }

  AllocationMetricCollector()
  {
    try {
      if (initialized) {
        previousResults = new Long2LongOpenHashMap();
      }
    }
    catch (Exception e) {
      log.warn(e, "Cannot initialize %s", AllocationMetricCollector.class.getName());
    }
  }

  public Optional<Long> calculateDelta()
  {
    if (!initialized) {
      return Optional.empty();
    }
    try {
      long[] allThreadIds = threadMXBean.getAllThreadIds();
      // the call time depends on number of threads, for 500 threads the estimated time is 4 ms
      long[] bytes = (long[]) getThreadAllocatedBytes.invoke(threadMXBean, (Object) allThreadIds);
      long sum = 0;
      for (int i = 0; i < allThreadIds.length; i++) {
        long threadId = allThreadIds[i];
        Long previous = previousResults.get(threadId);
        Long current = bytes[i];
        // a) some threads can be terminated and their ids won't be present
        // b) if new threads ids can collide with terminated threads ids then the current allocation can be lesser than
        // before
        if (previous == null || previous > current) {
          sum += current;
        } else if (previous <= current) {
          sum += current - previous;
        }
        this.previousResults.put(threadId, current);
      }
      // remove terminated thread ids
      previousResults.keySet().retainAll(Arrays.stream(allThreadIds).boxed().collect(Collectors.toSet()));
      return Optional.of(sum);
    }
    catch (ReflectiveOperationException e) {
      log.error(e, "Cannot calculate delta");
    }
    return Optional.empty();
  }
}
