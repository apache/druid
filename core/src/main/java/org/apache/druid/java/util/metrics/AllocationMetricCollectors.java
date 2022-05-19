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

import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;

class AllocationMetricCollectors
{
  private static final Logger log = new Logger(AllocationMetricCollectors.class);
  private static Method getThreadAllocatedBytes;
  private static ThreadMXBean threadMXBean;
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

  @Nullable
  static AllocationMetricCollector getAllocationMetricCollector()
  {
    if (initialized) {
      return new AllocationMetricCollector(getThreadAllocatedBytes, threadMXBean);
    }
    return null;
  }
}
