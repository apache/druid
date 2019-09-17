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
  private static final Logger LOG = new Logger(AllocationMetricCollectors.class);
  @SuppressWarnings("SSBasedInspection")
  private static Method GET_THREAD_ALLOCATED_BYTES;
  @SuppressWarnings("SSBasedInspection")
  private static ThreadMXBean THREAD_MX_BEAN;
  @SuppressWarnings("SSBasedInspection")
  private static boolean INITIALIZED = false;

  static {
    try {
      // classes in the sun.* packages are not part of the public/supported Java API and should not be used directly.
      THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
      GET_THREAD_ALLOCATED_BYTES = THREAD_MX_BEAN.getClass().getMethod("getThreadAllocatedBytes", long[].class);
      GET_THREAD_ALLOCATED_BYTES.setAccessible(true);
      GET_THREAD_ALLOCATED_BYTES.invoke(THREAD_MX_BEAN, (Object) THREAD_MX_BEAN.getAllThreadIds());
      INITIALIZED = true;
    }
    catch (Exception e) {
      LOG.warn(e, "Cannot initialize %s", AllocationMetricCollector.class.getName());
    }
  }

  @Nullable
  static AllocationMetricCollector getAllocationMetricCollector()
  {
    if (INITIALIZED) {
      return new AllocationMetricCollector(GET_THREAD_ALLOCATED_BYTES, THREAD_MX_BEAN);
    }
    return null;
  }
}
