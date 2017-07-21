/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.common.utils;

import io.druid.java.util.common.UOE;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;

public class VMUtils
{
  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

  public static boolean isThreadCpuTimeEnabled()
  {
    return THREAD_MX_BEAN.isThreadCpuTimeSupported() && THREAD_MX_BEAN.isThreadCpuTimeEnabled();
  }

  public static long safeGetThreadCpuTime()
  {
    if (!isThreadCpuTimeEnabled()) {
      return 0L;
    } else {
      return getCurrentThreadCpuTime();
    }
  }

  /**
   * Returns the total CPU time for current thread.
   * This method should be called after verifying that cpu time measurement for current thread is supported by JVM
   *
   * @return total CPU time for the current thread in nanoseconds.
   *
   * @throws UnsupportedOperationException if the Java virtual machine does not support CPU time measurement for
   * the current thread.
   */
  public static long getCurrentThreadCpuTime()
  {
    return THREAD_MX_BEAN.getCurrentThreadCpuTime();
  }

  public static long getMaxDirectMemory() throws UnsupportedOperationException
  {
    try {
      Class<?> vmClass = Class.forName("sun.misc.VM");
      Object maxDirectMemoryObj = vmClass.getMethod("maxDirectMemory").invoke(null);

      if (maxDirectMemoryObj == null || !(maxDirectMemoryObj instanceof Number)) {
        throw new UOE("Cannot determine maxDirectMemory from [%s]", maxDirectMemoryObj);
      } else {
        return ((Number) maxDirectMemoryObj).longValue();
      }
    }
    catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException("No VM class, cannot do memory check.", e);
    }
    catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException("VM.maxDirectMemory doesn't exist, cannot do memory check.", e);
    }
    catch (InvocationTargetException e) {
      throw new UnsupportedOperationException("static method shouldn't throw this", e);
    }
    catch (IllegalAccessException e) {
      throw new UnsupportedOperationException("public method, shouldn't throw this", e);
    }
  }
}
