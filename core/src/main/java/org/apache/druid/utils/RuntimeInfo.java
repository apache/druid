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

package org.apache.druid.utils;

import org.apache.druid.java.util.common.UOE;

import java.lang.reflect.InvocationTargetException;

public class RuntimeInfo
{
  public int getAvailableProcessors()
  {
    return Runtime.getRuntime().availableProcessors();
  }

  public long getMaxHeapSizeBytes()
  {
    return Runtime.getRuntime().maxMemory();
  }

  public long getTotalHeapSizeBytes()
  {
    return Runtime.getRuntime().totalMemory();
  }

  public long getFreeHeapSizeBytes()
  {
    return Runtime.getRuntime().freeMemory();
  }

  public long getDirectMemorySizeBytes()
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
