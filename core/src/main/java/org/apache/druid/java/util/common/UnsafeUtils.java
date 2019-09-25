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

package org.apache.druid.java.util.common;

import java.lang.reflect.Field;

/**
 * This utiliy class enables runtime access to sun.misc.Unsafe using reflection,
 * since Unsafe may not be directly accessible in newer JDK versions
 */
public class UnsafeUtils
{
  private static final Object UNSAFE;
  private static final Class<?> UNSAFE_CLASS;
  private static final Exception UNSAFE_NOT_SUPPORTED_EXCEPTION;
  private static final String MESSAGE = "sun.misc.Unsafe is not supported on this platform, because internal " +
                                        "Java APIs are not compatible with this Druid version";

  static {
    Object theUnsafe = null;
    Class<?> unsafeClass = null;
    Exception exception = null;

    try {
      unsafeClass = Class.forName("sun.misc.Unsafe");
      Field f = unsafeClass.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      theUnsafe = f.get(null);
    }
    catch (ReflectiveOperationException | RuntimeException e) {
      exception = e;
    }

    if (theUnsafe != null) {
      UNSAFE = theUnsafe;
      UNSAFE_CLASS = unsafeClass;
      UNSAFE_NOT_SUPPORTED_EXCEPTION = exception;
    } else {
      UNSAFE_CLASS = null;
      UNSAFE = null;
      UNSAFE_NOT_SUPPORTED_EXCEPTION = exception;
    }
  }

  public static Object theUnsafe()
  {
    if (UNSAFE != null) {
      return UNSAFE;
    } else {
      throw new UnsupportedOperationException(MESSAGE, UNSAFE_NOT_SUPPORTED_EXCEPTION);
    }
  }

  public static Class<?> theUnsafeClass()
  {
    if (UNSAFE_CLASS != null) {
      return UNSAFE_CLASS;
    } else {
      throw new UnsupportedOperationException(MESSAGE, UNSAFE_NOT_SUPPORTED_EXCEPTION);
    }
  }
}
