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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * This utility class provides a thin runtime abstraction to pick between
 * - sun.misc.Unsafe.defineClass in Java 8,
 * - and MethodHandles.Lookup.defineClass in Java 9 and above,
 * while still providing compile-time support for both Java 8 and Java 9+.
 *
 * See also {@link ByteBufferUtils}
 */
public class DefineClassUtils
{
  private static final MethodHandle DEFINE_CLASS;
  private static final Exception DEFINE_CLASS_NOT_SUPPORTED_EXCEPTION;

  static {
    MethodHandle defineClass = null;
    Exception exception = null;
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      defineClass = getMethodHandle(lookup);
    }
    catch (ReflectiveOperationException | RuntimeException e) {
      exception = e;
    }
    if (defineClass != null) {
      DEFINE_CLASS = defineClass;
      DEFINE_CLASS_NOT_SUPPORTED_EXCEPTION = null;
    } else {
      DEFINE_CLASS = null;
      DEFINE_CLASS_NOT_SUPPORTED_EXCEPTION = exception;
    }
  }

  /**
   * "Compile" a MethodHandle that is equivalent to the following closure:
   *
   *  Class<?> defineClass(Class targetClass, String className, byte[] byteCode) {
   *    MethodHandles.Lookup targetClassLookup = MethodHandles.privateLookupIn(targetClass, lookup);
   *    return targetClassLookup.defineClass(byteCode);
   *  }
   */
  private static MethodHandle getMethodHandle(MethodHandles.Lookup lookup) throws ReflectiveOperationException
  {
    // this is getting meta
    MethodHandle defineClass = lookup.unreflect(MethodHandles.Lookup.class.getMethod("defineClass", byte[].class));
    MethodHandle privateLookupIn = lookup.findStatic(
        MethodHandles.class,
        "privateLookupIn",
        MethodType.methodType(MethodHandles.Lookup.class, Class.class, MethodHandles.Lookup.class)
    );

    // bind privateLookupIn lookup argument to this method's lookup
    // privateLookupIn = (Class targetClass) -> privateLookupIn(MethodHandles.privateLookupIn(targetClass, lookup))
    privateLookupIn = MethodHandles.insertArguments(privateLookupIn, 1, lookup);

    // defineClass = (Class targetClass, byte[] byteCode) -> privateLookupIn(targetClass).defineClass(byteCode)
    defineClass = MethodHandles.filterArguments(defineClass, 0, privateLookupIn);

    // add a dummy String argument to match the corresponding JDK8 version
    // defineClass = (Class targetClass, byte[] byteCode, String className) -> defineClass(targetClass, byteCode)
    defineClass = MethodHandles.dropArguments(defineClass, 2, String.class);
    return defineClass;
  }

  public static Class defineClass(
      Class<?> targetClass,
      byte[] byteCode,
      String className
  )
  {
    if (DEFINE_CLASS == null) {
      throw new UnsupportedOperationException(
          "defineClass is not supported on this platform, "
          + "because internal Java APIs are not compatible with this Druid version",
          DEFINE_CLASS_NOT_SUPPORTED_EXCEPTION
      );
    }

    try {
      return (Class) DEFINE_CLASS.invokeExact(targetClass, byteCode, className);
    }
    catch (Throwable t) {
      throw new UnsupportedOperationException("Unable to define specialized class: " + className, t);
    }
  }
}
