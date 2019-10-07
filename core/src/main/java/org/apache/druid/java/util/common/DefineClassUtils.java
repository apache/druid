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

import org.apache.druid.utils.JvmUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.security.ProtectionDomain;

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
      if (JvmUtils.isIsJava9Compatible()) {
        defineClass = defineClassJava9(lookup);
      } else {
        defineClass = defineClassJava8(lookup);
      }
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
  private static MethodHandle defineClassJava9(MethodHandles.Lookup lookup) throws ReflectiveOperationException
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

  /**
   * "Compile" a MethodHandle that is equilavent to:
   *
   *  Class<?> defineClass(Class targetClass, byte[] byteCode, String className) {
   *    return Unsafe.defineClass(
   *        className,
   *        byteCode,
   *        0,
   *        byteCode.length,
   *        targetClass.getClassLoader(),
   *        targetClass.getProtectionDomain()
   *    );
   *  }
   */
  private static MethodHandle defineClassJava8(MethodHandles.Lookup lookup) throws ReflectiveOperationException
  {
    MethodHandle defineClass = lookup.findVirtual(
        UnsafeUtils.theUnsafeClass(),
        "defineClass",
        MethodType.methodType(
            Class.class,
            String.class,
            byte[].class,
            int.class,
            int.class,
            ClassLoader.class,
            ProtectionDomain.class
        )
    ).bindTo(UnsafeUtils.theUnsafe());

    MethodHandle getProtectionDomain = lookup.unreflect(Class.class.getMethod("getProtectionDomain"));
    MethodHandle getClassLoader = lookup.unreflect(Class.class.getMethod("getClassLoader"));

    // apply getProtectionDomain and getClassLoader to the targetClass, modifying the methodHandle as follows:
    // defineClass = (String className, byte[] byteCode, int offset, int length, Class class1, Class class2) ->
    //   defineClass(className, byteCode, offset, length, class1.getClassLoader(), class2.getProtectionDomain())
    defineClass = MethodHandles.filterArguments(defineClass, 5, getProtectionDomain);
    defineClass = MethodHandles.filterArguments(defineClass, 4, getClassLoader);

    // duplicate the last argument to apply the methods above to the same class, modifying the methodHandle as follows:
    // defineClass = (String className, byte[] byteCode, int offset, int length, Class targetClass) ->
    //   defineClass(className, byteCode, offset, length, targetClass, targetClass)
    defineClass = MethodHandles.permuteArguments(
        defineClass,
        MethodType.methodType(Class.class, String.class, byte[].class, int.class, int.class, Class.class),
        0, 1, 2, 3, 4, 4
    );

    // set offset argument to 0, modifying the methodHandle as follows:
    // defineClass = (String className, byte[] byteCode, int length, Class targetClass) ->
    //   defineClass(className, byteCode, 0, length, targetClass)
    defineClass = MethodHandles.insertArguments(defineClass, 2, (int) 0);

    // JDK8 does not implement MethodHandles.arrayLength so we have to roll our own
    MethodHandle arrayLength = lookup.findStatic(
        lookup.lookupClass(),
        "getArrayLength",
        MethodType.methodType(int.class, byte[].class)
    );

    // apply arrayLength to the length argument, modifying the methodHandle as follows:
    // defineClass = (String className, byte[] byteCode1, byte[] byteCode2, Class targetClass) ->
    //   defineClass(className, byteCode1, byteCode2.length, targetClass)
    defineClass = MethodHandles.filterArguments(defineClass, 2, arrayLength);

    // duplicate the byteCode argument and reorder to match JDK9 signature, modifying the methodHandle as follows:
    // defineClass = (Class targetClass, byte[] byteCode, String className) ->
    //   defineClass(className, byteCode, byteCode, targetClass)
    defineClass = MethodHandles.permuteArguments(
        defineClass,
        MethodType.methodType(Class.class, Class.class, byte[].class, String.class),
        2, 1, 1, 0
    );

    return defineClass;
  }

  static int getArrayLength(byte[] bytes)
  {
    return bytes.length;
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
