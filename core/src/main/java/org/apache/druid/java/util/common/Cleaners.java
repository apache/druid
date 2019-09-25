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

public class Cleaners
{
  public interface Cleanable
  {
    void clean();
  }

  public interface Cleaner
  {
    Cleanable register(Object object, Runnable runnable);
  }

  private static final Cleaner CLEANER;
  private static final RuntimeException CLEANER_NOT_SUPPORTED_EXCEPTION;

  static {
    Cleaners.Cleaner cleaner = null;
    RuntimeException exception = null;
    try {
      cleaner = takeMeToTheCleaners();
    }
    catch (RuntimeException e) {
      exception = e;
    }
    if (cleaner != null) {
      CLEANER = cleaner;
      CLEANER_NOT_SUPPORTED_EXCEPTION = null;
    } else {
      CLEANER = null;
      CLEANER_NOT_SUPPORTED_EXCEPTION = exception;
    }
  }

  private static Cleaner takeMeToTheCleaners()
  {
    final MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      if (JvmUtils.isIsJava9Compatible()) {
        return lookupCleanerJava9(lookup);
      } else {
        return lookupCleanerJava8(lookup);
      }
    }
    catch (ReflectiveOperationException | RuntimeException e) {
      throw new UnsupportedOperationException("Cleaning is not support on this platform, because internal " +
                                              "Java APIs are not compatible with this Druid version", e);
    }
  }

  private static Cleaner lookupCleanerJava9(MethodHandles.Lookup lookup) throws ReflectiveOperationException
  {
    Class<?> cleaner = Class.forName("java.lang.ref.Cleaner");
    Class<?> cleanable = Class.forName("java.lang.ref.Cleaner$Cleanable");

    MethodHandle create = lookup.findStatic(cleaner, "create", MethodType.methodType(cleaner));

    Object theCleaner;
    try {
      theCleaner = create.invoke();
    }
    catch (Throwable t) {
      throw new RuntimeException("Unable to create cleaner", t);
    }

    MethodHandle register = lookup.findVirtual(
        cleaner,
        "register",
        MethodType.methodType(cleanable, Object.class, Runnable.class)
    ).bindTo(theCleaner);

    MethodHandle clean = lookup.findVirtual(cleanable, "clean", MethodType.methodType(void.class));

    return new CleanerImpl(register, clean);
  }

  private static Cleaner lookupCleanerJava8(MethodHandles.Lookup lookup) throws ReflectiveOperationException
  {
    Class<?> cleaner = Class.forName("sun.misc.Cleaner");
    MethodHandle register = lookup.findStatic(
        cleaner,
        "create",
        MethodType.methodType(cleaner, Object.class, Runnable.class)
    );

    MethodHandle clean = lookup.findVirtual(cleaner, "clean", MethodType.methodType(void.class));
    return new CleanerImpl(register, clean);
  }

  public static Cleanable register(Object object, Runnable runnable)
  {
    if (CLEANER == null) {
      throw new UnsupportedOperationException(CLEANER_NOT_SUPPORTED_EXCEPTION);
    }

    return CLEANER.register(object, runnable);
  }

  private static class CleanerImpl implements Cleaner
  {
    private final MethodHandle register;
    private final MethodHandle clean;

    private CleanerImpl(MethodHandle register, MethodHandle clean)
    {
      this.register = register;
      this.clean = clean;
    }

    @Override
    public Cleanable register(Object object, Runnable runnable)
    {
      try {
        Object cleanable = (Object) register.invoke(object, runnable);
        return createCleanable(clean, cleanable);
      }
      catch (Throwable t) {
        throw new RuntimeException("Unable to register cleaning action", t);
      }
    }

    private static Cleanable createCleanable(MethodHandle clean, Object cleanable)
    {
      return () -> {
        try {
          clean.invoke(cleanable);
        }
        catch (Throwable t) {
          throw new RuntimeException("Unable to run cleaning action", t);
        }
      };
    }
  }
}
