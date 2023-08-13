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

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.io.Closer;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Methods in this class could have belonged to {@link Closer}, but not editing
 * that class to keep its source close to Guava source.
 */
public final class CloseableUtils
{
  /**
   * Call method instead of code like
   *
   * first.close();
   * second.close();
   *
   * to have safety of {@link Closer}, but without associated boilerplate code
   * of creating a Closer and registering objects in it.
   */
  public static void closeAll(Closeable first, Closeable... others) throws IOException
  {
    final List<Closeable> closeables = new ArrayList<>(others.length + 1);
    closeables.add(first);
    closeables.addAll(Arrays.asList(others));
    closeAll(closeables);
  }

  /**
   * Close all the provided {@param closeables}, from first to last.
   */
  public static <T extends Closeable> void closeAll(Iterable<T> closeables) throws IOException
  {
    final Closer closer = Closer.create();

    // Register in reverse order, so we close from first to last.
    closer.registerAll(Lists.reverse(Lists.newArrayList(closeables)));
    closer.close();
  }

  /**
   * Like {@link Closeable#close()}, but guaranteed to throw {@param caught}. Will add any exceptions encountered
   * during closing to {@param caught} using {@link Throwable#addSuppressed(Throwable)}.
   *
   * Should be used like {@code throw CloseableUtils.closeInCatch(e, closeable)}. (The "throw" is important for
   * reachability detection.)
   */
  public static <E extends Throwable> RuntimeException closeInCatch(
      final E caught,
      @Nullable final Closeable closeable
  ) throws E
  {
    if (caught == null) {
      // Incorrect usage; throw an exception with an error message that may be useful to the programmer.
      final RuntimeException e1 = new IllegalStateException("Must be called with non-null caught exception");

      if (closeable != null) {
        try {
          closeable.close();
        }
        catch (Throwable e2) {
          e1.addSuppressed(e2);
        }
      }

      throw e1;
    }

    if (closeable != null) {
      try {
        closeable.close();
      }
      catch (Throwable e) {
        caught.addSuppressed(e);
      }
    }

    throw caught;
  }

  /**
   * Like {@link #closeInCatch} but wraps {@param caught} in a {@link RuntimeException} if it is a checked exception.
   */
  public static <E extends Throwable> RuntimeException closeAndWrapInCatch(
      final E caught,
      @Nullable final Closeable closeable
  )
  {
    try {
      throw closeInCatch(caught, closeable);
    }
    catch (RuntimeException | Error e) {
      // Unchecked exception.
      throw e;
    }
    catch (Throwable e) {
      // Checked exception; must wrap.
      throw new RuntimeException(e);
    }
  }

  /**
   * Like {@link Closeable#close()} but wraps IOExceptions in RuntimeExceptions.
   */
  public static void closeAndWrapExceptions(@Nullable final Closeable closeable)
  {
    if (closeable == null) {
      return;
    }

    try {
      closeable.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Like {@link Closeable#close()} but sends any exceptions to the provided Consumer and then returns quietly.
   *
   * If the Consumer throws an exception, that exception is thrown by this method. So if your intent is to chomp
   * exceptions, you should avoid writing a Consumer that might throw an exception.
   */
  public static void closeAndSuppressExceptions(
      @Nullable final Closeable closeable,
      final Consumer<Throwable> chomper
  )
  {
    if (closeable == null) {
      return;
    }

    try {
      closeable.close();
    }
    catch (Throwable e) {
      chomper.accept(e);
    }
  }

  private CloseableUtils()
  {
  }
}
