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

package org.apache.druid.common.guava;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 */
public class GuavaUtils
{
  private static final Logger log = new Logger(GuavaUtils.class);

  /**
   * To fix semantic difference of Longs.tryParse() from Long.parseLong (Longs.tryParse() returns null for '+' started
   * value)
   */
  @Nullable
  public static Long tryParseLong(@Nullable String string)
  {
    return Strings.isNullOrEmpty(string)
           ? null
           : Longs.tryParse(string.charAt(0) == '+' ? string.substring(1) : string);
  }

  /**
   * Like Guava's Enums.getIfPresent, with some differences.
   * <ul>
   * <li>Returns nullable rather than Optional</li>
   * <li>Does not require Guava 12</li>
   * </ul>
   */
  @Nullable
  public static <T extends Enum<T>> T getEnumIfPresent(final Class<T> enumClass, final String value)
  {
    Preconditions.checkNotNull(enumClass, "enumClass");
    Preconditions.checkNotNull(value, "value");

    for (T enumValue : enumClass.getEnumConstants()) {
      if (enumValue.name().equals(value)) {
        return enumValue;
      }
    }

    return null;
  }

  /**
   * If first argument is not null, return it, else return the other argument. Sort of like
   * {@link com.google.common.base.Objects#firstNonNull(Object, Object)} except will not explode if both arguments are
   * null.
   */
  @Nullable
  public static <T> T firstNonNull(@Nullable T arg1, @Nullable T arg2)
  {
    if (arg1 == null) {
      return arg2;
    }
    return arg1;
  }

  /**
   * Cancel futures manually, because sometime we can't cancel all futures in {@link com.google.common.util.concurrent.Futures.CombinedFuture}
   * automatically. Especially when we call {@link  com.google.common.util.concurrent.Futures#allAsList(Iterable)} to create a batch of
   * future.
   * @param mayInterruptIfRunning {@code true} if the thread executing this
   * task should be interrupted; otherwise, in-progress tasks are allowed
   * to complete
   * @param combinedFuture The combinedFuture that associated with futures
   * @param futures The futures that we want to cancel
   */
  public static <F extends Future<?>> void cancelAll(
      boolean mayInterruptIfRunning,
      @Nullable Future<?> combinedFuture,
      List<F> futures
  )
  {
    final List<Future> allFuturesToCancel = new ArrayList<>();
    allFuturesToCancel.add(combinedFuture);
    allFuturesToCancel.addAll(futures);
    if (allFuturesToCancel.isEmpty()) {
      return;
    }
    allFuturesToCancel.forEach(f -> {
      try {
        if (f != null) {
          f.cancel(mayInterruptIfRunning);
        }
      }
      catch (Throwable t) {
        log.warn(t, "Error while cancelling future.");
      }
    });
  }
}
