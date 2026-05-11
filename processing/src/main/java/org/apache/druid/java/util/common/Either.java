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

import com.google.common.base.Preconditions;
import org.apache.druid.error.DruidException;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Function;

/**
 * Encapsulates either an "error" or a "value".
 *
 * Similar to the Either class in Scala or Haskell, except the possibilities are named "error" and "value" instead of
 * "left" and "right".
 */
public class Either<L, R>
{
  private final L error;
  private final R value;

  private Either(L error, R value)
  {
    this.error = error;
    this.value = value;
  }

  public static <L, R> Either<L, R> error(final L error)
  {
    return new Either<>(Preconditions.checkNotNull(error, "error"), null);
  }

  public static <L, R> Either<L, R> value(@Nullable final R value)
  {
    return new Either<>(null, value);
  }

  public boolean isValue()
  {
    return error == null;
  }

  public boolean isError()
  {
    return error != null;
  }

  /**
   * Returns the error object.
   *
   * @throws IllegalStateException if this instance is not an error
   */
  public L error()
  {
    if (isError()) {
      return error;
    } else {
      throw new IllegalStateException("Not an error; check isError first");
    }
  }

  /**
   * If this Either represents a value, returns it. If this Either represents an error, throw an error.
   *
   * If the error is a {@link DruidException}, it is thrown. If it is some other {@link Throwable}, it is
   * wrapped in a {@link DruidException} and thrown. If it is not a throwable, a generic {@link DruidException}
   * is thrown containing the string representation of the error object.
   *
   * To retrieve the error as-is, use {@link #isError()} and {@link #error()} instead.
   */
  @Nullable
  public R valueOrThrow()
  {
    if (isValue()) {
      return value;
    } else if (error instanceof Throwable) {
      // The exception happened somewhere else, perhaps in another thread entirely. If it is a DruidException
      // targeting a non-DEVELOPER persona, re-throw it as-is so we keep the original intent of the error message.
      // Otherwise, wrap it in a DEVELOPER-oriented DruidException so the stack trace of the current thread is
      // added to the exception details.
      if (error instanceof DruidException
          && ((DruidException) error).getTargetPersona() != DruidException.Persona.DEVELOPER) {
        throw (DruidException) error;
      }

      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.UNCATEGORIZED)
                          .build((Throwable) error, ((Throwable) error).getMessage());
    } else {
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.UNCATEGORIZED)
                          .build("%s", error);
    }
  }

  /**
   * Applies a function to this value, if present.
   *
   * If the mapping function throws an exception, it is thrown by this method instead of being packed up into
   * the returned Either.
   *
   * If this Either represents an error, the mapping function is not applied.
   *
   * @throws NullPointerException if the mapping function returns null
   */
  public <T> Either<L, T> map(final Function<R, T> fn)
  {
    if (isValue()) {
      return Either.value(fn.apply(value));
    } else {
      // Safe because the value is never going to be returned.
      //noinspection unchecked
      return (Either<L, T>) this;
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Either<?, ?> either = (Either<?, ?>) o;
    return Objects.equals(error, either.error) && Objects.equals(value, either.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(error, value);
  }

  @Override
  public String toString()
  {
    if (isValue()) {
      return "Value[" + value + "]";
    } else {
      return "Error[" + error + "]";
    }
  }
}
