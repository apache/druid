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

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import javax.el.MethodNotFoundException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 *
 */
public class GuavaUtils
{
  private static final CharMatcher BREAKING_WHITESPACE_INSTANCE;

  static {
    CharMatcher matcher;
    try {
      final Method m = CharMatcher.class.getDeclaredMethod("breakingWhitespace");
      matcher = (CharMatcher) m.invoke(null);
    }
    catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Failed to fetch breakingWhitespace", e);
    }
    catch (NoSuchMethodException e) {
      try {
        final Field f = CharMatcher.class.getDeclaredField("BREAKING_WHITESPACE");
        matcher = (CharMatcher) f.get(null);
      }
      catch (IllegalAccessException e1) {
        throw new IllegalStateException("Failed to access BREAKING_WHITESPACE", e1);
      }
      catch (NoSuchFieldException e1) {
        throw new IllegalStateException("Cannot find breaking white space in guava", e1);
      }
    }
    if (matcher == null) {
      throw new IllegalStateException("wtf!?");
    }
    BREAKING_WHITESPACE_INSTANCE = matcher;
  }

  /**
   * To fix semantic difference of Longs.tryParse() from Long.parseLong (Longs.tryParse() returns null for '+' started value)
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
   * Try the various methods (zero arguments) against the object. This is handy for maintaining guava compatability
   *
   * @param object  The object to call the methods on
   * @param methods The sequence of methods to call
   * @param <T>     The return type
   *
   * @return The result of invoking the method on the object
   */
  private static <T> T tryMethods(Object object, Class<T> assignableTo, String... methods)
  {
    for (String method : methods) {
      try {
        final Method m = object.getClass().getDeclaredMethod(method);
        if (!assignableTo.isAssignableFrom(m.getReturnType())) {
          throw new IAE(
              "Cannot assign [%s] to [%s] in [%s] from [%s]",
              m.getReturnType(),
              assignableTo,
              m,
              object.getClass()
          );
        }
        try {
          return (T) m.invoke(object);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
          throw new IAE("Failed to invoke [%s] on [%s]", m, object);
        }
      }
      catch (NoSuchMethodException e) {
        // Keep going
      }
    }
    throw new MethodNotFoundException(StringUtils.format(
        "Unable to find methods %s in [%s]",
        Arrays.toString(methods),
        object.getClass()
    ));
  }

  /**
   * Get the host portion of the {@link HostAndPort} that has the host's text. Changes in different guava versions
   *
   * @param hostAndPort The object to pull from
   *
   * @return The host portion of the host and port
   */
  public static String getHostText(HostAndPort hostAndPort)
  {
    return tryMethods(hostAndPort, String.class, "getHostText", "getHost");
  }

  /**
   * Returns the instance of the breaking whitespace char matcher in guava.
   *
   * This moved starting in guava 19
   *
   * @return `CharMatcher.BREAKING_WHITESPACE` or `CharMatcher.breakingWhitespace()` depending on whichever works
   */

  public static CharMatcher breakingWhitespace()
  {
    return BREAKING_WHITESPACE_INSTANCE;
  }
}
