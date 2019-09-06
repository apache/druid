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

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;

import javax.annotation.Nullable;

public final class Numbers
{
  /**
   * Parse the given object as a {@code long}. The input object can be a {@link String} or one of the implementations of
   * {@link Number}. You may want to use {@code GuavaUtils.tryParseLong()} instead if the input is a nullable string and
   * you want to avoid any exceptions.
   *
   * @throws NumberFormatException if the input is an unparseable string.
   * @throws NullPointerException if the input is null.
   * @throws ISE if the input is not a string or a number.
   */
  public static long parseLong(Object val)
  {
    if (val instanceof String) {
      return Long.parseLong((String) val);
    } else if (val instanceof Number) {
      return ((Number) val).longValue();
    } else {
      if (val == null) {
        throw new NullPointerException("Input is null");
      } else {
        throw new ISE("Unknown type [%s]", val.getClass());
      }
    }
  }

  /**
   * Parse the given object as a {@code int}. The input object can be a {@link String} or one of the implementations of
   * {@link Number}.
   *
   * @throws NumberFormatException if the input is an unparseable string.
   * @throws NullPointerException if the input is null.
   * @throws ISE if the input is not a string or a number.
   */
  public static int parseInt(Object val)
  {
    if (val instanceof String) {
      return Integer.parseInt((String) val);
    } else if (val instanceof Number) {
      return ((Number) val).intValue();
    } else {
      if (val == null) {
        throw new NullPointerException("Input is null");
      } else {
        throw new ISE("Unknown type [%s]", val.getClass());
      }
    }
  }

  /**
   * Parse the given object as a {@code boolean}. The input object can be a {@link String} or {@link Boolean}.
   *
   * @return {@code true} only if the input is a {@link Boolean} representing {@code true} or a {@link String} of
   * {@code "true"}.
   *
   * @throws NullPointerException if the input is null.
   * @throws ISE if the input is not a string or a number.
   */
  public static boolean parseBoolean(Object val)
  {
    if (val instanceof String) {
      return Boolean.parseBoolean((String) val);
    } else if (val instanceof Boolean) {
      return (boolean) val;
    } else {
      if (val == null) {
        throw new NullPointerException("Input is null");
      } else {
        throw new ISE("Unknown type [%s]", val.getClass());
      }
    }
  }

  /**
   * Try parsing the given Number or String object val as double.
   * @param val
   * @param nullValue value to return when input was string type but not parseable into double value
   * @return parsed double value
   */
  public static double tryParseDouble(@Nullable Object val, double nullValue)
  {
    if (val == null) {
      return nullValue;
    } else if (val instanceof Number) {
      return ((Number) val).doubleValue();
    } else if (val instanceof String) {
      Double d = Doubles.tryParse((String) val);
      return d == null ? nullValue : d.doubleValue();
    } else {
      throw new IAE("Unknown object type [%s]", val.getClass().getName());
    }
  }

  /**
   * Try parsing the given Number or String object val as long.
   * @param val
   * @param nullValue value to return when input was string type but not parseable into long value
   * @return parsed long value
   */
  public static long tryParseLong(@Nullable Object val, long nullValue)
  {
    if (val == null) {
      return nullValue;
    } else if (val instanceof Number) {
      return ((Number) val).longValue();
    } else if (val instanceof String) {
      long l = nullValue;
      Long lobj = Longs.tryParse((String) val);
      if (lobj == null) {  // for "ddd.dd" , Longs.tryParse(..) returns null
        Double dobj = Doubles.tryParse((String) val);
        if (dobj != null) {
          l = dobj.longValue();
        }
      } else {
        l = lobj.longValue();
      }
      return l;
    } else {
      throw new IAE("Unknown object type [%s]", val.getClass().getName());
    }
  }

  /**
   * Try parsing the given Number or String object val as float.
   * @param val
   * @param nullValue value to return when input was string type but not parseable into float value
   * @return parsed float value
   */
  public static float tryParseFloat(@Nullable Object val, float nullValue)
  {
    if (val == null) {
      return nullValue;
    } else if (val instanceof Number) {
      return ((Number) val).floatValue();
    } else if (val instanceof String) {
      Float f = Floats.tryParse((String) val);
      return f == null ? nullValue : f.floatValue();
    } else {
      throw new IAE("Unknown object type [%s]", val.getClass().getName());
    }
  }

  public static int toIntExact(long value, String error)
  {
    if ((int) value != value) {
      throw new ArithmeticException(error);
    }
    return (int) value;
  }

  private Numbers()
  {
  }
}
