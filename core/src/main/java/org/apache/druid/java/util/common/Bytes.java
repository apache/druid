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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.Locale;

@JsonSerialize(using = BytesSerializer.class)
public class Bytes implements Serializable
{
  public static final Bytes ZERO = new Bytes(0L);

  private long value;

  public Bytes(String value)
  {
    this.value = Bytes.parse(value);
  }

  public Bytes(long value)
  {
    this.value = value;
  }

  public long getValue()
  {
    return value;
  }

  @Override
  public boolean equals(Object thatObj)
  {
    if (thatObj == null) {
      return false;
    }
    if (thatObj instanceof Bytes) {
      return value == ((Bytes) thatObj).value;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode()
  {
    return Long.hashCode(value);
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  public static Bytes valueOf(int bytes)
  {
    return new Bytes(bytes);
  }

  public static Bytes valueOf(long bytes)
  {
    return new Bytes(bytes);
  }

  public static long parse(String number)
  {
    if (number == null) {
      throw new IAE("number is null");
    }

    number = number.trim().toLowerCase(Locale.getDefault());
    int len = number.length();
    if (len == 0) {
      throw new IAE("number is empty");
    }

    return parseInner(number);
  }

  /**
   * parse the case-insensitive string number, which is either:
   * <p>
   * a number string
   * <p>
   * or
   * <p>
   * a number string with a suffix which indicates the unit the of number
   * the unit must be one of following
   * k - kilobyte = 1000
   * m - megabyte = 1,000,000
   * g - gigabyte = 1,000,000,000
   * t - terabyte = 1,000,000,000,000
   * p - petabyte = 1,000,000,000,000,000
   * KiB - kilo binary byte = 1024
   * MiB - mega binary byte = 1024*1204
   * GiB - giga binary byte = 1024*1024*1024
   * TiB - tera binary byte = 1024*1024*1024*1024
   * PiB - peta binary byte = 1024*1024*1024*1024*1024
   * <p>
   *
   * @param nullValue to be returned when given number is null or empty
   * @return nullValue if input is null or empty
   * value of number
   * @throws IAE if the input is invalid
   */
  public static long parse(String number, long nullValue)
  {
    if (number == null) {
      return nullValue;
    }

    number = number.trim().toLowerCase(Locale.getDefault());
    if (number.length() == 0) {
      return nullValue;
    }
    return parseInner(number);
  }

  private static long parseInner(String number)
  {
    int index = number.length() - 1;
    boolean isBinary = false;
    char unit = number.charAt(index--);
    if (unit == 'b') {
      if (index < 2) {
        throw new IAE("invalid format of number[%s]", number);
      }
      if (number.charAt(index--) != 'i') {
        throw new IAE("invalid format of number[%s]", number);
      }

      unit = number.charAt(index--);
      isBinary = true;
    }
    long base = 1;
    switch (unit) {
      case 'k':
        base = isBinary ? 1024 : 1_000;
        break;

      case 'm':
        base = isBinary ? 1024 * 1024 : 1_000_000;
        break;

      case 'g':
        base = isBinary ? 1024 * 1024 * 1024 : 1_000_000_000;
        break;

      case 't':
        base = isBinary ? 1024 * 1024 * 1024 * 1024L : 1_000_000_000_000L;
        break;

      case 'p':
        base = isBinary ? 1024L * 1024 * 1024 * 1024 * 1024 : 1_000_000_000_000_000L;
        break;

      default:
        if (!Character.isDigit(unit)) {
          throw new IAE("invalid character in number[%s]", number);
        }
        break;
    }

    try {
      long value = 0;
      if (base > 1 && index >= 0) {
        value = Long.parseLong(number.substring(0, index + 1)) * base;
        if (value < base) {
          throw new IAE("number [%s] overflow", number);
        }
      } else {
        value = Long.parseLong(number);
      }
      return value;
    }
    catch (NumberFormatException e) {
      throw new IAE("invalid format of number[%s]", number);
    }
  }
}
