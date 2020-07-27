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
      throw new IAE("Invalid format of number: number is null");
    }

    number = number.trim();
    if (number.length() == 0) {
      throw new IAE("Invalid format of number: Number is empty");
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

    number = number.trim();
    if (number.length() == 0) {
      return nullValue;
    }
    return parseInner(number);
  }

  private static long parseInner(String rawNumber)
  {
    String number = StringUtils.toLowerCase(rawNumber);
    if (number.charAt(0) == '-') {
      throw new IAE("Invalid format of number: %s. Negative value is not allowed.");
    }

    int lastDigitIndex = number.length() - 1;
    boolean isBinaryByte = false;
    char unit = number.charAt(lastDigitIndex--);
    if (unit == 'b') {
      //unit ends with 'b' must be format of KiB/MiB/GiB/TiB/PiB, so at least 3 extra characters are required
      if (lastDigitIndex < 2) {
        throw new IAE("Invalid format of number: %s", rawNumber);
      }
      if (number.charAt(lastDigitIndex--) != 'i') {
        throw new IAE("Invalid format of number: %s", rawNumber);
      }

      unit = number.charAt(lastDigitIndex--);
      isBinaryByte = true;
    }

    long base = 1;
    switch (unit) {
      case 'k':
        base = isBinaryByte ? 1024 : 1_000;
        break;

      case 'm':
        base = isBinaryByte ? 1024 * 1024 : 1_000_000;
        break;

      case 'g':
        base = isBinaryByte ? 1024 * 1024 * 1024 : 1_000_000_000;
        break;

      case 't':
        base = isBinaryByte ? 1024 * 1024 * 1024 * 1024L : 1_000_000_000_000L;
        break;

      case 'p':
        base = isBinaryByte ? 1024L * 1024 * 1024 * 1024 * 1024 : 1_000_000_000_000_000L;
        break;

      default:
        lastDigitIndex++;
        if (!Character.isDigit(unit)) {
          throw new IAE("Invalid format of number: %s", rawNumber);
        }
        break;
    }

    try {
      long value = Long.parseLong(number.substring(0, lastDigitIndex + 1)) * base;
      if (base > 1 && value < base) {
        //for base == 1, overflow has been checked in parseLong
        throw new IAE("Number overflow: %s", rawNumber);
      }
      return value;
    }
    catch (NumberFormatException e) {
      throw new IAE("Invalid format of number: %s", rawNumber);
    }
  }
}
