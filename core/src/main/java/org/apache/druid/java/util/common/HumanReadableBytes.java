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

@JsonSerialize(using = HumanReadableBytesSerializer.class)
public class HumanReadableBytes
{
  public static final HumanReadableBytes ZERO = new HumanReadableBytes(0L);

  private final long bytes;

  public HumanReadableBytes(String bytes)
  {
    this.bytes = HumanReadableBytes.parse(bytes);
  }

  public HumanReadableBytes(long bytes)
  {
    this.bytes = bytes;
  }

  public long getBytes()
  {
    return bytes;
  }

  public int getBytesInInt()
  {
    if (bytes > Integer.MAX_VALUE) {
      throw new ISE("Number [%d] exceeds range of Integer.MAX_VALUE", bytes);
    }

    return (int) bytes;
  }

  @Override
  public boolean equals(Object thatObj)
  {
    if (thatObj == null) {
      return false;
    }
    if (thatObj instanceof HumanReadableBytes) {
      return bytes == ((HumanReadableBytes) thatObj).bytes;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode()
  {
    return Long.hashCode(bytes);
  }

  @Override
  public String toString()
  {
    return String.valueOf(bytes);
  }

  public static HumanReadableBytes valueOf(int bytes)
  {
    return new HumanReadableBytes(bytes);
  }

  public static HumanReadableBytes valueOf(long bytes)
  {
    return new HumanReadableBytes(bytes);
  }

  public static long parse(String number)
  {
    if (number == null) {
      throw new IAE("Invalid format of number: number is null");
    }

    number = number.trim();
    if (number.length() == 0) {
      throw new IAE("Invalid format of number: number is blank");
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
   * Ki(B) - kilo binary byte = 1024
   * Mi(B) - mega binary byte = 1024*1204
   * Gi(B) - giga binary byte = 1024*1024*1024
   * Ti(B) - tera binary byte = 1024*1024*1024*1024
   * Pi(B) - peta binary byte = 1024*1024*1024*1024*1024
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
      throw new IAE("Invalid format of number: %s. Negative value is not allowed.", rawNumber);
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
    } else if (unit == 'i') {
      //unit ends with 'i' must be format of Ki/Mi/Gi/Ti/Pi, so at least 2 extra characters are required
      if (lastDigitIndex < 1) {
        throw new IAE("Invalid format of number [%s]. The unit should be one of Pi/Ti/Gi/Mi/Ki", rawNumber);
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
        base = isBinaryByte ? 1024L * 1024 * 1024 * 1024 : 1_000_000_000_000L;
        break;

      case 'p':
        base = isBinaryByte ? 1024L * 1024 * 1024 * 1024 * 1024 : 1_000_000_000_000_000L;
        break;

      default:
        if (!Character.isDigit(unit)) {
          throw new IAE("Invalid format of number: %s", rawNumber);
        }

        //lastDigitIndex here holds the index which is prior to current digit
        //move backward so that it's at the right place
        lastDigitIndex++;
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
      throw new IAE("Invalid format or out of range of long: %s", rawNumber);
    }
  }

  public enum UnitSystem
  {
    /**
     * also known as IEC format
     * eg: B, KiB, MiB, GiB ...
     */
    BINARY_BYTE,

    /**
     * also known as SI format
     * eg: B, KB, MB ...
     */
    DECIMAL_BYTE,

    /**
     * simplified SI format without 'B' indicator
     * eg: K, M, G ...
     */
    DECIMAL
  }

  /**
   * Returns a human-readable string version of input value
   *
   * @param bytes      input value. Negative value is also allowed
   * @param precision  [0,3]
   * @param unitSystem which unit system is adopted to format the input value, see {@link UnitSystem}
   */
  public static String format(long bytes, long precision, UnitSystem unitSystem)
  {
    if (precision < 0 || precision > 3) {
      throw new IAE("precision [%d] must be in the range of [0,3]", precision);
    }

    String pattern = "%." + precision + "f %s%s";
    switch (unitSystem) {
      case BINARY_BYTE:
        return BinaryFormatter.format(bytes, pattern, "B");
      case DECIMAL_BYTE:
        return DecimalFormatter.format(bytes, pattern, "B");
      case DECIMAL:
        return DecimalFormatter.format(bytes, pattern, "").trim();
      default:
        throw new IAE("Unkonwn unit system[%s]", unitSystem);
    }
  }

  static class BinaryFormatter
  {
    private static final String[] UNITS = {"", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei"};

    static String format(long bytes, String pattern, String suffix)
    {
      if (bytes > -1024 && bytes < 1024) {
        return bytes + " " + suffix;
      }

      if (bytes == Long.MIN_VALUE) {
        /**
         * special path for Long.MIN_VALUE
         *
         * Long.MIN_VALUE = 2^63 = (2^60=1EiB) * 2^3
         */
        return StringUtils.format(pattern, -8.0, UNITS[UNITS.length - 1], suffix);
      }

      /**
       * A number and its binary bits are listed as fellows
       * [0,    1KiB) = [0,    2^10)
       * [1KiB, 1MiB) = [2^10, 2^20),
       * [1MiB, 1GiB) = [2^20, 2^30),
       * [1GiB, 1PiB) = [2^30, 2^40),
       * ...
       *
       * So, expression (63 - Long.numberOfLeadingZeros(absValue))) helps us to get the right number of bits of the given input
       *
       * Internal implementaion of Long.numberOfLeadingZeros uses bit operations to do calculation so the cost is very cheap
       */
      int unitIndex = (63 - Long.numberOfLeadingZeros(Math.abs(bytes))) / 10;
      return StringUtils.format(pattern, (double) bytes / (1L << (unitIndex * 10)), UNITS[unitIndex], suffix);
    }
  }

  static class DecimalFormatter
  {
    private static final String[] UNITS = {"K", "M", "G", "T", "P", "E"};

    static String format(long bytes, String pattern, String suffix)
    {
      /**
       * handle number between (-1000, 1000) first to simply further processing
       */
      if (bytes > -1000 && bytes < 1000) {
        return bytes + " " + suffix;
      }

      /**
       * because max precision is 3, extra fraction can be ignored by use of integer division which might be a little more efficient
       */
      int unitIndex = 0;
      while (bytes <= -1000_000 || bytes >= 1000_000) {
        bytes /= 1000;
        unitIndex++;
      }
      return StringUtils.format(pattern, bytes / 1000.0, UNITS[unitIndex], suffix);
    }
  }
}
