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

package org.apache.druid.query.expression;

import com.google.common.net.InetAddresses;

import javax.annotation.Nullable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.regex.Pattern;

class IPv4AddressExprUtils
{
  private static final Pattern IPV4_PATTERN = Pattern.compile(
      "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
  );

  /**
   * @return True if argument cannot be represented by an unsigned integer (4 bytes), else false
   */
  static boolean overflowsUnsignedInt(long value)
  {
    return value < 0L || 0xff_ff_ff_ffL < value;
  }

  /**
   * @return True if argument is a valid IPv4 address dotted-decimal string
   */
  static boolean isValidAddress(@Nullable String string)
  {
    return string != null && IPV4_PATTERN.matcher(string).matches();
  }

  @Nullable
  static Inet4Address parse(@Nullable String string)
  {
    // Explicitly check for valid address to avoid overhead of InetAddresses#forString() potentially
    // throwing IllegalArgumentException
    if (isValidAddress(string)) {
      // Do not use java.lang.InetAddress#getByName() as it may do DNS lookups
      InetAddress address = InetAddresses.forString(string);
      if (address instanceof Inet4Address) {
        return (Inet4Address) address;
      }
    }
    return null;
  }

  static Inet4Address parse(int value)
  {
    return InetAddresses.fromInteger(value);
  }

  /**
   * @return IPv4 address dotted-decimal notated string
   */
  static String toString(Inet4Address address)
  {
    return address.getHostAddress();
  }

  static long toLong(Inet4Address address)
  {
    int value = InetAddresses.coerceToInteger(address);
    return Integer.toUnsignedLong(value);
  }
}
