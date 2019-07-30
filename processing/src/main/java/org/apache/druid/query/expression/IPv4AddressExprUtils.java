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

class IPv4AddressExprUtils
{
  /**
   * @return True if argument cannot be represented by an unsigned integer (4 bytes)
   */
  static boolean overflowsUnsignedInt(long value)
  {
    return value < 0L || 0xff_ff_ff_ffL < value;
  }

  /**
   * @return IPv4 address dotted-decimal notated string or null if the argument is not a valid IPv4 address string or
   * IPv6 IPv4-mapped address string.
   */
  @Nullable
  static String extractIPv4Address(String string)
  {
    if (string != null) {
      try {
        InetAddress address = InetAddresses.forString(string);
        if (address instanceof Inet4Address) {
          return address.getHostAddress();
        }
      }
      catch (IllegalArgumentException ignored) {
        // fall through
      }
    }

    return null;
  }
}
