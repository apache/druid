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

import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import inet.ipaddr.ipv4.IPv4Address;

import javax.annotation.Nullable;

class IPv4AddressExprUtils
{
  /**
   * @return True if argument cannot be represented by an unsigned integer (4 bytes), else false
   */
  static boolean overflowsUnsignedInt(long value)
  {
    return value < 0L || 0xff_ff_ff_ffL < value;
  }

  /**
   * @return True if argument is a valid IPv4 address dotted-decimal string. Single segments, Inet addresses and subnets are not allowed.
   */
  static boolean isValidIPv4Address(@Nullable String string)
  {
    IPAddressStringParameters strParams = new IPAddressStringParameters.Builder().allowSingleSegment(false).allow_inet_aton(false).allowIPv6(false).allowPrefix(false).toParams();
    return string != null && new IPAddressString(string, strParams).isIPv4();
  }

  /**
   *
   * @return True if argument is a valid IPv4 subnet address.
   */
  static boolean isValidIPv4Subnet(@Nullable String subnetString)
  {
    IPAddressStringParameters strParams = new IPAddressStringParameters.Builder().allowSingleSegment(false).allow_inet_aton(false).allowIPv6(false).toParams();
    return subnetString != null && new IPAddressString(subnetString, strParams).isPrefixed();
  }

  @Nullable
  static IPv4Address parse(@Nullable String string)
  {
    if (isValidIPv4Address(string)) {
      return new IPAddressString(string).getAddress().toIPv4();
    }
    return null;
  }

  static IPv4Address parse(int value)
  {
    return new IPv4Address(value);
  }

  /**
   * @return IPv4 address dotted-decimal notated string
   */
  static String toString(IPv4Address address)
  {
    return address.toString();
  }

  static long toLong(IPv4Address address)
  {
    return address.longValue();
  }
}
