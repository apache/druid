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

public class IPv4AddressExprUtils
{

  private static final IPAddressStringParameters IPV4_ADDRESS_PARAMS = new IPAddressStringParameters.Builder().allowSingleSegment(false).allow_inet_aton(false).allowIPv6(false).allowPrefix(false).allowEmpty(false).toParams();
  private static final IPAddressStringParameters IPV4_SUBNET_PARAMS = new IPAddressStringParameters.Builder().allowSingleSegment(false).allow_inet_aton(false).allowEmpty(false).allowIPv6(false).toParams();

  /**
   * @return True if argument cannot be represented by an unsigned integer (4 bytes), else false
   */
  public static boolean overflowsUnsignedInt(long value)
  {
    return value < 0L || 0xff_ff_ff_ffL < value;
  }

  /**
   * @return True if argument is a valid IPv4 address dotted-decimal string. Single segments, Inet addresses and subnets are not allowed.
   */
  static boolean isValidIPv4Address(@Nullable String addressString)
  {
    return addressString != null && new IPAddressString(addressString, IPV4_ADDRESS_PARAMS).isIPv4();
  }

  /**
   * @return True if argument is a valid IPv4 subnet address.
   */
  static boolean isValidIPv4Subnet(@Nullable String subnetString)
  {
    return subnetString != null && new IPAddressString(subnetString, IPV4_SUBNET_PARAMS).isPrefixed();
  }

  /**
   * @return IPv4 address if the supplied string is a valid dotted-decimal IPv4 Address string.
   */
  @Nullable
  public static IPv4Address parse(@Nullable String string)
  {
    IPAddressString ipAddressString = new IPAddressString(string, IPV4_ADDRESS_PARAMS);
    if (ipAddressString.isIPv4()) {
      return ipAddressString.getAddress().toIPv4();
    }
    return null;
  }

  @Nullable
  public static IPAddressString parseString(@Nullable String string)
  {
    IPAddressString ipAddressString = new IPAddressString(string, IPV4_ADDRESS_PARAMS);
    if (ipAddressString.isIPv4()) {
      return ipAddressString;
    }
    return null;
  }

  /**
   * @return IPv4 address if the supplied integer is a valid IPv4 integer number.
   */
  @Nullable
  public static IPv4Address parse(long value)
  {
    if (!overflowsUnsignedInt(value)) {
      return new IPv4Address((int) value);
    }
    return null;
  }

  /**
   * @return IPv4 address dotted-decimal canonical string.
   */
  public static String toString(IPv4Address address)
  {
    return address.toString();
  }

  /**
   *
   * @return IPv4 address as an integer.
   */
  public static long toLong(IPv4Address address)
  {
    return address.longValue();
  }
}
