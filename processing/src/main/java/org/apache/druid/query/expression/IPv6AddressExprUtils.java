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
import inet.ipaddr.ipv6.IPv6Address;
 
import javax.annotation.Nullable;
 
public class IPv6AddressExprUtils
{

  private static final IPAddressStringParameters IPV6_ADDRESS_PARAMS = new IPAddressStringParameters.Builder().allowSingleSegment(false).allow_inet_aton(false).allowIPv4(false).allowPrefix(false).allowEmpty(false).toParams();
  private static final IPAddressStringParameters IPV6_SUBNET_PARAMS = new IPAddressStringParameters.Builder().allowSingleSegment(false).allow_inet_aton(false).allowEmpty(false).allowIPv4(false).toParams();

  /**
  * @return True if argument is a valid IPv6 address semicolon separated string. Single segments, Inet addresses and subnets are not allowed.
  */
  static boolean isValidIPv6Address(@Nullable String addressString)
  {
    return addressString != null && new IPAddressString(addressString, IPV6_ADDRESS_PARAMS).isIPv6();
  }

  /**
  * @return True if argument is a valid IPv6 subnet address.
  */
  static boolean isValidIPv6Subnet(@Nullable String subnetString)
  {
    return subnetString != null && new IPAddressString(subnetString, IPV6_SUBNET_PARAMS).isPrefixed();
  }

  /**
  * @return IPv6 address if the supplied string is a valid semicolon separated IPv6 Address string.
  */
  @Nullable
  public static IPv6Address parse(@Nullable String string)
  {
    IPAddressString ipAddressString = new IPAddressString(string, IPV6_ADDRESS_PARAMS);
    if (ipAddressString.isIPv6()) {
      return ipAddressString.getAddress().toIPv6();
    } 
    return null;
  }

  @Nullable
  public static IPAddressString parseString(@Nullable String string)
  {
    IPAddressString ipAddressString = new IPAddressString(string, IPV6_ADDRESS_PARAMS);
    if (ipAddressString.isIPv6()) {
      return ipAddressString;
    }
    return null;
  }
 
  /**
  * @return IPv6 address from supplied array of bytes
  */
  @Nullable
  public static IPv6Address parse(@Nullable byte[] bytes)
  {
    return bytes == null ? null : new IPv6Address(bytes);
  }
}
