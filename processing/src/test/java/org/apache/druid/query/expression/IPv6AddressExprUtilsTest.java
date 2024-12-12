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

import inet.ipaddr.ipv6.IPv6Address;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IPv6AddressExprUtilsTest
{
  private static final List<String> VALID_IPV6_ADDRESSES = Arrays.asList(
        "2001:db8:85a3:8d3:1319:8a2e:370:7348",
        "::",
        "2001::8a2e:370:7348",
        "::8a2e:370:7348"
    );
   
  private static final List<String> INVALID_IPV6_ADDRESSES = Arrays.asList(
        "druid.apache.org",  // URL
        "190.0.0.0",  // IPv4 address
        "gff2::8a2e:370:7348",  // first octet exceeds max size
        "023a:8a2e:7348",  // invalid address length
        "2001:0db8:/32"  // CIDR
    );

  private static final List<String> VALID_IPV6_SUBNETS = Arrays.asList(
        "2001:db8:85a3:8d3::/64",
        "2001:db8::/8"
    );
 
  private static final List<String> INVALID_IPV6_SUBNETS = Arrays.asList(
        "2001:db8:85a3::/129", // subnet mask too large
        "f3ed::" // no subnet mask
    );

  @Test
  public void testIsValidIPv6AddressNull()
  {
    Assert.assertFalse(IPv6AddressExprUtils.isValidIPv6Address(null));
  }
 
  @Test
  public void testIsValidIPv6Address()
  {
    for (String address : VALID_IPV6_ADDRESSES) {
      Assert.assertTrue(getErrMsg(address), IPv6AddressExprUtils.isValidIPv6Address(address));
    }
  }
 
  @Test
  public void testIsValidIPv6AddressNotIpAddress()
  {
    for (String address : INVALID_IPV6_ADDRESSES) {
      Assert.assertFalse(getErrMsg(address), IPv6AddressExprUtils.isValidIPv6Address(address));
    }
  }
 
  @Test
  public void testIsValidSubnetNull()
  {
    Assert.assertFalse(IPv6AddressExprUtils.isValidIPv6Subnet(null));
  }
 
  @Test
  public void testIsValidIPv6SubnetValid()
  {
    for (String address : VALID_IPV6_SUBNETS) {
      Assert.assertTrue(getErrMsg(address), IPv6AddressExprUtils.isValidIPv6Subnet(address));
    }
  }
 
  @Test
  public void testIsValidIPv6SubnetInvalid()
  {
    for (String address : INVALID_IPV6_SUBNETS) {
      Assert.assertFalse(getErrMsg(address), IPv6AddressExprUtils.isValidIPv6Subnet(address));
    }
  }

  @Test
  public void testParseNullString()
  {
    Assert.assertNull(IPv6AddressExprUtils.parse((String) null));
  }
  
  @Test
  public void testParseNullBytes()
  {
    Assert.assertNull(IPv6AddressExprUtils.parse((byte[]) null));
  }
 
  @Test
  public void testParseIPv6()
  {
    for (String string : VALID_IPV6_ADDRESSES) {
      String errMsg = getErrMsg(string);
      IPv6Address address = IPv6AddressExprUtils.parse(string);
      Assert.assertNotNull(errMsg, address);
      Assert.assertEquals(errMsg, string, address.toString());
    }
  }
 
  @Test
  public void testParseNotIpV6Addresses()
  {
    for (String address : INVALID_IPV6_ADDRESSES) {
      Assert.assertNull(getErrMsg(address), IPv6AddressExprUtils.parse(address));
    }
  }
 
  private String getErrMsg(String msg)
  {
    String prefix = "Failed: ";
    return prefix + msg;
  }
}
