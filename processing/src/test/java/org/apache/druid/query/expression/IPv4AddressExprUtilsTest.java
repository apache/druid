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

import inet.ipaddr.IPAddressNetwork;
import inet.ipaddr.ipv4.IPv4Address;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class IPv4AddressExprUtilsTest
{
  private static final List<String> VALID_IPV4_ADDRESSES = Arrays.asList(
      "192.168.0.1",
      "0.0.0.0",
      "255.255.255.255",
      "255.0.0.0",
      "0.255.0.0",
      "0.0.255.0",
      "0.0.0.255"
  );
  private static final List<String> INVALID_IPV4_ADDRESSES = Arrays.asList(
      "druid.apache.org",  // no octets are numbers
      "a.b.c.d",  // no octets are numbers
      "abc.def.ghi.jkl",  // no octets are numbers
      "1..3.4",  // missing octet
      "1.2..4",  // missing octet
      "1.2.3..", // missing octet
      "1",  // missing octets
      "1.2",  // missing octets
      "1.2.3",  // missing octet
      "1.2.3.4.5",  // too many octets
      "256.0.0.0",  // first octet too large
      "0.265.0.0",  // second octet too large
      "0.0.266.0",  // third octet too large
      "0.0.0.355",  // fourth octet too large
      "a.2.3.4",  // first octet not number
      "1.a.3.4",  // second octet not number
      "1.2.c.4",  // third octet not number
      "1.2.3.d",  // fourth octet not number
      "1.2.3.0/24"  // prefixed cidr
  );
  private static final String IPV6_MAPPED = "::ffff:192.168.0.1";
  private static final String IPV6_COMPATIBLE = "::192.168.0.1";
  private static final List<String> VALID_IPV4_SUBNETS = Arrays.asList(
      "1.1.1.0/24",
      "255.255.255.0/18",
      "1.2.3.0/21",
      "1.0.0.0/8"
  );

  private static final List<String> INVALID_IPV4_SUBNETS = Arrays.asList(
      "1.2.3.0/45", // subnet mask too large
      "1.1.1/24", // missing octet
      "1/24", // missing octets
      "1", // missing octets
      "::/23", // IPv6 subnet
      "1.1.1.1" // no subnet mask
      );

  @Test
  public void testOverflowsUnsignedIntTooLow()
  {
    Assertions.assertTrue(IPv4AddressExprUtils.overflowsUnsignedInt(-1L));
  }

  @Test
  public void testOverflowsUnsignedIntLowest()
  {
    Assertions.assertFalse(IPv4AddressExprUtils.overflowsUnsignedInt(0L));
  }

  @Test
  public void testOverflowsUnsignedIntMiddle()
  {
    Assertions.assertFalse(IPv4AddressExprUtils.overflowsUnsignedInt(0xff_ffL));
  }

  @Test
  public void testOverflowsUnsignedIntHighest()
  {
    Assertions.assertFalse(IPv4AddressExprUtils.overflowsUnsignedInt(0xff_ff_ff_ffL));
  }

  @Test
  public void testOverflowsUnsignedIntTooHigh()
  {
    Assertions.assertTrue(IPv4AddressExprUtils.overflowsUnsignedInt(0x1_00_00_00_00L));
  }

  @Test
  public void testIsValidIPv4AddressNull()
  {
    Assertions.assertFalse(IPv4AddressExprUtils.isValidIPv4Address(null));
  }

  @Test
  public void testIsValidIPv4Address()
  {
    for (String address : VALID_IPV4_ADDRESSES) {
      Assertions.assertTrue(IPv4AddressExprUtils.isValidIPv4Address(address), getErrMsg(address));
    }
  }

  @Test
  public void testIsValidIPv4AddressIPv6Mapped()
  {
    Assertions.assertFalse(IPv4AddressExprUtils.isValidIPv4Address(IPV6_MAPPED));
  }

  @Test
  public void testIsValidIPv4AddressIPv6Compatible()
  {
    Assertions.assertFalse(IPv4AddressExprUtils.isValidIPv4Address(IPV6_COMPATIBLE));
  }

  @Test
  public void testIsValidIPv4AddressNotIpAddress()
  {
    for (String address : INVALID_IPV4_ADDRESSES) {
      Assertions.assertFalse(IPv4AddressExprUtils.isValidIPv4Address(address), getErrMsg(address));
    }
  }

  @Test
  public void testIsValidSubnetNull()
  {
    Assertions.assertFalse(IPv4AddressExprUtils.isValidIPv4Subnet(null));
  }

  @Test
  public void testIsValidIPv4SubnetValid()
  {
    for (String address : VALID_IPV4_SUBNETS) {
      Assertions.assertTrue(IPv4AddressExprUtils.isValidIPv4Subnet(address), getErrMsg(address));
    }
  }

  @Test
  public void testIsValidIPv4SubnetInvalid()
  {
    for (String address : INVALID_IPV4_SUBNETS) {
      Assertions.assertFalse(IPv4AddressExprUtils.isValidIPv4Subnet(address), getErrMsg(address));
    }
  }


  @Test
  public void testParseNull()
  {
    Assertions.assertNull(IPv4AddressExprUtils.parse(null));
  }

  @Test
  public void testParseIPv4()
  {
    for (String string : VALID_IPV4_ADDRESSES) {
      String errMsg = getErrMsg(string);
      IPv4Address address = IPv4AddressExprUtils.parse(string);
      Assertions.assertNotNull(address, errMsg);
      Assertions.assertEquals(string, address.toString(), errMsg);
    }
  }

  @Test
  public void testParseIPv6Mapped()
  {
    Assertions.assertNull(IPv4AddressExprUtils.parse(IPV6_MAPPED));
  }

  @Test
  public void testParseIPv6Compatible()
  {
    Assertions.assertNull(IPv4AddressExprUtils.parse(IPV6_COMPATIBLE));
  }

  @Test
  public void testParseNotIpAddress()
  {
    for (String address : INVALID_IPV4_ADDRESSES) {
      Assertions.assertNull(IPv4AddressExprUtils.parse(address), getErrMsg(address));
    }
  }

  @Test
  public void testParseLong()
  {
    IPv4Address address = IPv4AddressExprUtils.parse(0xC0A80001L);
    Assertions.assertNotNull(address);
    Assertions.assertArrayEquals(new byte[]{(byte) 0xC0, (byte) 0xA8, 0x00, 0x01}, address.getBytes());
  }

  @Test
  public void testToString()
  {
    byte[] bytes = new byte[]{(byte) 192, (byte) 168, 0, 1};

    IPAddressNetwork.IPAddressGenerator generator = new IPAddressNetwork.IPAddressGenerator();
    IPv4Address iPv4Address = generator.from(bytes).toIPv4();
    Assertions.assertEquals("192.168.0.1", IPv4AddressExprUtils.toString(iPv4Address));
  }

  @Test
  public void testToLong()
  {
    byte[] bytes = new byte[]{(byte) 0xC0, (byte) 0xA8, 0x00, 0x01};

    IPAddressNetwork.IPAddressGenerator generator = new IPAddressNetwork.IPAddressGenerator();
    IPv4Address iPv4Address = generator.from(bytes).toIPv4();
    Assertions.assertEquals(0xC0A80001L, IPv4AddressExprUtils.toLong(iPv4Address));
  }

  private String getErrMsg(String msg)
  {
    String prefix = "Failed: ";
    return prefix + msg;
  }
}
