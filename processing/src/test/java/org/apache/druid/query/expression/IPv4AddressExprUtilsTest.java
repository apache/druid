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

import org.junit.Assert;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
      "1.2.3.d"  // fourth octet not number
  );
  private static final String IPV6_MAPPED = "::ffff:192.168.0.1";
  private static final String IPV6_COMPATIBLE = "::192.168.0.1";

  @Test
  public void testOverflowsUnsignedIntTooLow()
  {
    Assert.assertTrue(IPv4AddressExprUtils.overflowsUnsignedInt(-1L));
  }

  @Test
  public void testOverflowsUnsignedIntLowest()
  {
    Assert.assertFalse(IPv4AddressExprUtils.overflowsUnsignedInt(0L));
  }

  @Test
  public void testOverflowsUnsignedIntMiddle()
  {
    Assert.assertFalse(IPv4AddressExprUtils.overflowsUnsignedInt(0xff_ffL));
  }

  @Test
  public void testOverflowsUnsignedIntHighest()
  {
    Assert.assertFalse(IPv4AddressExprUtils.overflowsUnsignedInt(0xff_ff_ff_ffL));
  }

  @Test
  public void testOverflowsUnsignedIntTooHigh()
  {
    Assert.assertTrue(IPv4AddressExprUtils.overflowsUnsignedInt(0x1_00_00_00_00L));
  }

  @Test
  public void testIsValidAddressNull()
  {
    Assert.assertFalse(IPv4AddressExprUtils.isValidAddress(null));
  }

  @Test
  public void testIsValidAddressIPv4()
  {
    for (String address : VALID_IPV4_ADDRESSES) {
      Assert.assertTrue(getErrMsg(address), IPv4AddressExprUtils.isValidAddress(address));
    }
  }

  @Test
  public void testIsValidAddressIPv6Mapped()
  {
    Assert.assertFalse(IPv4AddressExprUtils.isValidAddress(IPV6_MAPPED));
  }

  @Test
  public void testIsValidAddressIPv6Compatible()
  {
    Assert.assertFalse(IPv4AddressExprUtils.isValidAddress(IPV6_COMPATIBLE));
  }

  @Test
  public void testIsValidAddressNotIpAddress()
  {
    for (String address : INVALID_IPV4_ADDRESSES) {
      Assert.assertFalse(getErrMsg(address), IPv4AddressExprUtils.isValidAddress(address));
    }
  }

  @Test
  public void testParseNull()
  {
    Assert.assertNull(IPv4AddressExprUtils.parse(null));
  }

  @Test
  public void testParseIPv4()
  {
    for (String string : VALID_IPV4_ADDRESSES) {
      String errMsg = getErrMsg(string);
      Inet4Address address = IPv4AddressExprUtils.parse(string);
      Assert.assertNotNull(errMsg, address);
      Assert.assertEquals(errMsg, string, address.getHostAddress());
    }
  }

  @Test
  public void testParseIPv6Mapped()
  {
    Assert.assertNull(IPv4AddressExprUtils.parse(IPV6_MAPPED));
  }

  @Test
  public void testParseIPv6Compatible()
  {
    Assert.assertNull(IPv4AddressExprUtils.parse(IPV6_COMPATIBLE));
  }

  @Test
  public void testParseNotIpAddress()
  {
    for (String address : INVALID_IPV4_ADDRESSES) {
      Assert.assertNull(getErrMsg(address), IPv4AddressExprUtils.parse(address));
    }
  }

  @Test
  public void testParseInt()
  {
    Inet4Address address = IPv4AddressExprUtils.parse((int) 0xC0A80001L);
    Assert.assertArrayEquals(new byte[]{(byte) 0xC0, (byte) 0xA8, 0x00, 0x01}, address.getAddress());
  }

  @Test
  public void testToString() throws UnknownHostException
  {
    byte[] bytes = new byte[]{(byte) 192, (byte) 168, 0, 1};
    InetAddress address = InetAddress.getByAddress(bytes);
    Assert.assertEquals("192.168.0.1", IPv4AddressExprUtils.toString((Inet4Address) address));
  }

  @Test
  public void testToLong() throws UnknownHostException
  {
    byte[] bytes = new byte[]{(byte) 0xC0, (byte) 0xA8, 0x00, 0x01};
    InetAddress address = InetAddress.getByAddress(bytes);
    Assert.assertEquals(0xC0A80001L, IPv4AddressExprUtils.toLong((Inet4Address) address));
  }

  private String getErrMsg(String msg)
  {
    String prefix = "Failed: ";
    return prefix + msg;
  }
}
