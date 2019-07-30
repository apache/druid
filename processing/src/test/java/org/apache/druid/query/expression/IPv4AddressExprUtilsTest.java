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

public class IPv4AddressExprUtilsTest
{
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
  public void testExtractIPv4AddressNull()
  {
    Assert.assertNull(IPv4AddressExprUtils.extractIPv4Address(null));
  }

  @Test
  public void testExtractIPv4AddressIPv4()
  {
    String ipv4 = "192.168.0.1";
    Assert.assertEquals(ipv4, IPv4AddressExprUtils.extractIPv4Address(ipv4));
  }

  @Test
  public void testExtractIPv4AddressIPv6Mapped()
  {
    String ipv4 = "192.168.0.1";
    String ipv6Mapped = "::ffff:" + ipv4;
    Assert.assertEquals(ipv4, IPv4AddressExprUtils.extractIPv4Address(ipv6Mapped));
  }

  @Test
  public void testExtractIPv4AddressIPv6Compatible()
  {
    String ipv6Compatible = "::192.168.0.1";
    Assert.assertNull(IPv4AddressExprUtils.extractIPv4Address(ipv6Compatible));
  }

  @Test
  public void testExtractIPv4AddressInvalid()
  {
    String notIpAddress = "druid.apache.org";
    Assert.assertNull(IPv4AddressExprUtils.extractIPv4Address(notIpAddress));
  }
}
