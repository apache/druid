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

package org.apache.druid.common.guava;

import com.google.common.net.HostAndPort;
import com.google.common.primitives.Longs;
import org.junit.Assert;
import org.junit.Test;

public class GuavaUtilsTest
{
  @Test
  public void testParseLong()
  {
    Assert.assertNull(Longs.tryParse("+100"));
    Assert.assertNull(GuavaUtils.tryParseLong(""));
    Assert.assertNull(GuavaUtils.tryParseLong(null));
    Assert.assertNull(GuavaUtils.tryParseLong("+"));
    Assert.assertNull(GuavaUtils.tryParseLong("++100"));
    Assert.assertEquals((Object) Long.parseLong("+100"), GuavaUtils.tryParseLong("+100"));
    Assert.assertEquals((Object) Long.parseLong("-100"), GuavaUtils.tryParseLong("-100"));
    Assert.assertNotEquals(new Long(100), GuavaUtils.tryParseLong("+101"));
  }

  @Test
  public void testGetEnumIfPresent()
  {
    Assert.assertEquals(MyEnum.ONE, GuavaUtils.getEnumIfPresent(MyEnum.class, "ONE"));
    Assert.assertEquals(MyEnum.TWO, GuavaUtils.getEnumIfPresent(MyEnum.class, "TWO"));
    Assert.assertEquals(MyEnum.BUCKLE_MY_SHOE, GuavaUtils.getEnumIfPresent(MyEnum.class, "BUCKLE_MY_SHOE"));
    Assert.assertEquals(null, GuavaUtils.getEnumIfPresent(MyEnum.class, "buckle_my_shoe"));
  }

  @Test
  public void testHostAndPorthostText()
  {
    Assert.assertEquals("localhost", GuavaUtils.getHostText(HostAndPort.fromString("localhost")));
    Assert.assertEquals("127.0.0.1", GuavaUtils.getHostText(HostAndPort.fromString("127.0.0.1")));
    Assert.assertEquals("::1", GuavaUtils.getHostText(HostAndPort.fromString("::1")));
  }

  @Test
  public void testBreakingWhitespaceExists()
  {
    Assert.assertNotNull(GuavaUtils.breakingWhitespace());
  }

  enum MyEnum
  {
    ONE,
    TWO,
    BUCKLE_MY_SHOE
  }
}
