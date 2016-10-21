/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.common.utils;

import org.hamcrest.number.OrderingComparison;
import org.junit.Assert;
import org.junit.Test;

import io.druid.java.util.common.ISE;

public class SocketUtilTest
{
  private final int MAX_PORT = 0xffff;
  @Test
  public void testSocketUtil()
  {
    int port = SocketUtil.findOpenPort(0);
    Assert.assertThat("Port is greater than the maximum port 0xffff",port, OrderingComparison.lessThanOrEqualTo(MAX_PORT));
    Assert.assertThat("Port is less than minimum port 0",port, OrderingComparison.greaterThanOrEqualTo(0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArgument()
  {
    SocketUtil.findOpenPort(-1);
  }

  @Test(expected = ISE.class)
  public void testISEexception()
  {
    SocketUtil.findOpenPort(0xffff);
  }
}
