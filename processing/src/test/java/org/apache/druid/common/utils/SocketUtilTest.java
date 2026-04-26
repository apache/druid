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

package org.apache.druid.common.utils;

import org.apache.druid.java.util.common.ISE;
import org.hamcrest.MatcherAssert;
import org.hamcrest.number.OrderingComparison;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SocketUtilTest
{
  private final int MAX_PORT = 0xffff;

  @Test
  public void testSocketUtil()
  {
    int port = SocketUtil.findOpenPort(0);
    MatcherAssert.assertThat("Port is greater than the maximum port 0xffff", port, OrderingComparison.lessThanOrEqualTo(MAX_PORT));
    MatcherAssert.assertThat("Port is less than minimum port 0", port, OrderingComparison.greaterThanOrEqualTo(0));
  }

  @Test
  public void testIllegalArgument()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () -> SocketUtil.findOpenPort(-1));
  }

  @Test
  public void testISEexception()
  {
    Assertions.assertThrows(ISE.class, () -> SocketUtil.findOpenPort(0xffff));
  }
}
