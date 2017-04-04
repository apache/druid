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

package io.druid.server.coordination;

import org.junit.Assert;
import org.junit.Test;

public class ServerTypeTest
{
  @Test
  public void testAssignable()
  {
    Assert.assertTrue(ServerType.HISTORICAL.isAssignable());
    Assert.assertTrue(ServerType.BRIDGE.isAssignable());
    Assert.assertFalse(ServerType.REALTIME.isAssignable());
  }

  @Test
  public void testFromString()
  {
    Assert.assertEquals(ServerType.HISTORICAL, ServerType.fromString("historical"));
    Assert.assertEquals(ServerType.BRIDGE, ServerType.fromString("bridge"));
    Assert.assertEquals(ServerType.REALTIME, ServerType.fromString("realtime"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidName()
  {
    ServerType.fromString("invalid");
  }
}
