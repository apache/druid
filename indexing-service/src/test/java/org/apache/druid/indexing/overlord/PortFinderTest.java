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

package org.apache.druid.indexing.overlord;

import com.google.common.collect.ImmutableList;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class PortFinderTest
{
  private final List<PortFinder> finders = new ArrayList<>();

  @Before
  public void setUp()
  {
    // use startPort and endPort to generate usable ports.
    PortFinder finder1 = EasyMock.createMockBuilder(PortFinder.class)
                                 .withConstructor(1200, 1201, ImmutableList.of())
                                 .addMockedMethod("canBind")
                                 .createMock();
    // chose usable ports from candidates
    PortFinder finder2 = EasyMock.createMockBuilder(PortFinder.class)
                                 .withConstructor(1024, 1025, ImmutableList.of(1200, 1201))
                                 .addMockedMethod("canBind")
                                 .createMock();

    finders.add(finder1);
    finders.add(finder2);
  }

  @Test
  public void testUsedPort()
  {
    for (PortFinder finder : finders) {
      EasyMock.expect(finder.canBind(1200)).andReturn(true).andReturn(false);
      EasyMock.expect(finder.canBind(1201)).andReturn(true);
      EasyMock.replay(finder);

      final int port1 = finder.findUnusedPort();
      Assert.assertEquals(1200, port1);
      finder.markPortUnused(port1);

      final int port2 = finder.findUnusedPort();
      Assert.assertEquals(1201, port2);
      finder.markPortUnused(port2);

      EasyMock.verify(finder);
    }
  }
}
