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

package org.apache.druid.discovery;

import org.apache.druid.server.DruidNode;
import org.junit.Test;

import java.util.Collection;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalDruidNodeDiscoveryTest
{
  public static class TestListener implements DruidNodeDiscovery.Listener
  {
    int netCount;

    @Override
    public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
    {
      netCount++;
    }

    @Override
    public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
    {
      netCount--;
    }
  }

  @Test
  public void testBasics()
  {
    LocalDruidNodeDiscoveryProvider p = new LocalDruidNodeDiscoveryProvider();
    TestListener tl = new TestListener();
    p.getForNodeRole(NodeRole.BROKER).registerListener(tl);

    DruidNode node = new DruidNode("broker", "localhost", false, 1000, -1, true, false);
    BooleanSupplier isPresent = p.getForNode(node, NodeRole.BROKER);
    assertFalse(isPresent.getAsBoolean());

    DiscoveryDruidNode dn = new DiscoveryDruidNode(node, NodeRole.BROKER, null);
    p.announce(dn);
    assertTrue(isPresent.getAsBoolean());
    assertEquals(1, tl.netCount);
    p.unannounce(dn);
    assertEquals(0, tl.netCount);
    assertFalse(isPresent.getAsBoolean());
  }
}
