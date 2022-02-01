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

package org.apache.druid.server;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class NodeTest
{
  @Test
  public void testNode()
  {
    Node node = new Node("host", "service", 80, 81);
    assertEquals("host", node.getHost());
    assertEquals("service", node.getService());
    assertEquals((Integer) 80, node.getPlaintextPort());
    assertEquals((Integer) 81, node.getTlsPort());
    assertEquals(node, node);

    Node node2 = new Node("host", "service", null, null);
    assertNull(node2.getPlaintextPort());
    assertNull(node2.getTlsPort());
    assertNotEquals(node, node2);
  }

  @Test
  public void testFromDruidNode()
  {
    DruidNode druidNode = new DruidNode("service", "host", true, 80, 81, true, true);
    Node node = Node.from(druidNode);
    assertEquals("host", node.getHost());
    assertEquals("service", node.getService());
    assertEquals((Integer) 80, node.getPlaintextPort());
    assertEquals((Integer) 81, node.getTlsPort());

    druidNode = new DruidNode("service", "host", true, 80, 81, false, true);
    node = Node.from(druidNode);
    assertEquals("host", node.getHost());
    assertEquals("service", node.getService());
    assertNull(node.getPlaintextPort());
    assertEquals((Integer) 81, node.getTlsPort());

    druidNode = new DruidNode("service", "host", true, 80, 81, true, false);
    node = Node.from(druidNode);
    assertEquals("host", node.getHost());
    assertEquals("service", node.getService());
    assertEquals((Integer) 80, node.getPlaintextPort());
    assertNull(node.getTlsPort());
  }

}
