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
    DruidNode druidNode = new DruidNode("service", "host", true, 80, 81, true, true);
    Node node = new Node(druidNode);
    assertEquals("host", node.getHost());
    assertEquals("service", node.getService());
    assertEquals((Integer) 80, node.getPlaintextPort());
    assertEquals((Integer) 81, node.getTlsPort());
    assertEquals(node, node);

    DruidNode druidNode2 = new DruidNode("service", "host", true, 80, 81, true, false);
    Node node2 = new Node(druidNode2);
    assertEquals((Integer) 80, node2.getPlaintextPort());
    assertNull(node2.getTlsPort());
    assertNotEquals(node, node2);

    DruidNode druidNode3 = new DruidNode("service", "host", true, 80, 81, false, true);
    Node node3 = new Node(druidNode3);
    assertNull(node3.getPlaintextPort());
    assertEquals((Integer) 81, node.getTlsPort());
    assertNotEquals(node, node2);
  }
}
