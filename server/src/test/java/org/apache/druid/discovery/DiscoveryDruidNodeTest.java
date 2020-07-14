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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.ServerType;
import org.junit.Assert;
import org.junit.Test;

public class DiscoveryDruidNodeTest
{
  @Test
  public void testIsDiscoverableDataServer()
  {
    final DiscoveryDruidNode nonDataServer = new DiscoveryDruidNode(
        new DruidNode(
            "coordinator",
            "localhost",
            false,
            8081,
            null,
            true,
            false
        ),
        NodeRole.COORDINATOR,
        ImmutableMap.of()
    );
    Assert.assertFalse(nonDataServer.isDiscoverableDataServer());

    final DiscoveryDruidNode nonDiscoverableDataServer = new DiscoveryDruidNode(
        new DruidNode(
            "broker",
            "localhost",
            false,
            8082,
            null,
            true,
            false
        ),
        NodeRole.BROKER,
        ImmutableMap.of(
            DataNodeService.DISCOVERY_SERVICE_KEY,
            new DataNodeService("tier", 0L, ServerType.BROKER, 0, false)
        )
    );
    Assert.assertFalse(nonDiscoverableDataServer.isDiscoverableDataServer());

    final DiscoveryDruidNode discoveryDataServer = new DiscoveryDruidNode(
        new DruidNode(
            "broker",
            "localhost",
            false,
            8082,
            null,
            true,
            false
        ),
        NodeRole.BROKER,
        ImmutableMap.of(
            DataNodeService.DISCOVERY_SERVICE_KEY,
            new DataNodeService("tier", 0L, ServerType.BROKER, 0, true)
        )
    );
    Assert.assertTrue(discoveryDataServer.isDiscoverableDataServer());
  }
}
