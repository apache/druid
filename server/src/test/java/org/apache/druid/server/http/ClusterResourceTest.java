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

package org.apache.druid.server.http;

import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;
import org.apache.druid.test.utils.ResponseTestUtils;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.function.BooleanSupplier;

public class ClusterResourceTest
{
  private final ClusterResource resource = new ClusterResource(new DruidNodeDiscoveryProvider()
  {
    @Override
    public BooleanSupplier getForNode(DruidNode node, NodeRole nodeRole)
    {
      return null;
    }

    @Override
    public DruidNodeDiscovery getForNodeRole(NodeRole nodeRole)
    {
      return null;
    }
  });

  @Test
  public void testGetClusterServersWithBadRequest()
  {
    ResponseTestUtils.assertErrorResponse(
        resource.getClusterServers(null, true),
        Response.Status.BAD_REQUEST,
        "Invalid nodeRole of null. Valid node roles are [COORDINATOR, HISTORICAL, BROKER, OVERLORD, PEON, ROUTER, MIDDLE_MANAGER, INDEXER]"
    );
  }
}
