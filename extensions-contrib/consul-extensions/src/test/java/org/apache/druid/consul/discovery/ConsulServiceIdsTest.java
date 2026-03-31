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

package org.apache.druid.consul.discovery;

import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

public class ConsulServiceIdsTest
{
  @Test
  public void testServiceNameAndIdAndKvKey()
  {
    ConsulDiscoveryConfig.ServiceConfig serviceConfig = new ConsulDiscoveryConfig.ServiceConfig(
        "druid",
        "dc1",
        null,
        Duration.standardSeconds(10),
        Duration.standardSeconds(90)
    );

    ConsulDiscoveryConfig config = ConsulDiscoveryConfig.create(
        new ConsulDiscoveryConfig.ConnectionConfig(null, null, null, null, null, null, null),
        null,
        serviceConfig,
        null,
        null
    );

    NodeRole role = NodeRole.PEON;
    String serviceName = ConsulServiceIds.serviceName(config, role);
    Assert.assertEquals("druid-peon", serviceName);

    DruidNode druidNode = new DruidNode("service", "host", false, 8080, null, true, false);
    DiscoveryDruidNode discoveryNode = new DiscoveryDruidNode(druidNode, role, null);

    String serviceId = ConsulServiceIds.serviceId(config, discoveryNode);
    Assert.assertEquals("druid-peon-host-8080", serviceId);

    String kvKey = ConsulServiceIds.nodeKvKey(config, serviceId);
    Assert.assertEquals("druid/nodes/druid-peon-host-8080", kvKey);
  }
}
