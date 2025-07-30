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

package org.apache.druid.testing.embedded.docker;

import org.apache.druid.testing.DruidCommand;
import org.apache.druid.testing.cli.CliEventCollector;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.server.HighAvailabilityTest;
import org.junit.jupiter.api.Test;

public class CustomNodeRoleDockerTest extends EmbeddedClusterTestBase
{
  private final DruidContainerResource customNodeEventCollector =
      new DruidContainerResource(DruidCommand.TEST_EVENT_COLLECTOR).usingTestImage();

  private final EmbeddedRouter router = new EmbeddedRouter();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withZookeeper()
        .useDruidContainers()
        .addCommonProperty("druid.extensions.loadList", "[\"druid-testing-tools\"]")
        .addResource(customNodeEventCollector)
        .addServer(router);
  }

  @Test
  public void test_customNode_isDiscoveredByOtherServices()
  {
    HighAvailabilityTest.verifyNodeRoleHasServerCount(
        CliEventCollector.NODE_ROLE,
        1,
        router.bindings().nodeDiscovery(),
        router.bindings().escalatedHttpClient()
    );
  }
}
