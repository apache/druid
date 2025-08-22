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

import java.util.Map;

/**
 * Docker test to verify running a Druid service with a custom node role provided
 * by an extension. This test uses Docker containers so that we can verify
 * starting the custom node from the command line.
 * <p>
 * See {@code HttpEmitterEventCollector} for running the custom node as an
 * embedded server.
 */
public class CustomNodeRoleDockerTest extends EmbeddedClusterTestBase implements LatestImageDockerTest
{
  // Server used only for its bindings
  private final EmbeddedRouter router = new EmbeddedRouter();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withZookeeper()
        .useContainerFriendlyHostname()
        .addCommonProperty("druid.extensions.loadList", "[\"druid-testing-tools\"]")
        .addResource(new DruidContainerResource(new EventCollectorCommand()).usingTestImage())
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

  private static class EventCollectorCommand implements DruidCommand
  {
    @Override
    public String getName()
    {
      return "eventCollector";
    }

    @Override
    public String getJavaOpts()
    {
      return "-Xms128m -Xmx128m";
    }

    @Override
    public Integer[] getExposedPorts()
    {
      return new Integer[]{9301};
    }

    @Override
    public Map<String, String> getDefaultProperties()
    {
      return Map.of();
    }

    @Override
    public Integer getExposedOperatorPort()
    {
      return 9302;
    }

    @Override
    public Map<String, String> getOperatorConfiguration()
    {
      return Map.of();
    }
  }
}
