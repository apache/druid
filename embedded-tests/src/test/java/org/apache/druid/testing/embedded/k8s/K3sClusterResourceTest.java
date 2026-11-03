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

package org.apache.druid.testing.embedded.k8s;

import org.apache.druid.testing.DruidCommand;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class K3sClusterResourceTest
{
  @Test
  public final void testDruidServiceManifestHasReadinessProbe()
  {
    final K3sDruidService coordinatorService = new K3sDruidService(DruidCommand.Server.COORDINATOR);
    final String yaml = coordinatorService.createManifestYaml("manifests/druid-service.yaml", "druid:test");

    Assertions.assertTrue(yaml.contains("readinessProbe:"));
    Assertions.assertTrue(yaml.contains("path: /status/health"));
    Assertions.assertTrue(yaml.contains("port: " + coordinatorService.getServicePort()));
    Assertions.assertTrue(yaml.contains("initialDelaySeconds: 5"));
    Assertions.assertTrue(yaml.contains("periodSeconds: 5"));
    Assertions.assertTrue(yaml.contains("value: \"256m\""));
  }

  @Test
  public final void testDumpPodLogsWithNullDiagnosticsDoesNotThrow()
  {
    final K3sClusterResource resource = new K3sClusterResource();
    Assertions.assertNull(resource.getPodDiagnostics());
    Assertions.assertNull(resource.getKubernetesClient());
    Assertions.assertDoesNotThrow(() -> resource.dumpPodLogs(K3sClusterResource.DRUID_NAMESPACE));
  }
}
