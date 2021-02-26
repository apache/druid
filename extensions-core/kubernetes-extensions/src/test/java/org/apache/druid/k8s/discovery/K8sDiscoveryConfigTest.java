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

package org.apache.druid.k8s.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

public class K8sDiscoveryConfigTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testDefaultValuesSerde() throws Exception
  {
    testSerde(
        "{\"clusterIdentifier\": \"test-cluster\"}\n",
        new K8sDiscoveryConfig("test-cluster", null, null, null, null, null, null, null)
    );
  }

  @Test
  public void testCustomizedValuesSerde() throws Exception
  {
    testSerde(
        "{\n"
        + "  \"clusterIdentifier\": \"test-cluster\",\n"
        + "  \"podNameEnvKey\": \"PODNAMETEST\",\n"
        + "  \"podNamespaceEnvKey\": \"PODNAMESPACETEST\",\n"
        + "  \"coordinatorLeaderElectionConfigMapNamespace\": \"coordinatorns\",\n"
        + "  \"overlordLeaderElectionConfigMapNamespace\": \"overlordns\",\n"
        + "  \"leaseDuration\": \"PT3S\",\n"
        + "  \"renewDeadline\": \"PT2S\",\n"
        + "  \"retryPeriod\": \"PT1S\"\n"
        + "}\n",
        new K8sDiscoveryConfig(
            "test-cluster",
            "PODNAMETEST",
            "PODNAMESPACETEST",
            "coordinatorns",
            "overlordns",
            Duration.millis(3000),
            Duration.millis(2000),
            Duration.millis(1000)
        )
    );
  }

  private void testSerde(String jsonStr, K8sDiscoveryConfig expected) throws Exception
  {
    K8sDiscoveryConfig actual = jsonMapper.readValue(
        jsonMapper.writeValueAsString(
            jsonMapper.readValue(jsonStr, K8sDiscoveryConfig.class)
        ),
        K8sDiscoveryConfig.class
    );

    Assert.assertEquals(expected, actual);
  }
}
