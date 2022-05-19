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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.DruidNode;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class K8sDruidNodeAnnouncerTest
{
  private final DiscoveryDruidNode testNode = new DiscoveryDruidNode(
      new DruidNode("druid/router", "test-host", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private final PodInfo podInfo = new PodInfo("testpod", "testns");

  private final K8sDiscoveryConfig discoveryConfig = new K8sDiscoveryConfig("druid-cluster", null, null, null, null, null, null, null);

  @Test
  public void testAnnounce() throws Exception
  {
    K8sApiClient mockK8sApiClient = EasyMock.createMock(K8sApiClient.class);
    Capture<String> podNameArg = Capture.newInstance();
    Capture<String> namespaceArg = Capture.newInstance();
    Capture<String> patchArg = Capture.newInstance();
    mockK8sApiClient.patchPod(EasyMock.capture(podNameArg), EasyMock.capture(namespaceArg), EasyMock.capture(patchArg));
    EasyMock.replay(mockK8sApiClient);

    K8sDruidNodeAnnouncer announcer = new K8sDruidNodeAnnouncer(podInfo, discoveryConfig, mockK8sApiClient, jsonMapper);
    announcer.announce(testNode);

    Assert.assertEquals(podInfo.getPodName(), podNameArg.getValue());
    Assert.assertEquals(podInfo.getPodNamespace(), namespaceArg.getValue());

    List<Map<String, Object>> actualPatchList = jsonMapper.readValue(
        patchArg.getValue(),
        new TypeReference<List<Map<String, Object>>>()
        {
        }
    );

    List<Map<String, Object>> expectedPatchList = Lists.newArrayList(
        ImmutableMap.of(
            "op", "add",
            "path", "/metadata/labels/druidDiscoveryAnnouncement-router",
            "value", "true"
        ),
        ImmutableMap.of(
            "op", "add",
            "path", "/metadata/labels/druidDiscoveryAnnouncement-id-hash",
            "value", "1429561393"
        ),
        ImmutableMap.of(
            "op", "add",
            "path", "/metadata/labels/druidDiscoveryAnnouncement-cluster-identifier",
            "value", discoveryConfig.getClusterIdentifier()
        ),
        ImmutableMap.of(
            "op", "add",
            "path", "/metadata/annotations/druidNodeInfo-router",
            "value", jsonMapper.writeValueAsString(testNode)
        )
    );
    Assert.assertEquals(expectedPatchList, actualPatchList);
  }

  @Test
  public void testUnannounce() throws Exception
  {
    K8sApiClient mockK8sApiClient = EasyMock.createMock(K8sApiClient.class);
    Capture<String> podNameArg = Capture.newInstance();
    Capture<String> namespaceArg = Capture.newInstance();
    Capture<String> patchArg = Capture.newInstance();
    mockK8sApiClient.patchPod(EasyMock.capture(podNameArg), EasyMock.capture(namespaceArg), EasyMock.capture(patchArg));
    EasyMock.replay(mockK8sApiClient);

    K8sDruidNodeAnnouncer announcer = new K8sDruidNodeAnnouncer(podInfo, discoveryConfig, mockK8sApiClient, jsonMapper);
    announcer.unannounce(testNode);

    Assert.assertEquals(podInfo.getPodName(), podNameArg.getValue());
    Assert.assertEquals(podInfo.getPodNamespace(), namespaceArg.getValue());

    List<Map<String, String>> actualPatchList = jsonMapper.readValue(
        patchArg.getValue(),
        new TypeReference<List<Map<String, String>>>()
        {
        }
    );

    List<Map<String, String>> expectedPatchList = Lists.newArrayList(
        ImmutableMap.of(
            "op", "remove",
            "path", "/metadata/labels/druidDiscoveryAnnouncement-router"
        ),
        ImmutableMap.of(
            "op", "remove",
            "path", "/metadata/labels/druidDiscoveryAnnouncement-id-hash"
        ),
        ImmutableMap.of(
            "op", "remove",
            "path", "/metadata/labels/druidDiscoveryAnnouncement-cluster-identifier"
        ),
        ImmutableMap.of(
            "op", "remove",
            "path", "/metadata/annotations/druidNodeInfo-router"
        )
    );
    Assert.assertEquals(expectedPatchList, actualPatchList);
  }
}
