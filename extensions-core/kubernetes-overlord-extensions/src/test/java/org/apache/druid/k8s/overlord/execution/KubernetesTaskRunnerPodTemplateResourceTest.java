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

package org.apache.druid.k8s.overlord.execution;

import com.google.common.collect.ImmutableList;
import io.fabric8.kubernetes.api.model.PodTemplate;
import org.apache.druid.k8s.overlord.common.K8sTestUtils;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateSelector;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateWithName;
import org.apache.druid.k8s.overlord.taskadapter.SingleContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.util.List;

public class KubernetesTaskRunnerPodTemplateResourceTest
{
  @Test
  public void test_getPodTemplates_returnsConfiguredTemplates()
  {
    PodTemplate template = K8sTestUtils.fileToResource("basePodTemplate.yaml", PodTemplate.class);
    List<PodTemplateWithName> templates = ImmutableList.of(new PodTemplateWithName("base", template));

    PodTemplateSelector selector = EasyMock.createMock(PodTemplateSelector.class);
    EasyMock.expect(selector.getPodTemplates()).andReturn(templates);
    PodTemplateTaskAdapter adapter = EasyMock.createMock(PodTemplateTaskAdapter.class);
    EasyMock.expect(adapter.getPodTemplateSelector()).andReturn(selector);
    EasyMock.replay(selector, adapter);

    Response result = new KubernetesTaskRunnerPodTemplateResource(adapter).getPodTemplates();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assertions.assertEquals(templates, result.getEntity());
  }

  @Test
  public void test_getPodTemplates_whenAdapterIsNotPodTemplateAdapter_returnsNotFound()
  {
    TaskAdapter adapter = EasyMock.createMock(SingleContainerTaskAdapter.class);
    EasyMock.replay(adapter);

    Response result = new KubernetesTaskRunnerPodTemplateResource(adapter).getPodTemplates();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), result.getStatus());
  }
}
