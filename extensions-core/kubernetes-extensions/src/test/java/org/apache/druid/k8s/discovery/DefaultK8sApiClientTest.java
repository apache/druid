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

import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class DefaultK8sApiClientTest
{
  @Test
  public void testIsPodReady_nullStatus()
  {
    V1Pod pod = new V1Pod();
    Assertions.assertFalse(DefaultK8sApiClient.isPodReady(pod));
  }

  @Test
  public void testIsPodReady_nullContainerStatuses()
  {
    V1Pod pod = new V1Pod();
    pod.setStatus(new V1PodStatus());
    Assertions.assertFalse(DefaultK8sApiClient.isPodReady(pod));
  }

  @Test
  public void testIsPodReady_emptyContainerStatuses()
  {
    V1Pod pod = new V1Pod();
    V1PodStatus status = new V1PodStatus();
    status.setContainerStatuses(Collections.emptyList());
    pod.setStatus(status);
    Assertions.assertFalse(DefaultK8sApiClient.isPodReady(pod));
  }

  @Test
  public void testIsPodReady_allContainersReady()
  {
    V1Pod pod = new V1Pod();
    V1PodStatus status = new V1PodStatus();
    V1ContainerStatus cs = new V1ContainerStatus();
    cs.setReady(true);
    status.setContainerStatuses(Collections.singletonList(cs));
    pod.setStatus(status);
    Assertions.assertTrue(DefaultK8sApiClient.isPodReady(pod));
  }

  @Test
  public void testIsPodReady_containerNotReady()
  {
    V1Pod pod = new V1Pod();
    V1PodStatus status = new V1PodStatus();
    V1ContainerStatus cs = new V1ContainerStatus();
    cs.setReady(false);
    status.setContainerStatuses(Collections.singletonList(cs));
    pod.setStatus(status);
    Assertions.assertFalse(DefaultK8sApiClient.isPodReady(pod));
  }

  @Test
  public void testIsPodReady_mixedContainerReadiness()
  {
    V1Pod pod = new V1Pod();
    V1PodStatus status = new V1PodStatus();
    V1ContainerStatus cs1 = new V1ContainerStatus();
    cs1.setReady(true);
    V1ContainerStatus cs2 = new V1ContainerStatus();
    cs2.setReady(false);
    status.setContainerStatuses(Arrays.asList(cs1, cs2));
    pod.setStatus(status);
    Assertions.assertFalse(DefaultK8sApiClient.isPodReady(pod));
  }

  @Test
  public void testIsPodReady_containerReadyNull()
  {
    V1Pod pod = new V1Pod();
    V1PodStatus status = new V1PodStatus();
    V1ContainerStatus cs = new V1ContainerStatus();
    // ready is null (not set)
    status.setContainerStatuses(Collections.singletonList(cs));
    pod.setStatus(status);
    Assertions.assertFalse(DefaultK8sApiClient.isPodReady(pod));
  }
}
