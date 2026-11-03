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

import io.fabric8.kubernetes.api.model.ContainerStateBuilder;
import io.fabric8.kubernetes.api.model.ContainerStateTerminatedBuilder;
import io.fabric8.kubernetes.api.model.ContainerStateWaitingBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.PodConditionBuilder;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.PodResource;
import org.apache.druid.java.util.common.ISE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class K3sPodDiagnosticsTest
{
  @Test
  public final void testFormatPodDiagnosticsWithNullPod()
  {
    final String diagnostics = K3sPodDiagnostics.formatPodDiagnostics(null, null, null);
    Assertions.assertNotNull(diagnostics);
    Assertions.assertTrue(diagnostics.contains("Pod metadata is unavailable"));
  }

  @Test
  public final void testFormatPodDiagnosticsWithRunningAndReadyPod()
  {
    final PodCondition readyCondition = new PodConditionBuilder()
        .withType("Ready")
        .withStatus("True")
        .withReason("PodReady")
        .withMessage("All containers are ready")
        .build();

    final ContainerStatus containerStatus = new ContainerStatusBuilder()
        .withName("druid-coordinator")
        .withReady(true)
        .withRestartCount(0)
        .withState(new ContainerStateBuilder().withNewRunning().withStartedAt("2026-11-01T12:00:00Z").endRunning().build())
        .build();

    final PodStatus status = new PodStatusBuilder()
        .withPhase("Running")
        .withConditions(Collections.singletonList(readyCondition))
        .withContainerStatuses(Collections.singletonList(containerStatus))
        .build();

    final Pod pod = new PodBuilder()
        .withMetadata(new ObjectMetaBuilder().withName("druid-coordinator-1").withNamespace("druid").build())
        .withStatus(status)
        .build();

    final String diagnostics = K3sPodDiagnostics.formatPodDiagnostics(pod, null, null);
    Assertions.assertTrue(diagnostics.contains("pod [druid-coordinator-1] in namespace [druid]"));
    Assertions.assertTrue(diagnostics.contains("Phase: [Running]"));
    Assertions.assertTrue(diagnostics.contains("Type: [Ready], Status: [True], Reason: [PodReady]"));
    Assertions.assertTrue(diagnostics.contains("Container: [druid-coordinator], Ready: [true], RestartCount: [0]"));
    Assertions.assertTrue(diagnostics.contains("State: Running(startedAt=[2026-11-01T12:00:00Z])"));
  }

  @Test
  public final void testFormatPodDiagnosticsWithCrashLoopBackOffPod()
  {
    final PodCondition condition = new PodConditionBuilder()
        .withType("Ready")
        .withStatus("False")
        .withReason("ContainersNotReady")
        .withMessage("containers with unready status: [druid-coordinator]")
        .build();

    final ContainerStatus containerStatus = new ContainerStatusBuilder()
        .withName("druid-coordinator")
        .withReady(false)
        .withRestartCount(3)
        .withState(
            new ContainerStateBuilder()
                .withWaiting(
                    new ContainerStateWaitingBuilder()
                        .withReason("CrashLoopBackOff")
                        .withMessage("back-off 40s restarting failed container")
                        .build()
                )
                .build()
        )
        .withLastState(
            new ContainerStateBuilder()
                .withTerminated(
                    new ContainerStateTerminatedBuilder()
                        .withExitCode(137)
                        .withReason("OOMKilled")
                        .withMessage("Command terminated")
                        .build()
                )
                .build()
        )
        .build();

    final PodStatus status = new PodStatusBuilder()
        .withPhase("Running")
        .withConditions(Collections.singletonList(condition))
        .withContainerStatuses(Collections.singletonList(containerStatus))
        .build();

    final Pod pod = new PodBuilder()
        .withMetadata(new ObjectMetaBuilder().withName("druid-coordinator-1").withNamespace("druid").build())
        .withStatus(status)
        .build();

    final Event event = new EventBuilder()
        .withType("Warning")
        .withReason("BackOff")
        .withMessage("Back-off restarting failed container")
        .withCount(3)
        .withLastTimestamp("2026-11-01T12:05:00Z")
        .build();

    final String logTail = "java.lang.OutOfMemoryError: Java heap space\n\tat org.apache.druid.server.Main.start(Main.java:42)";

    final String diagnostics = K3sPodDiagnostics.formatPodDiagnostics(pod, List.of(event), logTail);
    Assertions.assertTrue(diagnostics.contains("Timed out waiting for pod [druid-coordinator-1] in namespace [druid] to be ready."));
    Assertions.assertTrue(diagnostics.contains("Phase: [Running]"));
    Assertions.assertTrue(diagnostics.contains("Type: [Ready], Status: [False], Reason: [ContainersNotReady]"));
    Assertions.assertTrue(diagnostics.contains("Container: [druid-coordinator], Ready: [false], RestartCount: [3]"));
    Assertions.assertTrue(diagnostics.contains("State: Waiting(reason=[CrashLoopBackOff], message=[back-off 40s restarting failed container])"));
    Assertions.assertTrue(diagnostics.contains("LastState: Terminated(exitCode=[137], reason=[OOMKilled], message=[Command terminated])"));
    Assertions.assertTrue(diagnostics.contains("Events:"));
    Assertions.assertTrue(diagnostics.contains("[Warning] BackOff: Back-off restarting failed container (count: [3], lastObserved: [2026-11-01T12:05:00Z])"));
    Assertions.assertTrue(diagnostics.contains("Log Tail (last lines):"));
    Assertions.assertTrue(diagnostics.contains("OutOfMemoryError: Java heap space"));
  }

  @Test
  public final void testFormatPodDiagnosticsWithUnhealthyReadinessProbeEvent()
  {
    final Pod pod = new PodBuilder()
        .withMetadata(new ObjectMetaBuilder().withName("druid-router-1").withNamespace("druid").build())
        .withStatus(new PodStatusBuilder().withPhase("Running").build())
        .build();

    final Event event = new EventBuilder()
        .withType("Warning")
        .withReason("Unhealthy")
        .withMessage("Readiness probe failed: HTTP probe failed with statuscode: 503")
        .withCount(5)
        .withLastTimestamp("2026-11-01T12:03:00Z")
        .build();

    final String diagnostics = K3sPodDiagnostics.formatPodDiagnostics(pod, List.of(event), null);
    Assertions.assertTrue(diagnostics.contains("pod [druid-router-1] in namespace [druid]"));
    Assertions.assertTrue(diagnostics.contains("[Warning] Unhealthy: Readiness probe failed: HTTP probe failed with statuscode: 503 (count: [5]"));
  }

  @Test
  public final void testFormatPodDiagnosticsWithCustomNamespace()
  {
    final Pod pod = new PodBuilder()
        .withMetadata(new ObjectMetaBuilder().withName("druid-operator-1").withNamespace("druid-operator-system").build())
        .withStatus(new PodStatusBuilder().withPhase("Pending").build())
        .build();

    final String diagnostics = K3sPodDiagnostics.formatPodDiagnostics(pod, null, null);
    Assertions.assertTrue(diagnostics.contains("pod [druid-operator-1] in namespace [druid-operator-system]"));
    Assertions.assertTrue(diagnostics.contains("Phase: [Pending]"));
  }

  @Test
  public final void testResolveNamespaceFromPodMetadata()
  {
    final PodResource podResource = Mockito.mock(PodResource.class);
    final Pod pod = new PodBuilder()
        .withMetadata(new ObjectMetaBuilder().withName("operator").withNamespace("druid-operator-system").build())
        .build();
    Mockito.when(podResource.get()).thenReturn(pod);

    Assertions.assertEquals("druid-operator-system", K3sPodDiagnostics.resolveNamespace(podResource, "druid"));
  }

  @Test
  public final void testResolveNamespaceFallback()
  {
    final PodResource podResourceWithoutNamespace = Mockito.mock(PodResource.class);
    final Pod podWithoutNamespace = new PodBuilder()
        .withMetadata(new ObjectMetaBuilder().withName("operator").build())
        .build();
    Mockito.when(podResourceWithoutNamespace.get()).thenReturn(podWithoutNamespace);

    Assertions.assertEquals(
        "druid-operator-system",
        K3sPodDiagnostics.resolveNamespace(podResourceWithoutNamespace, "druid-operator-system")
    );
    Assertions.assertEquals(
        K3sClusterResource.DRUID_NAMESPACE,
        K3sPodDiagnostics.resolveNamespace(podResourceWithoutNamespace, null)
    );

    final PodResource brokenPodResource = Mockito.mock(PodResource.class);
    Mockito.when(brokenPodResource.get()).thenThrow(new RuntimeException("API error"));
    Assertions.assertEquals(
        "fallback-ns",
        K3sPodDiagnostics.resolveNamespace(brokenPodResource, "fallback-ns")
    );
  }

  @Test
  public final void testOnReadyTimeout()
  {
    final PodResource podResource = Mockito.mock(PodResource.class);
    final Pod pod = new PodBuilder()
        .withMetadata(new ObjectMetaBuilder().withName("druid-broker-1").withNamespace("druid").build())
        .withStatus(new PodStatusBuilder().withPhase("Pending").build())
        .build();
    Mockito.when(podResource.get()).thenReturn(pod);

    final KubernetesClient client = Mockito.mock(KubernetesClient.class, Mockito.RETURNS_DEEP_STUBS);
    final K3sPodDiagnostics diagnostics = new K3sPodDiagnostics(client, "testClass");

    final ISE ise = diagnostics.onReadyTimeout(podResource, "druid");
    Assertions.assertNotNull(ise);
    Assertions.assertTrue(ise.getMessage().contains("Timed out waiting for pod [druid-broker-1] in namespace [druid] to be ready."));
    Assertions.assertTrue(ise.getMessage().contains("Phase: [Pending]"));
  }

  @Test
  public final void testDumpPodLogsWithNullClientDoesNotThrow()
  {
    final K3sPodDiagnostics diagnostics = new K3sPodDiagnostics(null);
    Assertions.assertDoesNotThrow(() -> diagnostics.dumpPodLogs("druid"));
  }
}
