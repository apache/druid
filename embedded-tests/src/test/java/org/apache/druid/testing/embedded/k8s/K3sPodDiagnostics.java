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

import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.PodResource;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;

import javax.annotation.Nullable;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

/**
 * Diagnostic helper for K3s pods. Gathers pod status, container states,
 * Kubernetes events, and log tails on timeout, and dumps pod logs to disk.
 */
public class K3sPodDiagnostics
{
  private static final Logger log = new Logger(K3sPodDiagnostics.class);

  @Nullable
  private final KubernetesClient client;
  @Nullable
  private final String testClassName;

  public K3sPodDiagnostics(@Nullable final KubernetesClient client)
  {
    this(client, (String) null);
  }

  public K3sPodDiagnostics(@Nullable final KubernetesClient client, @Nullable final EmbeddedDruidCluster cluster)
  {
    this(client, cluster != null ? cluster.getTestClassName() : null);
  }

  public K3sPodDiagnostics(@Nullable final KubernetesClient client, @Nullable final String testClassName)
  {
    this.client = client;
    this.testClassName = testClassName;
  }

  /**
   * Handles pod readiness timeout in the default {@link K3sClusterResource#DRUID_NAMESPACE}:
   * dumps pod logs, formats detailed diagnostics, and returns an {@link ISE}.
   */
  public ISE onReadyTimeout(final PodResource pod)
  {
    return onReadyTimeout(pod, K3sClusterResource.DRUID_NAMESPACE);
  }

  /**
   * Handles pod readiness timeout in the given default namespace:
   * dumps pod logs, formats detailed diagnostics, and returns an {@link ISE}.
   */
  public ISE onReadyTimeout(final PodResource pod, @Nullable final String defaultNamespace)
  {
    final String resolvedNamespace = resolveNamespace(pod, defaultNamespace);
    dumpPodLogs(resolvedNamespace);
    final String diagnostics = buildPodDiagnostics(pod, resolvedNamespace);
    return new ISE("%s", diagnostics);
  }

  public static String resolveNamespace(final PodResource podResource, @Nullable final String fallbackNamespace)
  {
    try {
      final Pod pod = podResource.get();
      if (pod != null && pod.getMetadata() != null && pod.getMetadata().getNamespace() != null) {
        return pod.getMetadata().getNamespace();
      }
    }
    catch (Exception ignored) {
      // ignore
    }
    return fallbackNamespace != null ? fallbackNamespace : K3sClusterResource.DRUID_NAMESPACE;
  }

  public String buildPodDiagnostics(final PodResource podResource, @Nullable final String defaultNamespace)
  {
    Pod pod = null;
    try {
      pod = podResource.get();
    }
    catch (Exception e) {
      log.warn(e, "Could not get pod resource");
    }

    final String podName = pod != null && pod.getMetadata() != null
                           ? pod.getMetadata().getName()
                           : null;
    final String namespace = pod != null && pod.getMetadata() != null && pod.getMetadata().getNamespace() != null
                             ? pod.getMetadata().getNamespace()
                             : (defaultNamespace != null ? defaultNamespace : K3sClusterResource.DRUID_NAMESPACE);

    List<Event> events = null;
    if (podName != null && client != null) {
      try {
        final ObjectReference objectRef = new ObjectReferenceBuilder()
            .withApiVersion("v1")
            .withKind("Pod")
            .withName(podName)
            .withNamespace(namespace)
            .build();
        events = client.v1()
                       .events()
                       .inNamespace(namespace)
                       .withInvolvedObject(objectRef)
                       .list()
                       .getItems();
      }
      catch (Exception e) {
        log.warn(e, "Could not fetch events for pod [%s] in namespace [%s]", podName, namespace);
      }
    }

    String logTail = null;
    try {
      logTail = podResource.tailingLines(100).getLog();
    }
    catch (Exception e) {
      log.warn(e, "Could not fetch log tail for pod [%s]", podName);
    }

    return formatPodDiagnostics(pod, events, logTail);
  }

  public static String formatPodDiagnostics(
      @Nullable final Pod pod,
      @Nullable final List<Event> events,
      @Nullable final String logTail
  )
  {
    final StringBuilder diagnostics = new StringBuilder();
    if (pod == null || pod.getMetadata() == null) {
      diagnostics.append("Timed out waiting for pod to be ready: Pod metadata is unavailable (pod may have been deleted).");
      if (events != null && !events.isEmpty()) {
        appendEvents(diagnostics, events);
      }
      return diagnostics.toString();
    }

    final String podName = pod.getMetadata().getName();
    final String namespace = pod.getMetadata().getNamespace();

    diagnostics.append(StringUtils.format(
        "Timed out waiting for pod [%s] in namespace [%s] to be ready.",
        podName,
        namespace
    ));

    final PodStatus status = pod.getStatus();
    if (status != null) {
      diagnostics.append(StringUtils.format("\nPhase: [%s]", status.getPhase()));
      if (status.getReason() != null || status.getMessage() != null) {
        diagnostics.append(StringUtils.format(
            ", Reason: [%s], Message: [%s]",
            status.getReason(),
            status.getMessage()
        ));
      }

      final List<PodCondition> conditions = status.getConditions();
      if (conditions != null && !conditions.isEmpty()) {
        diagnostics.append("\nConditions:");
        for (final PodCondition condition : conditions) {
          diagnostics.append(StringUtils.format(
              "\n  - Type: [%s], Status: [%s], Reason: [%s], Message: [%s]",
              condition.getType(),
              condition.getStatus(),
              condition.getReason(),
              condition.getMessage()
          ));
        }
      }

      final List<ContainerStatus> containerStatuses = status.getContainerStatuses();
      if (containerStatuses != null && !containerStatuses.isEmpty()) {
        diagnostics.append("\nContainer Statuses:");
        for (final ContainerStatus containerStatus : containerStatuses) {
          diagnostics.append(StringUtils.format(
              "\n  - Container: [%s], Ready: [%s], RestartCount: [%d]",
              containerStatus.getName(),
              containerStatus.getReady(),
              containerStatus.getRestartCount()
          ));
          final ContainerState state = containerStatus.getState();
          if (state != null) {
            if (state.getWaiting() != null) {
              diagnostics.append(StringUtils.format(
                  ", State: Waiting(reason=[%s], message=[%s])",
                  state.getWaiting().getReason(),
                  state.getWaiting().getMessage()
              ));
            } else if (state.getTerminated() != null) {
              diagnostics.append(StringUtils.format(
                  ", State: Terminated(exitCode=[%d], reason=[%s], message=[%s])",
                  state.getTerminated().getExitCode(),
                  state.getTerminated().getReason(),
                  state.getTerminated().getMessage()
              ));
            } else if (state.getRunning() != null) {
              diagnostics.append(StringUtils.format(
                  ", State: Running(startedAt=[%s])",
                  state.getRunning().getStartedAt()
              ));
            }
          }
          final ContainerState lastState = containerStatus.getLastState();
          if (lastState != null && lastState.getTerminated() != null) {
            diagnostics.append(StringUtils.format(
                ", LastState: Terminated(exitCode=[%d], reason=[%s], message=[%s])",
                lastState.getTerminated().getExitCode(),
                lastState.getTerminated().getReason(),
                lastState.getTerminated().getMessage()
            ));
          }
        }
      }
    }

    if (events != null && !events.isEmpty()) {
      appendEvents(diagnostics, events);
    }

    if (logTail != null && !logTail.isBlank()) {
      diagnostics.append("\nLog Tail (last lines):\n").append(logTail);
    }

    return diagnostics.toString();
  }

  private static void appendEvents(final StringBuilder diagnostics, final List<Event> events)
  {
    diagnostics.append("\nEvents:");
    for (final Event event : events) {
      diagnostics.append(StringUtils.format(
          "\n  - [%s] %s: %s (count: [%d], lastObserved: [%s])",
          event.getType(),
          event.getReason(),
          event.getMessage(),
          event.getCount() != null ? event.getCount() : 1,
          event.getLastTimestamp()
      ));
    }
  }

  public void dumpPodLogs(final String namespace)
  {
    if (client == null) {
      return;
    }
    final String logCategory = testClassName != null ? testClassName : "k3s";
    final File logDir = new File(new File("druid-container-logs", logCategory), "k3s-pods");
    try {
      FileUtils.mkdirp(logDir);
    }
    catch (Exception e) {
      log.warn(e, "Failed to create pod log directory [%s]", logDir.getAbsolutePath());
    }

    try {
      for (final Pod pod : client.pods().inNamespace(namespace).list().getItems()) {
        if (pod.getMetadata() == null || pod.getMetadata().getName() == null) {
          continue;
        }
        final String podName = pod.getMetadata().getName();
        final File podLogFile = new File(logDir, podName + ".log");
        try {
          final String podLogs = client.pods().inNamespace(namespace).withName(podName).getLog();
          if (podLogs != null) {
            Files.writeString(podLogFile.toPath(), podLogs, StandardCharsets.UTF_8);
            log.info("Dumped pod [%s] logs to [%s]", podName, podLogFile.getAbsolutePath());
          }
        }
        catch (Exception e) {
          log.warn(e, "Could not dump logs for pod [%s]", podName);
        }
      }
    }
    catch (Exception e) {
      log.warn(e, "Could not list pods in namespace [%s] for dumping logs", namespace);
    }
  }
}
