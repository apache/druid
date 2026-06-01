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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.LocalPortForward;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.query.DruidMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Regression test for https://github.com/apache/druid/issues/18791.
 *
 * Verifies that {@code distribution/docker/peon.sh} sources options from
 * {@code jvm.config} and that those options reach the peon JVM as system
 * properties. Before the fix, {@code peon.sh} silently ignored
 * {@code jvm.config}, so any JVM flags set there — including memory limits
 * users had configured to prevent OOMs — never applied.
 *
 * <p>The test uses an operator manifest that injects
 * {@code -Ddruid.test.peon.jvmconfig.marker=true} into the cluster-level
 * {@code jvm.options}. The Druid operator writes these to {@code jvm.config}
 * on each node, including the overlord. When a peon is launched via the
 * {@code K8sTaskAdapter}, it inherits the overlord's pod spec (including the
 * mounted {@code jvm.config}); {@code peon.sh} then sources that file and
 * prepends its contents to {@code JAVA_OPTS}. The marker therefore appears in
 * the peon JVM's system properties, which this test asserts by querying
 * {@code /status/properties} on the peon pod.
 */
@Disabled("requires charts.datainfra.io chart, see https://github.com/apache/druid/pull/19047")
public class KubernetesPeonJvmConfigDockerTest extends BaseKubernetesTaskRunnerDockerTest
{
  private static final String MARKER_KEY = "druid.test.peon.jvmconfig.marker";
  private static final String MARKER_VALUE = "true";
  private static final String MARKER_MANIFEST =
      "manifests/druid-service-with-operator-peonjvmconfig.yaml";

  /**
   * Matches {@code DruidK8sConstants.PORT} but duplicated here to avoid
   * pulling the whole {@code druid-kubernetes-overlord-extensions} module in
   * as a test-scope dep just for one integer.
   */
  private static final int PEON_HTTP_PORT = 8100;

  private static final long PEON_POD_READY_TIMEOUT_MILLIS = 180_000L;
  private static final long PROPERTIES_POLL_TIMEOUT_MILLIS = 60_000L;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

  @Override
  protected boolean useSharedInformers()
  {
    return false;
  }

  @Override
  protected String getManifestTemplate()
  {
    return MARKER_MANIFEST;
  }

  @Test
  public void test_peonSourcesJvmConfigMarker() throws Exception
  {
    final String taskId = IdUtils.getRandomId();
    // Keep the peon alive long enough to discover its pod, port-forward, and hit the status endpoint.
    final long runDurationMillis = 240_000L;

    cluster.callApi().onLeaderOverlord(
        o -> o.runTask(
            taskId,
            new NoopTask(taskId, null, dataSource, runDurationMillis, 0L, null)
        )
    );

    try {
      eventCollector.latchableEmitter().waitForEvent(
          event -> event.hasMetricName(NoopTask.EVENT_STARTED)
                        .hasDimension(DruidMetrics.TASK_ID, taskId)
      );

      final KubernetesClient client = k3sCluster.getKubernetesClient();
      final Pod peonPod = waitForReadyPeonPod(client);

      try (LocalPortForward portForward = client.pods()
          .inNamespace(K3sClusterResource.DRUID_NAMESPACE)
          .withName(peonPod.getMetadata().getName())
          .portForward(PEON_HTTP_PORT)) {
        final Map<String, String> peonProperties = pollForStatusProperties(portForward.getLocalPort());
        Assertions.assertEquals(
            MARKER_VALUE,
            peonProperties.get(MARKER_KEY),
            "Expected jvm.config marker to reach peon JVM as a system property. "
            + "This is a regression: peon.sh must source $SERVICE_CONF_DIR/jvm.config."
        );
      }
    }
    finally {
      try {
        cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId));
      }
      catch (Exception ignore) {
        // Best-effort cleanup.
      }
    }
  }

  private Pod waitForReadyPeonPod(KubernetesClient client) throws InterruptedException
  {
    final long deadline = System.currentTimeMillis() + PEON_POD_READY_TIMEOUT_MILLIS;
    while (System.currentTimeMillis() < deadline) {
      for (Pod pod : client.pods().inNamespace(K3sClusterResource.DRUID_NAMESPACE).list().getItems()) {
        if (!ownedByJob(pod)) {
          continue;
        }
        if (isReady(pod)) {
          return pod;
        }
      }
      Thread.sleep(2_000L);
    }
    throw new AssertionError(
        "No Job-owned pod became Ready within "
        + (PEON_POD_READY_TIMEOUT_MILLIS / 1000L) + "s — expected a peon pod to appear"
    );
  }

  private static boolean ownedByJob(Pod pod)
  {
    final List<OwnerReference> owners = pod.getMetadata().getOwnerReferences();
    return owners != null && owners.stream().anyMatch(o -> "Job".equals(o.getKind()));
  }

  private static boolean isReady(Pod pod)
  {
    return pod.getStatus() != null
           && pod.getStatus().getConditions() != null
           && pod.getStatus().getConditions().stream().anyMatch(
               c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus())
           );
  }

  private Map<String, String> pollForStatusProperties(int localPort) throws InterruptedException
  {
    final long deadline = System.currentTimeMillis() + PROPERTIES_POLL_TIMEOUT_MILLIS;
    Exception lastException = null;
    while (System.currentTimeMillis() < deadline) {
      try {
        final URL url = URI.create("http://localhost:" + localPort + "/status/properties").toURL();
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(2_000);
        conn.setReadTimeout(2_000);
        if (conn.getResponseCode() == 200) {
          try (InputStream is = conn.getInputStream()) {
            return MAPPER.readValue(is, MAP_TYPE);
          }
        }
      }
      catch (Exception e) {
        lastException = e;
      }
      Thread.sleep(1_000L);
    }
    final String suffix = lastException == null ? "" : " Last error: " + lastException;
    throw new AssertionError(
        "Peon /status/properties did not return 200 within "
        + (PROPERTIES_POLL_TIMEOUT_MILLIS / 1000L) + "s." + suffix
    );
  }
}
