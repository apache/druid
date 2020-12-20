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

package org.apache.druid.testing.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Config;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;

import java.io.IOException;
import java.net.HttpURLConnection;

public class K8sDruidClusterAdminClient extends AbstractDruidClusterAdminClient
{
  private static final Logger LOG = new Logger(K8sDruidClusterAdminClient.class);

  private static final String NAMESPACE = "default";
  private static final String COORDINATOR_POD_NAME = "druid-tiny-cluster-coordinator1-0";
  private static final String COORDINATOR_TWO_POD_NAME = "druid-tiny-cluster-coordinator2-0";
  private static final String HISTORICAL_POD_NAME = "druid-tiny-cluster-historicals-0";
  private static final String OVERLORD_POD_NAME = "druid-tiny-cluster-overlord1-0";
  private static final String OVERLORD_TWO_POD_NAME = "druid-tiny-cluster-overlord2-0";
  private static final String BROKER_POD_NAME = "druid-tiny-cluster-brokers-0";
  private static final String ROUTER_POD_NAME = "druid-tiny-cluster-routers-0";
  private static final String MIDDLEMANAGER_POD_NAME = "druid-tiny-cluster-middlemanagers-0";

  private final CoreV1Api k8sClient;

  @Inject
  public K8sDruidClusterAdminClient(
      ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    super(jsonMapper, httpClient, config);

    try {
      this.k8sClient = new CoreV1Api(Config.defaultClient());
    }
    catch (IOException ex) {
      throw new RE(ex, "Failed to create K8s ApiClient instance");
    }
  }

  @Override
  public void restartCoordinatorContainer()
  {
    restartPod(COORDINATOR_POD_NAME);
  }

  @Override
  public void restartCoordinatorTwoContainer()
  {
    restartPod(COORDINATOR_TWO_POD_NAME);
  }

  @Override
  public void restartHistoricalContainer()
  {
    restartPod(HISTORICAL_POD_NAME);
  }

  @Override
  public void restartOverlordContainer()
  {
    restartPod(OVERLORD_POD_NAME);
  }

  @Override
  public void restartOverlordTwoContainer()
  {
    restartPod(OVERLORD_TWO_POD_NAME);
  }

  @Override
  public void restartBrokerContainer()
  {
    restartPod(BROKER_POD_NAME);
  }

  @Override
  public void restartRouterContainer()
  {
    restartPod(ROUTER_POD_NAME);
  }

  @Override
  public void restartMiddleManagerContainer()
  {
    restartPod(MIDDLEMANAGER_POD_NAME);
  }

  private void restartPod(String podName)
  {
    // We only need to delete the pod, k8s StatefulSet controller will automatically recreate it.
    try {
      V1Pod prevPod = k8sClient.deleteNamespacedPod(
          podName,
          NAMESPACE,
          null,
          null,
          null,
          null,
          null,
          null
      );

      // Wait for previous pod to terminate and new pod to come up
      RetryUtils.retry(
          () -> {
            V1Pod newPod = getPod(podName);
            return newPod != null &&
                   !newPod.getMetadata().getResourceVersion().equals(prevPod.getMetadata().getResourceVersion());
          },
          (Throwable th) -> true,
          10
      );

      LOG.info("Restarted Pod [%s].", podName);
    }
    catch (Exception ex) {
      throw new RE(ex, "Failed to delete pod [%s]", podName);
    }
  }

  private V1Pod getPod(String podName)
  {
    try {
      return k8sClient.readNamespacedPod(
          podName,
          NAMESPACE,
          null,
          null,
          null
      );
    }
    catch (ApiException ex) {
      if (ex.getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
        throw new RE(ex, "Failed to get pod [%s]", podName);
      } else {
        return null;
      }
    }
  }
}
