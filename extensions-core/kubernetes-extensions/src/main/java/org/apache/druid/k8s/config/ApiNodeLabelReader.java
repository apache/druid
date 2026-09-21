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

package org.apache.druid.k8s.config;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.ClientBuilder;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads the labels of a Kubernetes node from the in-cluster API server.
 * <p>
 * {@link ClientBuilder#cluster()} supplies the endpoint from the environment, the trust material from the projected
 * cluster CA, and a token source that re-reads the projected token as the kubelet rotates it.
 */
class ApiNodeLabelReader implements NodeLabelReader
{
  private static final Logger log = new Logger(ApiNodeLabelReader.class);

  // A missing label is survivable, a slow task start is not.
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private final CoreV1Api api;

  ApiNodeLabelReader(ApiClient apiClient, Duration timeout)
  {
    apiClient.setHttpClient(
        apiClient.getHttpClient()
                 .newBuilder()
                 // Bounds the whole call. A read timeout alone lets a stalled response body run on far longer.
                 .callTimeout(timeout)
                 // Keeps the bearer token from reaching any host but the API server.
                 .followRedirects(false)
                 .followSslRedirects(false)
                 .build()
    );
    this.api = new CoreV1Api(apiClient);
  }

  /**
   * Returns the labels of the named node. Returns null if the read failed, or if the client could not be built,
   * which is the case outside Kubernetes.
   */
  @Nullable
  static Map<String, String> readFromCluster(String nodeName)
  {
    final ApiClient apiClient;
    try {
      apiClient = ClientBuilder.cluster().build();
    }
    catch (Exception e) {
      // Missing credentials or endpoint is configuration rather than a bug, so the cause is named without a trace.
      log.warn("Could not build a Kubernetes API client [%s], so the labels of node [%s] cannot be read.", e, nodeName);
      return null;
    }

    try {
      return new ApiNodeLabelReader(apiClient, TIMEOUT).readLabels(nodeName);
    }
    finally {
      // OkHttp is not closeable, and this client made its one call, so drop the pooled socket now rather than
      // leave it reachable from the shared connection pool until it idles out.
      apiClient.getHttpClient().connectionPool().evictAll();
    }
  }

  @Nullable
  @Override
  public Map<String, String> readLabels(String nodeName)
  {
    try {
      final V1Node node = api.readNode(nodeName, null);
      final V1ObjectMeta metadata = node == null ? null : node.getMetadata();
      if (metadata == null) {
        // Not a node, so a failure. An empty map here would be cached as if the node had no labels.
        log.warn("Kubernetes API server returned no object metadata for node [%s].", nodeName);
        return null;
      }

      final Map<String, String> labels = metadata.getLabels();
      return labels == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(labels));
    }
    catch (Exception e) {
      // An ApiException without a status code is a connection failure or the call timeout, not an answer.
      if (e instanceof ApiException apiException && apiException.getCode() != 0) {
        log.warn(
            "Kubernetes API server answered [%d] when asked for node [%s]: %s."
            + " Reading a node requires a ClusterRole granting 'get' on 'nodes'.",
            apiException.getCode(),
            nodeName,
            apiException.getResponseBody()
        );
      } else {
        log.warn(e, "Could not read the labels of node [%s] from the Kubernetes API server.", nodeName);
      }
      return null;
    }
  }
}
