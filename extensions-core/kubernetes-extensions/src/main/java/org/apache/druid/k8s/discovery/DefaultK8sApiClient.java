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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.Watch;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

/**
 * Concrete {@link K8sApiClient} impl using k8s-client java lib.
 */
public class DefaultK8sApiClient implements K8sApiClient
{
  private static final Logger LOGGER = new Logger(DefaultK8sApiClient.class);

  private final ApiClient realK8sClient;
  private final CoreV1Api coreV1Api;
  private final ObjectMapper jsonMapper;

  @Inject
  public DefaultK8sApiClient(ApiClient realK8sClient, @Json ObjectMapper jsonMapper)
  {
    this.realK8sClient = realK8sClient;
    this.coreV1Api = new CoreV1Api(realK8sClient);
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void patchPod(String podName, String podNamespace, String jsonPatchStr)
  {
    try {
      coreV1Api.patchNamespacedPod(podName, podNamespace, new V1Patch(jsonPatchStr), "true", null, null, null, null);
    }
    catch (ApiException ex) {
      throw new RE(ex, "Failed to patch pod[%s/%s], code[%d], error[%s].", podNamespace, podName, ex.getCode(), ex.getResponseBody());
    }
  }

  @Override
  public DiscoveryDruidNodeList listPods(
      String podNamespace,
      String labelSelector,
      NodeRole nodeRole
  )
  {
    try {
      V1PodList podList = coreV1Api.listNamespacedPod(podNamespace, null, null, null, null, labelSelector, 0, null, null, null, null, null);
      Preconditions.checkState(podList != null, "WTH: NULL podList");

      Map<String, DiscoveryDruidNode> allNodes = new HashMap();
      for (V1Pod podDef : podList.getItems()) {
        DiscoveryDruidNode node = getDiscoveryDruidNodeFromPodDef(nodeRole, podDef);
        allNodes.put(node.getDruidNode().getHostAndPortToUse(), node);
      }
      return new DiscoveryDruidNodeList(podList.getMetadata().getResourceVersion(), allNodes);
    }
    catch (ApiException ex) {
      throw new RE(ex, "Expection in listing pods, code[%d] and error[%s].", ex.getCode(), ex.getResponseBody());
    }
  }

  private DiscoveryDruidNode getDiscoveryDruidNodeFromPodDef(NodeRole nodeRole, V1Pod podDef)
  {
    String jsonStr = podDef.getMetadata().getAnnotations().get(K8sDruidNodeAnnouncer.getInfoAnnotation(nodeRole));
    try {
      return jsonMapper.readValue(jsonStr, DiscoveryDruidNode.class);
    }
    catch (JsonProcessingException ex) {
      throw new RE(ex, "Failed to deserialize DiscoveryDruidNode[%s]", jsonStr);
    }
  }

  @Override
  public WatchResult watchPods(String namespace, String labelSelector, String lastKnownResourceVersion, NodeRole nodeRole)
  {
    try {
      Watch<V1Pod> watch =
          Watch.createWatch(
              realK8sClient,
              coreV1Api.listNamespacedPodCall(namespace, null, true, null, null,
                                              labelSelector, null, lastKnownResourceVersion, null, null, 0, true, null
              ),
              new TypeReference<Watch.Response<V1Pod>>()
              {
              }.getType()
          );

      return new WatchResult()
      {
        private Watch.Response<DiscoveryDruidNodeAndResourceVersion> obj;

        @Override
        public boolean hasNext() throws SocketTimeoutException
        {
          try {
            while (watch.hasNext()) {
              Watch.Response<V1Pod> item = watch.next();
              if (item != null && item.type != null && !item.type.equals(WatchResult.BOOKMARK)) {
                DiscoveryDruidNodeAndResourceVersion result = null;
                if (item.object != null) {
                  result = new DiscoveryDruidNodeAndResourceVersion(
                    item.object.getMetadata().getResourceVersion(),
                    getDiscoveryDruidNodeFromPodDef(nodeRole, item.object)
                  );
                } else {
                  // The item's object can be null in some cases -- likely due to a blip
                  // in the k8s watch. Handle that by passing the null upwards. The caller
                  // needs to know that the object can be null.
                  LOGGER.debug("item of type " + item.type + " was NULL when watching nodeRole [%s]", nodeRole);
                }

                obj = new Watch.Response<DiscoveryDruidNodeAndResourceVersion>(
                    item.type,
                    result
                );
                return true;
              } else if (item != null && item.type != null && item.type.equals(WatchResult.BOOKMARK)) {
                // Events with type BOOKMARK will only contain resourceVersion and no metadata. See
                // Kubernetes API documentation for details.
                LOGGER.debug("BOOKMARK event fired, no nothing, only update resourceVersion");
                return true;
              } else {
                LOGGER.error("WTH! item or item.type is NULL");
              }
            }
          }
          catch (RuntimeException ex) {
            if (ex.getCause() instanceof SocketTimeoutException) {
              throw (SocketTimeoutException) ex.getCause();
            } else {
              throw ex;
            }
          }

          return false;
        }

        @Override
        public Watch.Response<DiscoveryDruidNodeAndResourceVersion> next()
        {
          return obj;
        }

        @Override
        public void close()
        {
          try {
            watch.close();
          }
          catch (IOException ex) {
            throw new RE(ex, "Exception while closing watch.");
          }
        }
      };
    }
    catch (ApiException ex) {
      if (ex.getCode() == 410) {
        // k8s no longer has history that we need
        return null;
      }

      throw new RE(ex, "Expection in watching pods, code[%d] and error[%s].", ex.getCode(), ex.getResponseBody());
    }
  }
}
