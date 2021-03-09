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
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Announcement creates following in the pod def...
 *
 * Labels -
 * druidDiscoveryAnnouncement-<nodeRole.getJsonName()> = true
 * druidDiscoveryAnnouncement-id = encodeHostPort(host:port)
 * druidDiscoveryAnnouncement-cluster-identifier = <clusterIdentifier>
 *
 * Annotation -
 * druidNodeInfo-<nodeRole.getJsonName()> = json_serialize(DiscoveryDruidNode)
 *
 * Note that, a node can have multiple roles e.g. coordinator can take up overlord's role as well.
 */
public class K8sDruidNodeAnnouncer implements DruidNodeAnnouncer
{
  private static final Logger LOGGER = new Logger(K8sDruidNodeAnnouncer.class);

  private static String POD_LABELS_PATH_PREFIX = "/metadata/labels";
  private static String POD_ANNOTATIONS_PATH_PREFIX = "/metadata/annotations";

  private static final String OP_ADD = "add";
  private static final String OP_REMOVE = "remove";

  public static final String ANNOUNCEMENT_DONE = "true";

  private final ObjectMapper jsonMapper;
  private final K8sDiscoveryConfig discoveryConfig;
  private final PodInfo podInfo;
  private final K8sApiClient k8sApiClient;

  @Inject
  public K8sDruidNodeAnnouncer(
      PodInfo podInfo,
      K8sDiscoveryConfig discoveryConfig,
      K8sApiClient k8sApiClient,
      @Json ObjectMapper jsonMapper
  )
  {
    this.discoveryConfig = discoveryConfig;
    this.podInfo = podInfo;
    this.k8sApiClient = k8sApiClient;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void announce(DiscoveryDruidNode discoveryDruidNode)
  {
    LOGGER.info("Announcing DiscoveryDruidNode[%s]", discoveryDruidNode);

    String roleAnnouncementLabel = getRoleAnnouncementLabel(discoveryDruidNode.getNodeRole());
    String idAnnouncementLabel = getIdAnnouncementLabel();
    String clusterIdentifierAnnouncementLabel = getClusterIdentifierAnnouncementLabel();
    String infoAnnotation = getInfoAnnotation(discoveryDruidNode.getNodeRole());

    try {
      List<Map<String, Object>> patches = new ArrayList<>();

      // Note: We assume here that at least one label and annotation exists on the pod already, so that
      // paths where labels/annotations are created, pre-exist.
      // See https://github.com/kubernetes-sigs/kustomize/issues/2986 , we can add workaround of getting pod spec,
      // checking if label/annotation path exists and create if not, however that could lead to race conditions
      // so assuming the existence for now.
      patches.add(createPatchObj(OP_ADD, getPodDefLabelPath(roleAnnouncementLabel), ANNOUNCEMENT_DONE));
      patches.add(createPatchObj(OP_ADD, getPodDefLabelPath(idAnnouncementLabel), encodeHostPort(discoveryDruidNode.getDruidNode().getHostAndPortToUse())));
      patches.add(createPatchObj(OP_ADD, getPodDefLabelPath(clusterIdentifierAnnouncementLabel), discoveryConfig.getClusterIdentifier()));
      patches.add(createPatchObj(OP_ADD, getPodDefAnnocationPath(infoAnnotation), jsonMapper.writeValueAsString(discoveryDruidNode)));

      // Creating patch string outside of retry block to not retry json serialization failures
      String jsonPatchStr = jsonMapper.writeValueAsString(patches);
      LOGGER.info("Json Patch For Node Announcement: [%s]", jsonPatchStr);

      RetryUtils.retry(
          () -> {
            k8sApiClient.patchPod(podInfo.getPodName(), podInfo.getPodNamespace(), jsonPatchStr);
            return "na";
          },
          (throwable) -> true,
          3
      );

      LOGGER.info("Announced DiscoveryDruidNode[%s]", discoveryDruidNode);
    }
    catch (Exception ex) {
      throw new RE(ex, "Failed to announce DiscoveryDruidNode[%s]", discoveryDruidNode);
    }
  }

  @Override
  public void unannounce(DiscoveryDruidNode discoveryDruidNode)
  {
    LOGGER.info("Unannouncing DiscoveryDruidNode[%s]", discoveryDruidNode);

    String roleAnnouncementLabel = getRoleAnnouncementLabel(discoveryDruidNode.getNodeRole());
    String idAnnouncementLabel = getIdAnnouncementLabel();
    String clusterIdentifierAnnouncementLabel = getClusterIdentifierAnnouncementLabel();
    String infoAnnotation = getInfoAnnotation(discoveryDruidNode.getNodeRole());

    try {
      List<Map<String, Object>> patches = new ArrayList<>();
      patches.add(createPatchObj(OP_REMOVE, getPodDefLabelPath(roleAnnouncementLabel), null));
      patches.add(createPatchObj(OP_REMOVE, getPodDefLabelPath(idAnnouncementLabel), null));
      patches.add(createPatchObj(OP_REMOVE, getPodDefLabelPath(clusterIdentifierAnnouncementLabel), null));
      patches.add(createPatchObj(OP_REMOVE, getPodDefAnnocationPath(infoAnnotation), null));

      // Creating patch string outside of retry block to not retry json serialization failures
      String jsonPatchStr = jsonMapper.writeValueAsString(patches);

      RetryUtils.retry(
          () -> {
            k8sApiClient.patchPod(podInfo.getPodName(), podInfo.getPodNamespace(), jsonPatchStr);
            return "na";
          },
          (throwable) -> true,
          3
      );

      LOGGER.info("Unannounced DiscoveryDruidNode[%s]", discoveryDruidNode);

    }
    catch (Exception ex) {
      // Unannouncement happens when druid process is shutting down, there is no point throwing exception
      // in shutdown sequence.
      if (ex instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }

      LOGGER.error(ex, "Failed to unannounce DiscoveryDruidNode[%s]", discoveryDruidNode);
    }
  }

  private Map<String, Object> createPatchObj(String op, String path, Object value)
  {
    if (value == null) {
      return ImmutableMap.of(
          "op", op,
          "path", path
      );
    } else {
      return ImmutableMap.of(
          "op", op,
          "path", path,
          "value", value
      );
    }
  }

  public static String getRoleAnnouncementLabel(NodeRole nodeRole)
  {
    return StringUtils.format("druidDiscoveryAnnouncement-%s", nodeRole.getJsonName());
  }

  private static String getIdAnnouncementLabel()
  {
    return "druidDiscoveryAnnouncement-id";
  }

  public static String getClusterIdentifierAnnouncementLabel()
  {
    return "druidDiscoveryAnnouncement-cluster-identifier";
  }

  public static String getInfoAnnotation(NodeRole nodeRole)
  {
    return StringUtils.format("druidNodeInfo-%s", nodeRole.getJsonName());
  }

  public static String getLabelSelectorForNodeRole(K8sDiscoveryConfig discoveryConfig, NodeRole nodeRole)
  {
    return StringUtils.format(
        "%s=%s,%s=%s",
        getClusterIdentifierAnnouncementLabel(),
        discoveryConfig.getClusterIdentifier(),
        K8sDruidNodeAnnouncer.getRoleAnnouncementLabel(nodeRole),
        K8sDruidNodeAnnouncer.ANNOUNCEMENT_DONE
    );
  }

  public static String getLabelSelectorForNode(K8sDiscoveryConfig discoveryConfig, NodeRole nodeRole, DruidNode node)
  {
    return StringUtils.format(
        "%s=%s,%s=%s,%s=%s",
        getClusterIdentifierAnnouncementLabel(),
        discoveryConfig.getClusterIdentifier(),
        K8sDruidNodeAnnouncer.getRoleAnnouncementLabel(nodeRole),
        K8sDruidNodeAnnouncer.ANNOUNCEMENT_DONE,
        K8sDruidNodeAnnouncer.getIdAnnouncementLabel(),
        encodeHostPort(node.getHostAndPortToUse())
    );
  }

  private String getPodDefLabelPath(String label)
  {
    return StringUtils.format("%s/%s", POD_LABELS_PATH_PREFIX, label);
  }

  private String getPodDefAnnocationPath(String annotation)
  {
    return StringUtils.format("%s/%s", POD_ANNOTATIONS_PATH_PREFIX, annotation);
  }

  private static String encodeHostPort(String hostPort)
  {
    //K8S requires that label values must match regex (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?
    //So, it is essential to replace ':' with '-'

    // it is assumed that hostname does not have ':' in it except for separating host and port
    Preconditions.checkState(
        hostPort.indexOf(':') == hostPort.lastIndexOf(':'),
        "hostname in host:port[%s] has ':' in it", hostPort
    );

    return hostPort.replace(':', '-');
  }

  private String replaceLast(String str, char oldChar, char newChar)
  {
    char[] chars = str.toCharArray();
    for (int i = chars.length - 1; i >= 0; i--) {
      if (chars[i] == oldChar) {
        chars[i] = newChar;
        break;
      }
    }

    return String.valueOf(chars);
  }
}
