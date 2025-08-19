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

package org.apache.druid.testing.embedded.kubernetes;

import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Druid Coordinator service component for managing data availability and segment assignment.
 * Also configured to run as Overlord for task management.
 */
public class DruidK8sCoordinatorComponent extends DruidK8sComponent
{
  private static final String COORDINATOR_CONFIG_MOUNT_PATH = "/opt/druid/conf/druid/cluster/master/coordinator-overlord";
  private static final int COORDINATOR_READINESS_TIMEOUT = 1800;

  public DruidK8sCoordinatorComponent(String namespace, String druidImage, String clusterName)
  {
    super(namespace, druidImage, clusterName);  }

  @Override
  public String getDruidServiceType()
  {
    return "coordinator";
  }

  @Override
  public int getDruidPort()
  {
    return DRUID_PORT;
  }

  @Override
  public Properties getRuntimeProperties()
  {
    Properties props = new Properties();
    props.setProperty("druid.service", "druid/coordinator");
    props.setProperty("druid.coordinator.startDelay", "PT30S");
    props.setProperty("druid.coordinator.period", "PT30S");
    props.setProperty("druid.coordinator.asOverlord.enabled", "true");
    props.setProperty("druid.coordinator.asOverlord.overlordService", "druid/overlord");
    props.setProperty("druid.indexer.queue.startDelay", "PT30S");
    props.setProperty("druid.indexer.runner.capacity", "2");
    props.setProperty("druid.indexer.runner.namespace", namespace);
    props.setProperty("druid.indexer.runner.type", "k8s");
    props.setProperty("druid.indexer.logs.type", "file");
    props.setProperty("druid.indexer.task.encapsulatedTask", "true");
    props.setProperty("druid.host", "druid-" + getMetadataName() + "-" + getDruidServiceType());
    return props;
  }

  @Override
  public String getJvmOptions()
  {
    return "-server\n" +
           "-Djava.net.preferIPv4Stack=true\n" +
           "-XX:MaxDirectMemorySize=1g\n" +
           "-Duser.timezone=UTC\n" +
           "-Dfile.encoding=UTF-8\n" +
           "-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager\n" +
           "-Xmx800m\n" +
           "-Xms800m";
  }

  @Override
  public void initialize(KubernetesClient client) throws Exception
  {
    applyDruidManifest(client);
  }

  @Override
  public Map<String, Object> getNodeConfig()
  {
    Map<String, Object> nodeConfig = getCommonNodeConfig(); // Use common NodePort config
    
    nodeConfig.put("nodeType", "coordinator");
    nodeConfig.put("druid.port", getDruidPort());
    nodeConfig.put("replicas", getReplicas());

    nodeConfig.put("nodeConfigMountPath", COORDINATOR_CONFIG_MOUNT_PATH);
    nodeConfig.put("livenessProbe", getLivenessProbe());
    nodeConfig.put("readinessProbe", getReadinessProbe());
    nodeConfig.put("runtime.properties", getRuntimePropertiesAsString().replace(getCommonDruidProperties(), "").trim());
    nodeConfig.put("extra.jvm.options", getJvmOptions());
    return nodeConfig;
  }
  
  @Override
  public String getNodeName()
  {
    return "coordinator";
  }

  @Override
  public int getReadyTimeoutSeconds()
  {
    return COORDINATOR_READINESS_TIMEOUT;
  }

}