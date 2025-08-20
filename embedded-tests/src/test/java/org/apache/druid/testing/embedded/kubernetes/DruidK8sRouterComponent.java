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
import org.apache.druid.java.util.common.StringUtils;

import java.util.Map;
import java.util.Properties;

/**
 * Druid Router service component for routing requests and running web console.
 */
public class DruidK8sRouterComponent extends DruidK8sComponent
{
  private static final String ROUTER_CONFIG_MOUNT_PATH = "/opt/druid/conf/druid/cluster/query/router";

  public DruidK8sRouterComponent(String namespace, String druidImage, String clusterName)
  {
    super(namespace, druidImage, clusterName);
  }
  
  @Override
  protected boolean needsSharedStorage()
  {
    return false; // Router doesn't need shared storage
  }

  @Override
  public String getDruidServiceType()
  {
    return "router";
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
    props.setProperty("druid.service", "druid/router");
    props.setProperty("druid.router.http.numConnections", "50");
    props.setProperty("druid.router.http.readTimeout", "PT5M");
    props.setProperty("druid.router.http.numMaxThreads", "100");
    props.setProperty("druid.server.http.numThreads", "100");
    props.setProperty("druid.router.defaultBrokerServiceName", "druid/broker");
    props.setProperty("druid.router.coordinatorServiceName", "druid/coordinator");
    props.setProperty("druid.router.managementProxy.enabled", "true");
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
           "-Xmx512m\n" +
           "-Xms512m";
  }

  @Override
  public void initialize(KubernetesClient client) throws Exception
  {
    applyDruidManifest(client);
  }

  @Override
  public Map<String, Object> getNodeConfig()
  {
    Map<String, Object> nodeConfig = getCommonNodeConfig();
    nodeConfig.put("nodeType", "router");
    nodeConfig.put("druid.port", getDruidPort());
    nodeConfig.put("replicas", getReplicas());
    nodeConfig.put("nodeConfigMountPath", ROUTER_CONFIG_MOUNT_PATH);
    nodeConfig.put("livenessProbe", getLivenessProbe());
    nodeConfig.put("readinessProbe", getReadinessProbe());
    nodeConfig.put("runtime.properties", StringUtils.replace(getRuntimePropertiesAsString(), getCommonDruidProperties(), "").trim());
    nodeConfig.put("extra.jvm.options", getJvmOptions());
    return nodeConfig;
  }

  @Override
  public String getNodeName()
  {
    return "router";
  }

}
