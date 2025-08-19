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
import java.util.Map;
import java.util.Properties;

/**
 * Druid Broker service component for handling queries.
 */
public class DruidK8sBrokerComponent extends DruidK8sComponent
{
  private static final String BROKER_CONFIG_MOUNT_PATH = "/opt/druid/conf/druid/cluster/query/broker";

  public DruidK8sBrokerComponent(String namespace, String druidImage, String clusterName)
  {
    super(namespace, druidImage, clusterName);
  }

  @Override
  public String getDruidServiceType()
  {
    return "broker";
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
    props.setProperty("druid.service", "druid/broker");
    props.setProperty("druid.broker.http.numConnections", "5");
    props.setProperty("druid.server.http.numThreads", "40");
    props.setProperty("druid.processing.buffer.sizeBytes", "25000000");
    props.setProperty("druid.sql.enable", "true");
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
    nodeConfig.put("nodeType", "broker");
    nodeConfig.put("druid.port", getDruidPort());
    nodeConfig.put("replicas", getReplicas());
    nodeConfig.put("nodeConfigMountPath", BROKER_CONFIG_MOUNT_PATH);
    nodeConfig.put("livenessProbe", getLivenessProbe());
    nodeConfig.put("readinessProbe", getReadinessProbe());
    nodeConfig.put("runtime.properties", getRuntimePropertiesAsString().replace(getCommonDruidProperties(), "").trim());
    nodeConfig.put("extra.jvm.options", getJvmOptions());
    return nodeConfig;
  }

  @Override
  public String getNodeName()
  {
    return "broker";
  }


}