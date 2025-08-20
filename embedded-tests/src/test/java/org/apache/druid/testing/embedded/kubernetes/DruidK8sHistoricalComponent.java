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
 * Druid Historical service component for storing and querying immutable historical data.
 */
public class DruidK8sHistoricalComponent extends DruidK8sComponent
{

  private static final String DEFAULT_TIER = "default";
  private static final String HISTORICAL_CONFIG_MOUNT_PATH = "/opt/druid/conf/druid/cluster/data/historical";

  private final String tier;
  private final int priority;
  private final long maxSize;
  private final String storageDir;

  public DruidK8sHistoricalComponent(
      String namespace,
      String druidImage,
      String clusterName,
      String tier,
      int priority,
      String storageDir
  )
  {
    super(namespace, druidImage, clusterName);
    this.tier = tier != null ? tier : DEFAULT_TIER;
    this.priority = priority;
    this.maxSize = DEFAULT_TIER.equals(this.tier) ? 1000000000L : 2000000000L;
    this.storageDir = storageDir;
  }


  @Override
  public String getDruidServiceType()
  {
    return "historical";
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
    props.setProperty("druid.service", "druid/" + tier);
    props.setProperty("druid.server.tier", tier);
    props.setProperty("druid.server.priority", String.valueOf(priority));
    props.setProperty("druid.processing.buffer.sizeBytes", "25000000");
    props.setProperty("druid.processing.numThreads", "2");
    props.setProperty(
        "druid.segmentCache.locations",
        "[{\"path\":\"" + storageDir + "\",\"maxSize\":" + maxSize + "}]"
    );
    props.setProperty("druid.segmentCache.numLoadingThreads", "4");
    props.setProperty("druid.segmentCache.announceIntervalMillis", "3000");
    props.setProperty("druid.server.maxSize", String.valueOf(maxSize));

    // Historical-specific configurations for segment loading
    props.setProperty("druid.segmentCache.infoDir", "/druid/data/segment-cache");
    props.setProperty("druid.historical.cache.useCache", "true");
    props.setProperty("druid.historical.cache.populateCache", "true");

    props.setProperty("druid.host", "druid-" + getMetadataName() + "-" + getTier());
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

  public String getTier()
  {
    return tier;
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
    nodeConfig.put("nodeType", "historical");
    nodeConfig.put("druid.port", getDruidPort());
    nodeConfig.put("replicas", getReplicas());

    nodeConfig.put("nodeConfigMountPath", HISTORICAL_CONFIG_MOUNT_PATH);
    nodeConfig.put("livenessProbe", getLivenessProbe());
    nodeConfig.put("readinessProbe", getReadinessProbe());

    // Remove individual volumes and volumeMounts - use shared storage from base class
    nodeConfig.put("runtime.properties", StringUtils.replace(getRuntimePropertiesAsString(), getCommonDruidProperties(), "").trim());
    nodeConfig.put("extra.jvm.options", getJvmOptions());
    return nodeConfig;
  }

  @Override
  public String getMetadataName()
  {
    return clusterName + "-" + getDruidServiceType() + "-" + tier;
  }

  @Override
  public String getNodeName()
  {
    return tier;
  }

  @Override
  public String getPodLabel()
  {
    return "druid-" + getMetadataName() + "-" + getTier();
  }
}
