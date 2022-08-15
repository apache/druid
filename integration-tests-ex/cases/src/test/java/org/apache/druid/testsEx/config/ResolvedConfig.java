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

package org.apache.druid.testsEx.config;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.curator.ExhibitorConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testsEx.config.ClusterConfig.ClusterType;
import org.apache.druid.testsEx.config.ResolvedService.ResolvedKafka;
import org.apache.druid.testsEx.config.ResolvedService.ResolvedZk;
import org.apache.druid.testsEx.config.ServiceConfig.DruidConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ResolvedConfig
{
  public static final String COORDINATOR = "coordinator";
  public static final String HISTORICAL = "historical";
  public static final String OVERLORD = "overlord";
  public static final String BROKER = "broker";
  public static final String ROUTER = "router";
  public static final String MIDDLEMANAGER = "middlemanager";
  public static final String INDEXER = "indexer";

  public static final int DEFAULT_READY_TIMEOUT_SEC = 120;
  public static final int DEFAULT_READY_POLL_MS = 2000;

  private final ClusterType type;
  private final String proxyHost;
  private final int readyTimeoutSec;
  private final int readyPollMs;
  private final String datasourceNameSuffix;
  private Map<String, Object> properties;

  private final ResolvedZk zk;
  private final ResolvedKafka kafka;
  private final ResolvedMetastore metastore;
  private final Map<String, ResolvedDruidService> druidServices = new HashMap<>();

  public ResolvedConfig(ClusterConfig config)
  {
    type = config.type() == null ? ClusterType.docker : config.type();
    if (!hasProxy()) {
      proxyHost = null;
    } else if (Strings.isNullOrEmpty(config.proxyHost())) {
      proxyHost = "localhost";
    } else {
      proxyHost = config.proxyHost();
    }
    readyTimeoutSec = config.readyTimeoutSec() > 0 ?
        config.readyTimeoutSec() : DEFAULT_READY_TIMEOUT_SEC;
    readyPollMs = config.readyPollMs() > 0 ? config.readyPollMs() : DEFAULT_READY_POLL_MS;
    if (config.properties() == null) {
      this.properties = ImmutableMap.of();
    } else {
      this.properties = config.properties();
    }
    if (config.datasourceSuffix() == null) {
      this.datasourceNameSuffix = "";
    } else {
      this.datasourceNameSuffix = config.datasourceSuffix();
    }

    if (config.zk() == null) {
      this.zk = null;
    } else {
      this.zk = new ResolvedZk(this, config.zk());
    }
    if (config.kafka() == null) {
      this.kafka = null;
    } else {
      this.kafka = new ResolvedKafka(this, config.kafka());
    }
    if (config.metastore() == null) {
      this.metastore = null;
    } else {
      this.metastore = new ResolvedMetastore(this, config.metastore(), config);
    }

    if (config.druid() != null) {
      for (Entry<String, DruidConfig> entry : config.druid().entrySet()) {
        druidServices.put(entry.getKey(),
            new ResolvedDruidService(this, entry.getValue(), entry.getKey()));
      }
    }
  }

  public ClusterType type()
  {
    return type;
  }

  public String proxyHost()
  {
    return proxyHost;
  }

  public int readyTimeoutSec()
  {
    return readyTimeoutSec;
  }

  public int readyPollMs()
  {
    return readyPollMs;
  }

  public boolean isDocker()
  {
    return type == ClusterType.docker;
  }

  public boolean hasProxy()
  {
    switch (type) {
      case docker:
      case k8s:
        return true;
      default:
        return false;
    }
  }

  public ResolvedZk zk()
  {
    return zk;
  }

  public ResolvedMetastore metastore()
  {
    return metastore;
  }

  public ResolvedKafka kafka()
  {
    return kafka;
  }

  public Map<String, Object> properties()
  {
    return properties;
  }

  public Map<String, ResolvedDruidService> requireDruid()
  {
    if (druidServices == null) {
      throw new ISE("Please configure Druid services");
    }
    return druidServices;
  }

  public ResolvedMetastore requireMetastore()
  {
    if (metastore == null) {
      throw new ISE("Please specify the Metastore configuration");
    }
    return metastore;
  }

  public ResolvedKafka requireKafka()
  {
    if (kafka == null) {
      throw new ISE("Please specify the Kafka configuration");
    }
    return kafka;
  }

  public ResolvedDruidService druidService(String serviceKey)
  {
    return requireDruid().get(serviceKey);
  }

  public ResolvedDruidService requireService(String serviceKey)
  {
    ResolvedDruidService service = druidService(serviceKey);
    if (service == null) {
      throw new ISE("Please configure Druid service " + serviceKey);
    }
    return service;
  }

  public ResolvedDruidService requireCoordinator()
  {
    return requireService(COORDINATOR);
  }

  public ResolvedDruidService requireOverlord()
  {
    return requireService(OVERLORD);
  }

  public ResolvedDruidService requireBroker()
  {
    return requireService(BROKER);
  }

  public ResolvedDruidService requireRouter()
  {
    return requireService(ROUTER);
  }

  public ResolvedDruidService requireMiddleManager()
  {
    return requireService(MIDDLEMANAGER);
  }

  public ResolvedDruidService requireHistorical()
  {
    return requireService(HISTORICAL);
  }

  public String routerUrl()
  {
    return requireRouter().clientUrl();
  }

  public CuratorConfig toCuratorConfig()
  {
    if (zk == null) {
      throw new ISE("ZooKeeper not configured");
    }
    // TODO: Add a builder for other properties
    return CuratorConfig.create(zk.clientHosts());
  }

  public ExhibitorConfig toExhibitorConfig()
  {
    // Does not yet support exhibitors
    return ExhibitorConfig.create(Collections.emptyList());
  }

  /**
   * Convert the config in this structure the the properties
   * used to configure Guice.
   */
  public Map<String, Object> toProperties()
  {
    Map<String, Object> properties = new HashMap<>();
    if (proxyHost != null) {
      properties.put("druid.test.config.dockerIp", proxyHost);
    }

    // Start with implicit properties from various sections.
    if (zk != null) {
      properties.putAll(zk.toProperties());
    }
    if (metastore != null) {
      properties.putAll(metastore.toProperties());
    }

    // Add explicit properties
    if (this.properties != null) {
      properties.putAll(properties);
    }
    return properties;
  }

  public IntegrationTestingConfig toIntegrationTestingConfig()
  {
    return new IntegrationTestingConfigShim();
  }

  /**
   * Adapter to the "legacy" cluster configuration used by tests.
   */
  private class IntegrationTestingConfigShim implements IntegrationTestingConfig
  {
    @Override
    public String getZookeeperHosts()
    {
      return zk.clientHosts();
    }

    @Override
    public String getKafkaHost()
    {
      return requireKafka().instance().clientHost();
    }

    @Override
    public String getKafkaInternalHost()
    {
      return requireKafka().instance().host();
    }

    @Override
    public String getBrokerHost()
    {
      return requireBroker().instance().clientHost();
    }

    @Override
    public String getBrokerInternalHost()
    {
      return requireBroker().instance().host();
    }

    @Override
    public String getRouterHost()
    {
      return requireRouter().instance().clientHost();
    }

    @Override
    public String getRouterInternalHost()
    {
      return requireRouter().instance().host();
    }

    @Override
    public String getCoordinatorHost()
    {
      return requireCoordinator().tagOrDefault("one").clientHost();
    }

    @Override
    public String getCoordinatorInternalHost()
    {
      return requireCoordinator().tagOrDefault("one").host();
    }

    @Override
    public String getCoordinatorTwoInternalHost()
    {
      return requireCoordinator().requireInstance("two").host();
    }

    @Override
    public String getCoordinatorTwoHost()
    {
      return requireCoordinator().tagOrDefault("one").clientHost();
    }

    @Override
    public String getOverlordHost()
    {
      return requireOverlord().tagOrDefault("one").clientHost();
    }

    @Override
    public String getOverlordTwoHost()
    {
      return requireOverlord().tagOrDefault("two").clientHost();
    }

    @Override
    public String getOverlordInternalHost()
    {
      return requireOverlord().tagOrDefault("one").host();
    }

    @Override
    public String getOverlordTwoInternalHost()
    {
      return requireOverlord().requireInstance("two").host();
    }

    @Override
    public String getMiddleManagerHost()
    {
      return requireMiddleManager().instance().clientHost();
    }

    @Override
    public String getMiddleManagerInternalHost()
    {
      return requireMiddleManager().instance().host();
    }

    @Override
    public String getHistoricalHost()
    {
      return requireHistorical().instance().clientHost();
    }

    @Override
    public String getHistoricalInternalHost()
    {
      return requireHistorical().instance().host();
    }

    @Override
    public String getCoordinatorUrl()
    {
      ResolvedDruidService config = requireCoordinator();
      return config.resolveUrl(config.tagOrDefault("one"));
    }

    @Override
    public String getCoordinatorTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCoordinatorTwoUrl()
    {
      ResolvedDruidService config = requireCoordinator();
      return config.resolveUrl(config.requireInstance("two"));
    }

    @Override
    public String getCoordinatorTwoTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getOverlordUrl()
    {
      ResolvedDruidService config = requireOverlord();
      return config.resolveUrl(config.tagOrDefault("one"));
    }

    @Override
    public String getOverlordTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getOverlordTwoUrl()
    {
      ResolvedDruidService config = requireOverlord();
      return config.resolveUrl(config.requireInstance("two"));
    }

    @Override
    public String getOverlordTwoTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getIndexerUrl()
    {
      ResolvedDruidService indexer = druidService(INDEXER);
      if (indexer == null) {
        indexer = requireMiddleManager();
      }
      return indexer.resolveUrl(indexer.instance());
    }

    @Override
    public String getIndexerTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getRouterUrl()
    {
      return routerUrl();
    }

    @Override
    public String getRouterTLSUrl()
    {
      ResolvedDruidService config = requireRouter();
      return config.resolveUrl(config.tagOrDefault("tls"));
    }

    @Override
    public String getPermissiveRouterUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getPermissiveRouterTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getNoClientAuthRouterUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getNoClientAuthRouterTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCustomCertCheckRouterUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCustomCertCheckRouterTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getBrokerUrl()
    {
      ResolvedDruidService config = requireBroker();
      return config.resolveUrl(config.instance());
    }

    @Override
    public String getBrokerTLSUrl()
    {
      ResolvedDruidService config = requireBroker();
      return config.resolveUrl(config.tagOrDefault("tls"));
    }

    @Override
    public String getHistoricalUrl()
    {
      return requireHistorical().resolveUrl();
    }

    @Override
    public String getHistoricalTLSUrl()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getProperty(String prop)
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getUsername()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getPassword()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public Map<String, String> getProperties()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public boolean manageKafkaTopic()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getExtraDatasourceNameSuffix()
    {
      return datasourceNameSuffix;
    }

    @Override
    public String getCloudBucket()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCloudPath()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getCloudRegion()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getS3AssumeRoleWithExternalId()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getS3AssumeRoleExternalId()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getS3AssumeRoleWithoutExternalId()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getAzureKey()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getHadoopGcsCredentialsPath()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getStreamEndpoint()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public String getSchemaRegistryHost()
    {
      throw new ISE("Not implemented");
    }

    @Override
    public boolean isDocker()
    {
      return ResolvedConfig.this.isDocker();
    }

    @Override
    public String getDockerHost()
    {
      return proxyHost();
    }
  }
}
