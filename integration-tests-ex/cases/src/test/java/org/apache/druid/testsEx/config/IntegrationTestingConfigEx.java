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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.IntegrationTestingConfigProvider;

import javax.inject.Inject;

import java.util.Map;
import java.util.Properties;

/**
 * Adapter to the "legacy" cluster configuration used by tests.
 */
class IntegrationTestingConfigEx implements IntegrationTestingConfig
{
  private final ResolvedConfig config;
  private final Map<String, String> properties;

  @Inject
  public IntegrationTestingConfigEx(
      final ResolvedConfig config,
      final Properties properties)
  {
    this.config = config;
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      String key = (String) entry.getKey();
      if (key.startsWith(IntegrationTestingConfigProvider.PROPERTY_BASE)) {
        key = key.substring(IntegrationTestingConfigProvider.PROPERTY_BASE.length() + 1);
        builder.put(key, (String) entry.getValue());
      }
    }
    this.properties = builder.build();
  }

  @Override
  public String getZookeeperHosts()
  {
    return config.requireZk().clientHosts();
  }

  @Override
  public String getKafkaHost()
  {
    return config.requireKafka().instance().clientHost();
  }

  @Override
  public String getKafkaInternalHost()
  {
    return config.requireKafka().instance().host();
  }

  @Override
  public String getBrokerHost()
  {
    return config.requireBroker().instance().clientHost();
  }

  @Override
  public String getBrokerInternalHost()
  {
    return config.requireBroker().instance().host();
  }

  @Override
  public String getRouterHost()
  {
    return config.requireRouter().instance().clientHost();
  }

  @Override
  public String getRouterInternalHost()
  {
    return config.requireRouter().instance().host();
  }

  @Override
  public String getCoordinatorHost()
  {
    return config.requireCoordinator().tagOrDefault("one").clientHost();
  }

  @Override
  public String getCoordinatorInternalHost()
  {
    return config.requireCoordinator().tagOrDefault("one").host();
  }

  @Override
  public String getCoordinatorTwoInternalHost()
  {
    return config.requireCoordinator().requireInstance("two").host();
  }

  @Override
  public String getCoordinatorTwoHost()
  {
    return config.requireCoordinator().tagOrDefault("one").clientHost();
  }

  @Override
  public String getOverlordHost()
  {
    return config.requireOverlord().tagOrDefault("one").clientHost();
  }

  @Override
  public String getOverlordTwoHost()
  {
    return config.requireOverlord().tagOrDefault("two").clientHost();
  }

  @Override
  public String getOverlordInternalHost()
  {
    return config.requireOverlord().tagOrDefault("one").host();
  }

  @Override
  public String getOverlordTwoInternalHost()
  {
    return config.requireOverlord().requireInstance("two").host();
  }

  @Override
  public String getMiddleManagerHost()
  {
    return config.requireMiddleManager().instance().clientHost();
  }

  @Override
  public String getMiddleManagerInternalHost()
  {
    return config.requireMiddleManager().instance().host();
  }

  @Override
  public String getHistoricalHost()
  {
    return config.requireHistorical().instance().clientHost();
  }

  @Override
  public String getHistoricalInternalHost()
  {
    return config.requireHistorical().instance().host();
  }

  @Override
  public String getCoordinatorUrl()
  {
    ResolvedDruidService serviceConfig = config.requireCoordinator();
    return serviceConfig.resolveUrl(serviceConfig.tagOrDefault("one"));
  }

  @Override
  public String getCoordinatorTLSUrl()
  {
    throw new ISE("Not implemented");
  }

  @Override
  public String getCoordinatorTwoUrl()
  {
    ResolvedDruidService serviceConfig = config.requireCoordinator();
    return serviceConfig.resolveUrl(serviceConfig.requireInstance("two"));
  }

  @Override
  public String getCoordinatorTwoTLSUrl()
  {
    throw new ISE("Not implemented");
  }

  @Override
  public String getOverlordUrl()
  {
    ResolvedDruidService serviceConfig = config.requireOverlord();
    return serviceConfig.resolveUrl(serviceConfig.tagOrDefault("one"));
  }

  @Override
  public String getOverlordTLSUrl()
  {
    throw new ISE("Not implemented");
  }

  @Override
  public String getOverlordTwoUrl()
  {
    ResolvedDruidService serviceConfig = config.requireOverlord();
    return serviceConfig.resolveUrl(serviceConfig.requireInstance("two"));
  }

  @Override
  public String getOverlordTwoTLSUrl()
  {
    throw new ISE("Not implemented");
  }

  @Override
  public String getIndexerUrl()
  {
    ResolvedDruidService indexer = config.druidService(ResolvedConfig.INDEXER);
    if (indexer == null) {
      indexer = config.requireMiddleManager();
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
    return config.routerUrl();
  }

  @Override
  public String getRouterTLSUrl()
  {
    ResolvedDruidService serviceConfig = config.requireRouter();
    return serviceConfig.resolveUrl(serviceConfig.tagOrDefault("tls"));
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
    ResolvedDruidService serviceConfig = config.requireBroker();
    return serviceConfig.resolveUrl(serviceConfig.instance());
  }

  @Override
  public String getBrokerTLSUrl()
  {
    ResolvedDruidService serviceConfig = config.requireBroker();
    return serviceConfig.resolveUrl(serviceConfig.tagOrDefault("tls"));
  }

  @Override
  public String getHistoricalUrl()
  {
    return config.requireHistorical().resolveUrl();
  }

  @Override
  public String getHistoricalTLSUrl()
  {
    throw new ISE("Not implemented");
  }

  @Override
  public String getProperty(String prop)
  {
    return properties.get(prop);
  }

  @Override
  public String getUsername()
  {
    return getProperty("username");
  }

  @Override
  public String getPassword()
  {
    return getProperty("password");
  }

  @Override
  public Map<String, String> getProperties()
  {
    return properties;
  }

  @Override
  public boolean manageKafkaTopic()
  {
    throw new ISE("Not implemented");
  }

  @Override
  public String getExtraDatasourceNameSuffix()
  {
    return config.datasourceNameSuffix;
  }

  @Override
  public String getCloudBucket()
  {
    return getProperty("cloudBucket");
  }

  @Override
  public String getCloudPath()
  {
    return getProperty("cloudPath");
  }

  @Override
  public String getCloudRegion()
  {
    return getProperty("cloudRegion");
  }

  @Override
  public String getS3AssumeRoleWithExternalId()
  {
    return getProperty("s3AssumeRoleWithExternalId");
  }

  @Override
  public String getS3AssumeRoleExternalId()
  {
    return getProperty("s3AssumeRoleExternalId");
  }

  @Override
  public String getS3AssumeRoleWithoutExternalId()
  {
    return getProperty("s3AssumeRoleWithoutExternalId");
  }

  @Override
  public String getAzureKey()
  {
    return getProperty("azureKey");
  }

  @Override
  public String getHadoopGcsCredentialsPath()
  {
    return getProperty("hadoopGcsCredentialsPath");
  }

  @Override
  public String getStreamEndpoint()
  {
    return getProperty("streamEndpoint");
  }

  @Override
  public String getSchemaRegistryHost()
  {
    return getProperty("schemaRegistryHost");
  }

  @Override
  public boolean isDocker()
  {
    return config.isDocker();
  }

  @Override
  public String getDockerHost()
  {
    return config.proxyHost();
  }
}
