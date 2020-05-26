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

package org.apache.druid.testing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ConfigFileConfigProvider implements IntegrationTestingConfigProvider
{
  private static final Logger LOG = new Logger(ConfigFileConfigProvider.class);
  private String routerUrl;
  private String brokerUrl;
  private String historicalUrl;
  private String coordinatorUrl;
  private String indexerUrl;
  private String permissiveRouterUrl;
  private String noClientAuthRouterUrl;
  private String customCertCheckRouterUrl;
  private String routerTLSUrl;
  private String brokerTLSUrl;
  private String historicalTLSUrl;
  private String coordinatorTLSUrl;
  private String indexerTLSUrl;
  private String permissiveRouterTLSUrl;
  private String noClientAuthRouterTLSUrl;
  private String customCertCheckRouterTLSUrl;
  private String middleManagerHost;
  private String zookeeperHosts;        // comma-separated list of host:port
  private String kafkaHost;
  private Map<String, String> props = null;
  private String username;
  private String password;
  private String cloudBucket;
  private String cloudPath;
  private String cloudRegion;
  private String hadoopGcsCredentialsPath;
  private String azureKey;
  private String streamEndpoint;

  @JsonCreator
  ConfigFileConfigProvider(@JsonProperty("configFile") String configFile)
  {
    loadProperties(configFile);
  }

  private void loadProperties(String configFile)
  {
    ObjectMapper jsonMapper = new ObjectMapper();
    try {
      props = jsonMapper.readValue(
          new File(configFile), JacksonUtils.TYPE_REFERENCE_MAP_STRING_STRING
      );
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    // there might not be a router; we want routerHost to be null in that case
    routerUrl = props.get("router_url");
    if (routerUrl == null) {
      String routerHost = props.get("router_host");
      if (null != routerHost) {
        routerUrl = StringUtils.format("http://%s:%s", routerHost, props.get("router_port"));
      }
    }
    routerTLSUrl = props.get("router_tls_url");
    if (routerTLSUrl == null) {
      String routerHost = props.get("router_host");
      if (null != routerHost) {
        routerTLSUrl = StringUtils.format("https://%s:%s", routerHost, props.get("router_tls_port"));
      }
    }
    permissiveRouterUrl = props.get("router_permissive_url");
    if (permissiveRouterUrl == null) {
      String permissiveRouterHost = props.get("router_permissive_host");
      if (null != permissiveRouterHost) {
        permissiveRouterUrl = StringUtils.format("http://%s:%s", permissiveRouterHost, props.get("router_permissive_port"));
      }
    }
    permissiveRouterTLSUrl = props.get("router_permissive_tls_url");
    if (permissiveRouterTLSUrl == null) {
      String permissiveRouterHost = props.get("router_permissive_host");
      if (null != permissiveRouterHost) {
        permissiveRouterTLSUrl = StringUtils.format("https://%s:%s", permissiveRouterHost, props.get("router_permissive_tls_port"));
      }
    }
    noClientAuthRouterUrl = props.get("router_no_client_auth_url");
    if (noClientAuthRouterUrl == null) {
      String noClientAuthRouterHost = props.get("router_no_client_auth_host");
      if (null != noClientAuthRouterHost) {
        noClientAuthRouterUrl = StringUtils.format("http://%s:%s", noClientAuthRouterHost, props.get("router_no_client_auth_port"));
      }
    }
    noClientAuthRouterTLSUrl = props.get("router_no_client_auth_tls_url");
    if (noClientAuthRouterTLSUrl == null) {
      String noClientAuthRouterHost = props.get("router_no_client_auth_host");
      if (null != noClientAuthRouterHost) {
        noClientAuthRouterTLSUrl = StringUtils.format("https://%s:%s", noClientAuthRouterHost, props.get("router_no_client_auth_tls_port"));
      }
    }
    customCertCheckRouterUrl = props.get("router_no_client_auth_url");
    if (customCertCheckRouterUrl == null) {
      String customCertCheckRouterHost = props.get("router_no_client_auth_host");
      if (null != customCertCheckRouterHost) {
        customCertCheckRouterUrl = StringUtils.format("http://%s:%s", customCertCheckRouterHost, props.get("router_no_client_auth_port"));
      }
    }
    customCertCheckRouterTLSUrl = props.get("router_no_client_auth_tls_url");
    if (customCertCheckRouterTLSUrl == null) {
      String customCertCheckRouterHost = props.get("router_no_client_auth_host");
      if (null != customCertCheckRouterHost) {
        customCertCheckRouterTLSUrl = StringUtils.format("https://%s:%s", customCertCheckRouterHost, props.get("router_no_client_auth_tls_port"));
      }
    }

    brokerUrl = props.get("broker_url");
    if (brokerUrl == null) {
      brokerUrl = StringUtils.format("http://%s:%s", props.get("broker_host"), props.get("broker_port"));
    }
    brokerTLSUrl = props.get("broker_tls_url");
    if (brokerTLSUrl == null) {
      String brokerHost = props.get("broker_host");
      if (null != brokerHost) {
        brokerTLSUrl = StringUtils.format("https://%s:%s", brokerHost, props.get("broker_tls_port"));
      }
    }
    
    historicalUrl = props.get("historical_url");
    if (historicalUrl == null) {
      historicalUrl = StringUtils.format("http://%s:%s", props.get("historical_host"), props.get("historical_port"));
    }
    historicalTLSUrl = props.get("historical_tls_url");
    if (historicalTLSUrl == null) {
      String historicalHost = props.get("historical_host");
      if (null != historicalHost) {
        historicalTLSUrl = StringUtils.format("https://%s:%s", historicalHost, props.get("historical_tls_port"));
      }
    }

    coordinatorUrl = props.get("coordinator_url");
    if (coordinatorUrl == null) {
      coordinatorUrl = StringUtils.format("http://%s:%s", props.get("coordinator_host"), props.get("coordinator_port"));
    }
    coordinatorTLSUrl = props.get("coordinator_tls_url");
    if (coordinatorTLSUrl == null) {
      String coordinatorHost = props.get("coordinator_host");
      if (null != coordinatorHost) {
        coordinatorTLSUrl = StringUtils.format("https://%s:%s", coordinatorHost, props.get("coordinator_tls_port"));
      }
    }

    indexerUrl = props.get("indexer_url");
    if (indexerUrl == null) {
      indexerUrl = StringUtils.format("http://%s:%s", props.get("indexer_host"), props.get("indexer_port"));
    }
    indexerTLSUrl = props.get("indexer_tls_url");
    if (indexerTLSUrl == null) {
      String indexerHost = props.get("indexer_host");
      if (null != indexerHost) {
        indexerTLSUrl = StringUtils.format("https://%s:%s", indexerHost, props.get("indexer_tls_port"));
      }
    }
    
    middleManagerHost = props.get("middlemanager_host");

    zookeeperHosts = props.get("zookeeper_hosts");
    kafkaHost = props.get("kafka_host") + ":" + props.get("kafka_port");

    username = props.get("username");

    password = props.get("password");

    cloudBucket = props.get("cloud_bucket");
    cloudPath = props.get("cloud_path");
    cloudRegion = props.get("cloud_region");
    hadoopGcsCredentialsPath = props.get("hadoopGcsCredentialsPath");
    azureKey = props.get("azureKey");
    streamEndpoint = props.get("stream_endpoint");

    LOG.info("router: [%s], [%s]", routerUrl, routerTLSUrl);
    LOG.info("broker: [%s], [%s]", brokerUrl, brokerTLSUrl);
    LOG.info("historical: [%s], [%s]", historicalUrl, historicalTLSUrl);
    LOG.info("coordinator: [%s], [%s]", coordinatorUrl, coordinatorTLSUrl);
    LOG.info("overlord: [%s], [%s]", indexerUrl, indexerTLSUrl);
    LOG.info("middle manager: [%s]", middleManagerHost);
    LOG.info("zookeepers: [%s]", zookeeperHosts);
    LOG.info("kafka: [%s]", kafkaHost);
    LOG.info("Username: [%s]", username);
  }

  @Override
  public IntegrationTestingConfig get()
  {
    return new IntegrationTestingConfig()
    {

      @Override
      public String getCoordinatorUrl()
      {
        return coordinatorUrl;
      }

      @Override
      public String getCoordinatorTLSUrl()
      {
        return coordinatorTLSUrl;
      }

      @Override
      public String getIndexerUrl()
      {
        return indexerUrl;
      }

      @Override
      public String getIndexerTLSUrl()
      {
        return indexerTLSUrl;
      }

      @Override
      public String getRouterUrl()
      {
        return routerUrl;
      }

      @Override
      public String getRouterTLSUrl()
      {
        return routerTLSUrl;
      }

      @Override
      public String getPermissiveRouterUrl()
      {
        return permissiveRouterUrl;
      }

      @Override
      public String getPermissiveRouterTLSUrl()
      {
        return permissiveRouterTLSUrl;
      }

      @Override
      public String getNoClientAuthRouterUrl()
      {
        return noClientAuthRouterUrl;
      }

      @Override
      public String getNoClientAuthRouterTLSUrl()
      {
        return noClientAuthRouterTLSUrl;
      }

      @Override
      public String getCustomCertCheckRouterUrl()
      {
        return customCertCheckRouterUrl;
      }

      @Override
      public String getCustomCertCheckRouterTLSUrl()
      {
        return customCertCheckRouterTLSUrl;
      }

      @Override
      public String getBrokerUrl()
      {
        return brokerUrl;
      }

      @Override
      public String getBrokerTLSUrl()
      {
        return brokerTLSUrl;
      }

      @Override
      public String getHistoricalUrl()
      {
        return historicalUrl;
      }

      @Override
      public String getHistoricalTLSUrl()
      {
        return historicalTLSUrl;
      }

      @Override
      public String getMiddleManagerHost()
      {
        return middleManagerHost;
      }

      @Override
      public String getZookeeperHosts()
      {
        return zookeeperHosts;
      }

      @Override
      public String getKafkaHost()
      {
        return kafkaHost;
      }

      @Override
      public String getProperty(String keyword)
      {
        return props.get(keyword);
      }

      @Override
      public String getUsername()
      {
        return username;
      }

      @Override
      public String getPassword()
      {
        return password;
      }

      @Override
      public String getCloudBucket()
      {
        return cloudBucket;
      }

      @Override
      public String getCloudPath()
      {
        return cloudPath;
      }

      @Override
      public String getCloudRegion()
      {
        return cloudRegion;
      }

      @Override
      public String getAzureKey()
      {
        return azureKey;
      }

      @Override
      public String getHadoopGcsCredentialsPath()
      {
        return hadoopGcsCredentialsPath;
      }

      @Override
      public String getStreamEndpoint()
      {
        return streamEndpoint;
      }

      @Override
      public Map<String, String> getProperties()
      {
        return props;
      }

      @Override
      public boolean manageKafkaTopic()
      {
        return Boolean.valueOf(props.getOrDefault("manageKafkaTopic", "true"));
      }

      @Override
      public String getExtraDatasourceNameSuffix()
      {
        return "";
      }
    };
  }
}
