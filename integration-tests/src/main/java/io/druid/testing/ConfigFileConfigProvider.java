/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.testing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ConfigFileConfigProvider implements IntegrationTestingConfigProvider
{
  private final static Logger LOG = new Logger(ConfigFileConfigProvider.class);
  private String routerUrl;
  private String brokerUrl;
  private String historicalUrl;
  private String coordinatorUrl;
  private String indexerUrl;
  private String middleManagerHost;
  private String zookeeperHosts;        // comma-separated list of host:port
  private String kafkaHost;
  private Map<String, String> props = null;
  private String username;
  private String password;

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
        new File(configFile), new TypeReference<Map<String, String>>()
        {
        }
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
    brokerUrl = props.get("broker_url");
    if (brokerUrl == null) {
      brokerUrl = StringUtils.format("http://%s:%s", props.get("broker_host"), props.get("broker_port"));
    }

    historicalUrl = props.get("historical_url");
    if (historicalUrl == null) {
      historicalUrl = StringUtils.format("http://%s:%s", props.get("historical_host"), props.get("historical_port"));
    }

    coordinatorUrl = props.get("coordinator_url");
    if (coordinatorUrl == null) {
      coordinatorUrl = StringUtils.format("http://%s:%s", props.get("coordinator_host"), props.get("coordinator_port"));
    }

    indexerUrl = props.get("indexer_url");
    if (indexerUrl == null) {
      indexerUrl = StringUtils.format("http://%s:%s", props.get("indexer_host"), props.get("indexer_port"));
    }
    middleManagerHost = props.get("middlemanager_host");

    zookeeperHosts = props.get("zookeeper_hosts");
    kafkaHost = props.get("kafka_host") + ":" + props.get("kafka_port");

    username = props.get("username");

    password = props.get("password");

    LOG.info("router: [%s]", routerUrl);
    LOG.info("broker: [%s]", brokerUrl);
    LOG.info("coordinator: [%s]", coordinatorUrl);
    LOG.info("overlord: [%s]", indexerUrl);
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
      public String getIndexerUrl()
      {
        return indexerUrl;
      }

      @Override
      public String getRouterUrl()
      {
        return routerUrl;
      }

      @Override
      public String getBrokerUrl()
      {
        return brokerUrl;
      }

      @Override
      public String getHistoricalUrl()
      {
        return historicalUrl;
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
    };
  }
}
