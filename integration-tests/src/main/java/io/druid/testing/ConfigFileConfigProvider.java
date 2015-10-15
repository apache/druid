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

import java.io.FileNotFoundException;
import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import com.metamx.common.logger.Logger;

public class ConfigFileConfigProvider implements IntegrationTestingConfigProvider
{
  private final static Logger LOG = new Logger(ConfigFileConfigProvider.class);
  private String routerHost = "";
  private String brokerHost = "";
  private String historicalHost = "";
  private String coordinatorHost = "";
  private String indexerHost = "";
  private String middleManagerHost = "";
  private String zookeeperHosts = "";        // comma-separated list of host:port
  private Map<String, String> props = null;

  @JsonCreator
  ConfigFileConfigProvider(@JsonProperty("configFile") String configFile){
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
    routerHost = props.get("router_host") + ":" + props.get("router_port");
    brokerHost = props.get("broker_host") + ":" + props.get("broker_port");
    historicalHost = props.get("historical_host") + ":" + props.get("historical_port");
    coordinatorHost = props.get("coordinator_host") + ":" + props.get("coordinator_port");
    indexerHost = props.get("indexer_host") + ":" + props.get("indexer_port");
    middleManagerHost = props.get("middlemanager_host");
    zookeeperHosts = props.get("zookeeper_hosts");

    LOG.info ("router: [%s]", routerHost);
    LOG.info ("broker [%s]: ", brokerHost);
    LOG.info ("coordinator: [%s]", coordinatorHost);
    LOG.info ("overlord: [%s]", indexerHost);
    LOG.info ("middle manager: [%s]", middleManagerHost);
    LOG.info ("zookeepers: [%s]", zookeeperHosts);
  }

  @Override
  public IntegrationTestingConfig get()
  {
    return new IntegrationTestingConfig()
    {
      @Override
      public String getCoordinatorHost()
      {
        return coordinatorHost;
      }

      @Override
      public String getIndexerHost()
      {
        return indexerHost;
      }

      @Override
      public String getRouterHost()
      {
        return routerHost;
      }

      @Override
      public String getBrokerHost()
      {
        return brokerHost;
      }

      @Override
      public String getHistoricalHost()
      {
        return historicalHost;
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
      public String getProperty(String keyword)
      {
        return props.get(keyword);
      }
    };
  }
}
