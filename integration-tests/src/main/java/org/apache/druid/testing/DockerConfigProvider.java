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


import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

public class DockerConfigProvider implements IntegrationTestingConfigProvider
{

  @JsonProperty
  @NotNull
  private String dockerIp;

  @JsonProperty
  private String extraDatasourceNameSuffix = "";

  @JsonProperty
  private String cloudPath;

  @JsonProperty
  private String cloudBucket;

  @JsonProperty
  private String cloudRegion;

  @JsonProperty
  private String hadoopGcsCredentialsPath;

  @JsonProperty
  private String azureKey;

  @JsonProperty
  private String streamEndpoint;

  @Override
  public IntegrationTestingConfig get()
  {
    return new IntegrationTestingConfig()
    {
      @Override
      public String getCoordinatorUrl()
      {
        return "http://" + dockerIp + ":8081";
      }

      @Override
      public String getCoordinatorTLSUrl()
      {
        return "https://" + dockerIp + ":8281";
      }

      @Override
      public String getIndexerUrl()
      {
        return "http://" + dockerIp + ":8090";
      }

      @Override
      public String getIndexerTLSUrl()
      {
        return "https://" + dockerIp + ":8290";
      }

      @Override
      public String getRouterUrl()
      {
        return "http://" + dockerIp + ":8888";
      }

      @Override
      public String getRouterTLSUrl()
      {
        return "https://" + dockerIp + ":9088";
      }

      @Override
      public String getPermissiveRouterUrl()
      {
        return "http://" + dockerIp + ":8889";
      }

      @Override
      public String getPermissiveRouterTLSUrl()
      {
        return "https://" + dockerIp + ":9089";
      }

      @Override
      public String getNoClientAuthRouterUrl()
      {
        return "http://" + dockerIp + ":8890";
      }

      @Override
      public String getNoClientAuthRouterTLSUrl()
      {
        return "https://" + dockerIp + ":9090";
      }

      @Override
      public String getCustomCertCheckRouterUrl()
      {
        return "http://" + dockerIp + ":8891";
      }

      @Override
      public String getCustomCertCheckRouterTLSUrl()
      {
        return "https://" + dockerIp + ":9091";
      }

      @Override
      public String getBrokerUrl()
      {
        return "http://" + dockerIp + ":8082";
      }

      @Override
      public String getBrokerTLSUrl()
      {
        return "https://" + dockerIp + ":8282";
      }

      @Override
      public String getHistoricalUrl()
      {
        return "http://" + dockerIp + ":8083";
      }

      @Override
      public String getHistoricalTLSUrl()
      {
        return "https://" + dockerIp + ":8283";
      }

      @Override
      public String getMiddleManagerHost()
      {
        return dockerIp;
      }

      @Override
      public String getZookeeperHosts()
      {
        return dockerIp + ":2181";
      }

      @Override
      public String getZookeeperInternalHosts()
      {
        // docker container name
        return "druid-zookeeper-kafka:2181";
      }

      @Override
      public String getKafkaHost()
      {
        return dockerIp + ":9093";
      }

      @Override
      public String getKafkaInternalHost()
      {
        // docker container name
        return "druid-zookeeper-kafka:9092";
      }

      @Override
      public String getProperty(String prop)
      {
        throw new UnsupportedOperationException("DockerConfigProvider does not support property " + prop);
      }

      @Override
      public String getUsername()
      {
        return null;
      }

      @Override
      public String getPassword()
      {
        return null;
      }

      @Override
      public Map<String, String> getProperties()
      {
        return new HashMap<>();
      }

      @Override
      public boolean manageKafkaTopic()
      {
        return true;
      }

      @Override
      public String getExtraDatasourceNameSuffix()
      {
        return extraDatasourceNameSuffix;
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
    };
  }
}
