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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
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

  @JsonProperty
  @JsonDeserialize(using = ArbitraryPropertiesJsonDeserializer.class)
  private Map<String, String> properties = new HashMap<>();

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
      public String getCoordinatorTwoUrl()
      {
        return "http://" + dockerIp + ":8581";
      }

      @Override
      public String getCoordinatorTwoTLSUrl()
      {
        return "https://" + dockerIp + ":8781";
      }

      @Override
      public String getOverlordUrl()
      {
        return "http://" + dockerIp + ":8090";
      }

      @Override
      public String getOverlordTLSUrl()
      {
        return "https://" + dockerIp + ":8290";
      }

      @Override
      public String getOverlordTwoUrl()
      {
        return "http://" + dockerIp + ":8590";
      }

      @Override
      public String getOverlordTwoTLSUrl()
      {
        return "https://" + dockerIp + ":8790";
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
      public String getIndexerUrl()
      {
        return "http://" + dockerIp + ":8091";
      }

      @Override
      public String getIndexerTLSUrl()
      {
        return "https://" + dockerIp + ":8291";
      }

      @Override
      public String getMiddleManagerHost()
      {
        return dockerIp;
      }

      @Override
      public String getHistoricalHost()
      {
        return dockerIp;
      }

      @Override
      public String getBrokerHost()
      {
        return dockerIp;
      }

      @Override
      public String getRouterHost()
      {
        return dockerIp;
      }

      @Override
      public String getCoordinatorHost()
      {
        return dockerIp;
      }

      @Override
      public String getCoordinatorTwoHost()
      {
        return dockerIp;
      }

      @Override
      public String getOverlordHost()
      {
        return dockerIp;
      }

      @Override
      public String getOverlordTwoHost()
      {
        return dockerIp;
      }

      @Override
      public String getZookeeperHosts()
      {
        return dockerIp + ":2181";
      }

      @Override
      public String getKafkaHost()
      {
        return dockerIp + ":9093";
      }

      @Override
      public String getZookeeperInternalHosts()
      {
        // docker container name
        return "druid-zookeeper-kafka:2181";
      }

      @Override
      public String getKafkaInternalHost()
      {
        // docker container name
        return "druid-zookeeper-kafka:9092";
      }


      @Override
      public String getBrokerInternalHost()
      {
        return "druid-broker";
      }

      @Override
      public String getRouterInternalHost()
      {
        return "druid-router";
      }

      @Override
      public String getCoordinatorInternalHost()
      {
        return "druid-coordinator";
      }

      @Override
      public String getCoordinatorTwoInternalHost()
      {
        return "druid-coordinator-two";
      }

      @Override
      public String getOverlordInternalHost()
      {
        return "druid-overlord";
      }

      @Override
      public String getOverlordTwoInternalHost()
      {
        return "druid-overlord-two";
      }

      @Override
      public String getHistoricalInternalHost()
      {
        return "druid-historical";
      }

      @Override
      public String getProperty(String prop)
      {
        return properties.get(prop);
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
        return properties;
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

      @Override
      public boolean isDocker()
      {
        return true;
      }

      @Override
      @Nullable
      public String getDockerHost()
      {
        return dockerIp;
      }
    };
  }

  // there is probably a better way to do this...
  static class ArbitraryPropertiesJsonDeserializer extends JsonDeserializer<Map<String, String>>
  {
    @Override
    public Map<String, String> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException
    {
      // given some config input, such as
      //    druid.test.config.properites.a.b.c=d
      // calling jsonParser.readValueAs(Map.class) here results in a map that has both nested objects and also
      // flattened string pairs, so the map looks something like this (in JSON form):
      //    {
      //      "a" : { "b": { "c" : "d" }}},
      //      "a.b.c":"d"
      //    }
      // The string pairs are the values we want to populate this map with, so filtering out the top level keys which
      // do not have string values leaves us with
      //    { "a.b.c":"d"}
      // from the given example, which is what we want
      Map<String, Object> parsed = jsonParser.readValueAs(Map.class);
      Map<String, String> flat = new HashMap<>();
      for (Map.Entry<String, Object> entry : parsed.entrySet()) {
        if (!(entry.getValue() instanceof Map)) {
          flat.put(entry.getKey(), (String) entry.getValue());
        }
      }
      return flat;
    }
  }
}
