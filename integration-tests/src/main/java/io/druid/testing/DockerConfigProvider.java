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


import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class DockerConfigProvider implements IntegrationTestingConfigProvider
{

  @JsonProperty
  @NotNull
  private String dockerIp;

  @Override
  public IntegrationTestingConfig get()
  {
    return new IntegrationTestingConfig()
    {
      @Override
      public String getCoordinatorHost()
      {
        return dockerIp+":8081";
      }

      @Override
      public String getIndexerHost()
      {
        return dockerIp+":8090";
      }

      @Override
      public String getRouterHost()
      {
        return dockerIp+ ":8888";
      }

      @Override
      public String getBrokerHost()
      {
        return dockerIp + ":8082";
      }

      @Override
      public String getHistoricalHost()
      {
        return dockerIp + ":8083";
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
      public String getKafkaHost()
      {
        return dockerIp + ":9092";
      }

      @Override
      public String getProperty(String prop)
      {
        throw new UnsupportedOperationException("DockerConfigProvider does not support getProperty()");
      }
    };
  }
}
