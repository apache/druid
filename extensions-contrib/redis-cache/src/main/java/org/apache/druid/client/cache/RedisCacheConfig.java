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

package org.apache.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class RedisCacheConfig
{
  @JsonProperty
  private String host;

  @JsonProperty
  @Min(0)
  @Max(65535)
  private int port;

  @JsonProperty
  private String cluster;

  // milliseconds, default to one day
  @JsonProperty
  private Time expiration = new Time("P1D");

  // milliseconds, the type is 'int' because current Jedis only accept 'int' for timeout
  @JsonProperty
  private int timeout = 2000;

  // max connections of redis connection pool
  @JsonProperty
  private int maxTotalConnections = 8;

  // max idle connections of redis connection pool
  @JsonProperty
  private int maxIdleConnections = 8;

  // min idle connections of redis connection pool
  @JsonProperty
  private int minIdleConnections = 0;

  // cluster
  @JsonProperty
  private int maxRedirection = 5;

  public String getHost()
  {
    return host;
  }

  public int getPort()
  {
    return port;
  }

  public String getCluster() { return cluster; }

  public Time getExpiration()
  {
    return expiration;
  }

  public int getTimeout()
  {
    return timeout;
  }

  public int getMaxTotalConnections()
  {
    return maxTotalConnections;
  }

  public int getMaxIdleConnections()
  {
    return maxIdleConnections;
  }

  public int getMinIdleConnections()
  {
    return minIdleConnections;
  }

  public int getMaxRedirection() { return maxRedirection; }
}
