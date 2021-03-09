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
import org.apache.druid.com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.PasswordProvider;
import org.joda.time.Period;
import redis.clients.jedis.Protocol;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class RedisCacheConfig
{
  public static class RedisClusterConfig
  {
    @JsonProperty
    private String nodes;

    // cluster
    @JsonProperty
    private int maxRedirection = 5;

    public String getNodes()
    {
      return nodes;
    }

    public int getMaxRedirection()
    {
      return maxRedirection;
    }
  }

  /**
   * Support for long-format and Period style format
   */
  public static class DurationConfig
  {
    private long milliseconds;

    public DurationConfig(String time)
    {
      try {
        // before 0.19.0, only long-format is support,
        // try to parse it as long
        this.milliseconds = Long.parseLong(time);
      }
      catch (NumberFormatException e) {
        // try to parse it as a Period string
        this.milliseconds = Period.parse(time).toStandardDuration().getMillis();
      }
    }

    /**
     * kept for test cases only
     */
    @VisibleForTesting
    DurationConfig(long milliseconds)
    {
      this.milliseconds = milliseconds;
    }

    public long getMilliseconds()
    {
      return milliseconds;
    }

    public int getMillisecondsAsInt()
    {
      if (milliseconds > Integer.MAX_VALUE) {
        throw new ISE("Milliseconds %d is out of range of int", milliseconds);
      }
      return (int) milliseconds;
    }

    public long getSeconds()
    {
      return milliseconds / 1000;
    }
  }

  /**
   * host of a standalone mode redis
   */
  @JsonProperty
  private String host;

  /**
   * port of a standalone mode redis
   */
  @JsonProperty
  @Min(0)
  @Max(65535)
  private int port;

  @JsonProperty
  private DurationConfig expiration = new DurationConfig("P1D");

  @JsonProperty
  private DurationConfig timeout = new DurationConfig("PT2S");

  /**
   * max connections of redis connection pool
   */
  @JsonProperty
  private int maxTotalConnections = 8;

  /**
   * max idle connections of redis connection pool
   */
  @JsonProperty
  private int maxIdleConnections = 8;

  /**
   * min idle connections of redis connection pool
   */
  @JsonProperty
  private int minIdleConnections = 0;

  @JsonProperty
  private PasswordProvider password;

  @JsonProperty
  @Min(0)
  private int database = Protocol.DEFAULT_DATABASE;

  @JsonProperty
  private RedisClusterConfig cluster;

  public String getHost()
  {
    return host;
  }

  public int getPort()
  {
    return port;
  }

  public DurationConfig getExpiration()
  {
    return expiration;
  }

  public DurationConfig getTimeout()
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

  public RedisClusterConfig getCluster()
  {
    return cluster;
  }

  public PasswordProvider getPassword()
  {
    return password;
  }

  public int getDatabase()
  {
    return database;
  }
}
