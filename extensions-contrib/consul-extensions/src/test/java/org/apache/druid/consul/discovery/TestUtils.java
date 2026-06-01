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

package org.apache.druid.consul.discovery;

import org.joda.time.Duration;

/**
 * Test utilities for Consul discovery extension tests.
 */
public class TestUtils
{

  /**
   * Creates a minimal ConsulDiscoveryConfig for testing.
   */
  public static ConsulDiscoveryConfig minimalConfig()
  {
    return builder().build();
  }

  /**
   * Builder for ConsulDiscoveryConfig to simplify test configuration.
   */
  public static class Builder
  {
    private String host = "localhost";
    private Integer port = 8500;
    private Duration connectTimeout;
    private Duration socketTimeout;
    private ConsulSSLConfig sslClientConfig;
    private String aclToken;
    private String basicAuthUser;
    private String basicAuthPassword;
    private Boolean allowBasicAuthOverHttp;
    private String servicePrefix = "test-service";
    private String datacenter;
    private java.util.Map<String, String> serviceTags;
    private Duration healthCheckInterval;
    private Duration deregisterAfter;
    private String coordinatorLeaderLockPath = "test/coordinator/leader";
    private String overlordLeaderLockPath = "test/overlord/leader";
    private Duration leaderSessionTtl;
    private Long leaderMaxErrorRetries;
    private Duration leaderRetryBackoffMax;
    private Duration watchSeconds;
    private Long maxWatchRetries;
    private Duration watchRetryDelay;

    public Builder host(String host)
    {
      this.host = host;
      return this;
    }

    public Builder port(int port)
    {
      this.port = port;
      return this;
    }

    public Builder connectTimeout(Duration connectTimeout)
    {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder socketTimeout(Duration socketTimeout)
    {
      this.socketTimeout = socketTimeout;
      return this;
    }

    public Builder sslClientConfig(ConsulSSLConfig sslClientConfig)
    {
      this.sslClientConfig = sslClientConfig;
      return this;
    }

    public Builder aclToken(String aclToken)
    {
      this.aclToken = aclToken;
      return this;
    }

    public Builder basicAuthUser(String basicAuthUser)
    {
      this.basicAuthUser = basicAuthUser;
      return this;
    }

    public Builder basicAuthPassword(String basicAuthPassword)
    {
      this.basicAuthPassword = basicAuthPassword;
      return this;
    }

    public Builder allowBasicAuthOverHttp(Boolean allowBasicAuthOverHttp)
    {
      this.allowBasicAuthOverHttp = allowBasicAuthOverHttp;
      return this;
    }

    public Builder servicePrefix(String servicePrefix)
    {
      this.servicePrefix = servicePrefix;
      return this;
    }

    public Builder datacenter(String datacenter)
    {
      this.datacenter = datacenter;
      return this;
    }

    public Builder serviceTags(java.util.Map<String, String> serviceTags)
    {
      this.serviceTags = serviceTags;
      return this;
    }

    public Builder healthCheckInterval(Duration healthCheckInterval)
    {
      this.healthCheckInterval = healthCheckInterval;
      return this;
    }

    public Builder deregisterAfter(Duration deregisterAfter)
    {
      this.deregisterAfter = deregisterAfter;
      return this;
    }

    public Builder coordinatorLeaderLockPath(String coordinatorLeaderLockPath)
    {
      this.coordinatorLeaderLockPath = coordinatorLeaderLockPath;
      return this;
    }

    public Builder overlordLeaderLockPath(String overlordLeaderLockPath)
    {
      this.overlordLeaderLockPath = overlordLeaderLockPath;
      return this;
    }

    public Builder leaderSessionTtl(Duration leaderSessionTtl)
    {
      this.leaderSessionTtl = leaderSessionTtl;
      return this;
    }

    public Builder leaderMaxErrorRetries(Long leaderMaxErrorRetries)
    {
      this.leaderMaxErrorRetries = leaderMaxErrorRetries;
      return this;
    }

    public Builder leaderRetryBackoffMax(Duration leaderRetryBackoffMax)
    {
      this.leaderRetryBackoffMax = leaderRetryBackoffMax;
      return this;
    }

    public Builder watchSeconds(Duration watchSeconds)
    {
      this.watchSeconds = watchSeconds;
      return this;
    }

    public Builder maxWatchRetries(Long maxWatchRetries)
    {
      this.maxWatchRetries = maxWatchRetries;
      return this;
    }

    public Builder watchRetryDelay(Duration watchRetryDelay)
    {
      this.watchRetryDelay = watchRetryDelay;
      return this;
    }

    public ConsulDiscoveryConfig build()
    {
      return ConsulDiscoveryConfig.create(
          new ConsulDiscoveryConfig.ConnectionConfig(host, port, connectTimeout, socketTimeout, sslClientConfig, null, null),
          new ConsulDiscoveryConfig.AuthConfig(aclToken, basicAuthUser, basicAuthPassword, allowBasicAuthOverHttp),
          new ConsulDiscoveryConfig.ServiceConfig(servicePrefix, datacenter, serviceTags, healthCheckInterval, deregisterAfter),
          new ConsulDiscoveryConfig.LeaderElectionConfig(
              coordinatorLeaderLockPath,
              overlordLeaderLockPath,
              leaderSessionTtl,
              leaderMaxErrorRetries,
              leaderRetryBackoffMax,
              healthCheckInterval
          ),
          new ConsulDiscoveryConfig.WatchConfig(watchSeconds, maxWatchRetries, watchRetryDelay, null)
      );
    }
  }

  /**
   * Creates a Builder for ConsulDiscoveryConfig.
   */
  public static Builder builder()
  {
    return new Builder();
  }
}
