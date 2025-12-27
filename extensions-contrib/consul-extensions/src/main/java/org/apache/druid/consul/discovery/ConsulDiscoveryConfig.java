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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for Consul-based service discovery.
 */
public class ConsulDiscoveryConfig
{
  private static final Logger LOGGER = new Logger(ConsulDiscoveryConfig.class);
  private static final long MIN_LEADER_SESSION_TTL_SECONDS = 10;

  @JsonProperty("connection")
  private final ConnectionConfig connection;

  @JsonProperty("auth")
  private final AuthConfig auth;

  @JsonProperty("service")
  private final ServiceConfig service;

  @JsonProperty("leader")
  private final LeaderElectionConfig leader;

  @JsonProperty("watch")
  private final WatchConfig watch;

  @JsonCreator
  public static ConsulDiscoveryConfig create(
      @JsonProperty("connection") @Nullable ConnectionConfig connection,
      @JsonProperty("auth") @Nullable AuthConfig auth,
      @JsonProperty("service") ServiceConfig service,
      @JsonProperty("leader") @Nullable LeaderElectionConfig leader,
      @JsonProperty("watch") @Nullable WatchConfig watch
  )
  {
    if (service == null) {
      throw new IAE("service cannot be null");
    }

    LeaderElectionConfig finalLeader = computeLeaderElectionConfig(leader, service.getHealthCheckInterval());
    return new ConsulDiscoveryConfig(connection, auth, service, finalLeader, watch);
  }

  private static LeaderElectionConfig computeLeaderElectionConfig(
      @Nullable LeaderElectionConfig leader,
      Duration healthCheckInterval
  )
  {
    if (leader != null) {
      // Compute default TTL based on health check interval when not explicitly set
      if (leader.getLeaderSessionTtl() == null) {
        return new LeaderElectionConfig(
            leader.getCoordinatorLeaderLockPath(),
            leader.getOverlordLeaderLockPath(),
            null,
            leader.getLeaderMaxErrorRetries(),
            leader.getLeaderRetryBackoffMax(),
            healthCheckInterval
        );
      } else {
        return leader;
      }
    } else {
      return new LeaderElectionConfig(null, null, null, null, null, healthCheckInterval);
    }
  }

  private ConsulDiscoveryConfig(
      ConnectionConfig connection,
      AuthConfig auth,
      ServiceConfig service,
      LeaderElectionConfig leader,
      WatchConfig watch
  )
  {
    this.connection = connection == null ? new ConnectionConfig(null, null, null, null, null, null, null) : connection;
    this.auth = auth == null ? new AuthConfig(null, null, null, null) : auth;
    this.service = service;
    this.leader = leader;
    this.watch = watch == null ? new WatchConfig(null, null, null, null) : watch;

    validateCrossFieldConstraints();
  }

  private void validateCrossFieldConstraints()
  {
    // Socket timeout must exceed watch timeout to avoid premature disconnects
    if (connection.getSocketTimeout().compareTo(watch.getWatchSeconds()) <= 0) {
      throw new IAE(
          StringUtils.format(
              "socketTimeout [%s] must be greater than watchSeconds [%s]",
              connection.getSocketTimeout(),
              watch.getWatchSeconds()
          )
      );
    }

    long serviceTtlSeconds = Math.max(30, service.getHealthCheckInterval().getStandardSeconds() * 3);
    if (service.getDeregisterAfter().getStandardSeconds() < serviceTtlSeconds) {
      throw new IAE(
          StringUtils.format(
              "deregisterAfter (%ds) must be >= service TTL (%ds = 3 × healthCheckInterval)",
              service.getDeregisterAfter().getStandardSeconds(),
              serviceTtlSeconds
          )
      );
    }

    // Large watchSeconds relative to session TTL can delay failure detection
    if (watch.getWatchSeconds().getStandardSeconds() > leader.getLeaderSessionTtl().getStandardSeconds() * 2) {
      LOGGER.warn(
          "watchSeconds (%ds) is much larger than leaderSessionTtl (%ds): delayed failure detection possible",
          watch.getWatchSeconds().getStandardSeconds(),
          leader.getLeaderSessionTtl().getStandardSeconds()
      );
    }
  }

  public ConnectionConfig getConnection()
  {
    return connection;
  }

  public AuthConfig getAuth()
  {
    return auth;
  }

  public ServiceConfig getService()
  {
    return service;
  }

  public LeaderElectionConfig getLeader()
  {
    return leader;
  }

  public WatchConfig getWatch()
  {
    return watch;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConsulDiscoveryConfig that = (ConsulDiscoveryConfig) o;
    return Objects.equals(connection, that.connection) &&
           Objects.equals(auth, that.auth) &&
           Objects.equals(service, that.service) &&
           Objects.equals(leader, that.leader) &&
           Objects.equals(watch, that.watch);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(connection, auth, service, leader, watch);
  }

  @Override
  public String toString()
  {
    return "ConsulDiscoveryConfig{" +
           "connection=" + connection +
           ", auth=" + auth +
           ", service=" + service +
           ", leader=" + leader +
           ", watch=" + watch +
           '}';
  }

  public static class ConnectionConfig
  {
    private static final long DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
    private static final long DEFAULT_SOCKET_TIMEOUT_MS = 75_000;
    private static final int DEFAULT_MAX_TOTAL_CONNECTIONS = 50;
    private static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 20;

    private final String host;
    private final int port;
    private final Duration connectTimeout;
    private final Duration socketTimeout;
    @Nullable
    private final ConsulSSLConfig sslClientConfig;
    private final int maxTotalConnections;
    private final int maxConnectionsPerRoute;

    @JsonCreator
    public ConnectionConfig(
        @JsonProperty("host") @Nullable String host,
        @JsonProperty("port") @Nullable Integer port,
        @JsonProperty("connectTimeout") @Nullable Duration connectTimeout,
        @JsonProperty("socketTimeout") @Nullable Duration socketTimeout,
        @JsonProperty("sslClientConfig") @Nullable ConsulSSLConfig sslClientConfig,
        @JsonProperty("maxTotalConnections") @Nullable Integer maxTotalConnections,
        @JsonProperty("maxConnectionsPerRoute") @Nullable Integer maxConnectionsPerRoute
    )
    {
      this.host = host == null ? "localhost" : host;
      this.port = validatePort(port);
      this.connectTimeout = validatePositive(connectTimeout, DEFAULT_CONNECT_TIMEOUT_MS, "connectTimeout");
      this.socketTimeout = validatePositive(socketTimeout, DEFAULT_SOCKET_TIMEOUT_MS, "socketTimeout");
      this.sslClientConfig = sslClientConfig;
      this.maxTotalConnections = validateConnectionPoolSize(
          maxTotalConnections,
          DEFAULT_MAX_TOTAL_CONNECTIONS,
          "maxTotalConnections"
      );
      this.maxConnectionsPerRoute = validateConnectionPoolSize(
          maxConnectionsPerRoute,
          DEFAULT_MAX_CONNECTIONS_PER_ROUTE,
          "maxConnectionsPerRoute"
      );
    }

    private static int validatePort(Integer port)
    {
      int portValue = port == null ? 8500 : port;
      if (portValue < 1 || portValue > 65535) {
        throw new IllegalArgumentException("port must be between 1 and 65535");
      }
      return portValue;
    }

    private static int validateConnectionPoolSize(Integer value, int defaultValue, String name)
    {
      int result = value == null ? defaultValue : value;
      if (result <= 0) {
        throw new IAE(name + " must be positive");
      }
      return result;
    }

    @JsonProperty
    public String getHost()
    {
      return host;
    }

    @JsonProperty
    public int getPort()
    {
      return port;
    }

    @JsonProperty
    public Duration getConnectTimeout()
    {
      return connectTimeout;
    }

    @JsonProperty
    public Duration getSocketTimeout()
    {
      return socketTimeout;
    }

    @JsonProperty
    @Nullable
    public ConsulSSLConfig getSslClientConfig()
    {
      return sslClientConfig;
    }

    @JsonProperty
    public int getMaxTotalConnections()
    {
      return maxTotalConnections;
    }

    @JsonProperty
    public int getMaxConnectionsPerRoute()
    {
      return maxConnectionsPerRoute;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ConnectionConfig that = (ConnectionConfig) o;
      return port == that.getPort() &&
             maxTotalConnections == that.getMaxTotalConnections() &&
             maxConnectionsPerRoute == that.getMaxConnectionsPerRoute() &&
             Objects.equals(host, that.getHost()) &&
             Objects.equals(connectTimeout, that.getConnectTimeout()) &&
             Objects.equals(socketTimeout, that.getSocketTimeout()) &&
             Objects.equals(sslClientConfig, that.getSslClientConfig());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(host, port, connectTimeout, socketTimeout, sslClientConfig, maxTotalConnections, maxConnectionsPerRoute);
    }

    @Override
    public String toString()
    {
      return "ConnectionConfig{host='" + host + "', port=" + port +
             ", connectTimeout=" + connectTimeout + ", socketTimeout=" + socketTimeout +
             ", maxTotalConnections=" + maxTotalConnections + ", maxConnectionsPerRoute=" + maxConnectionsPerRoute + '}';
    }
  }

  public static class AuthConfig
  {
    @Nullable
    private final String aclToken;
    @Nullable
    private final String basicAuthUser;
    @Nullable
    private final String basicAuthPassword;
    private final boolean allowBasicAuthOverHttp;

    @JsonCreator
    public AuthConfig(
        @JsonProperty("aclToken") @Nullable String aclToken,
        @JsonProperty("basicAuthUser") @Nullable String basicAuthUser,
        @JsonProperty("basicAuthPassword") @Nullable String basicAuthPassword,
        @JsonProperty("allowBasicAuthOverHttp") @Nullable Boolean allowBasicAuthOverHttp
    )
    {
      this.aclToken = aclToken;
      this.basicAuthUser = basicAuthUser;
      this.basicAuthPassword = basicAuthPassword;
      this.allowBasicAuthOverHttp = allowBasicAuthOverHttp != null ? allowBasicAuthOverHttp : false;
    }

    @JsonProperty
    @Nullable
    public String getAclToken()
    {
      return aclToken;
    }

    @JsonProperty
    @Nullable
    public String getBasicAuthUser()
    {
      return basicAuthUser;
    }

    @JsonProperty
    @Nullable
    public String getBasicAuthPassword()
    {
      return basicAuthPassword;
    }

    @JsonProperty
    public boolean getAllowBasicAuthOverHttp()
    {
      return allowBasicAuthOverHttp;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AuthConfig that = (AuthConfig) o;
      return allowBasicAuthOverHttp == that.getAllowBasicAuthOverHttp() &&
             Objects.equals(aclToken, that.getAclToken()) &&
             Objects.equals(basicAuthUser, that.getBasicAuthUser()) &&
             Objects.equals(basicAuthPassword, that.getBasicAuthPassword());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(aclToken, basicAuthUser, basicAuthPassword, allowBasicAuthOverHttp);
    }

    @Override
    public String toString()
    {
      return "AuthConfig{aclToken=" + mask(aclToken) +
             ", basicAuthUser=" + mask(basicAuthUser) +
             ", basicAuthPassword=" + mask(basicAuthPassword) +
             ", allowBasicAuthOverHttp=" + allowBasicAuthOverHttp + '}';
    }

    private static String mask(String value)
    {
      if (value == null) {
        return String.valueOf(value);
      }
      return "*****";
    }
  }

  public static class ServiceConfig
  {
    private static final long DEFAULT_HEALTH_CHECK_INTERVAL_MS = 10_000;
    private static final long DEFAULT_DEREGISTER_AFTER_MS = 90_000;

    private final String servicePrefix;
    @Nullable
    private final String datacenter;
    @Nullable
    private final Map<String, String> serviceTags;
    private final Duration healthCheckInterval;
    private final Duration deregisterAfter;

    @JsonCreator
    public ServiceConfig(
        @JsonProperty("servicePrefix") String servicePrefix,
        @JsonProperty("datacenter") @Nullable String datacenter,
        @JsonProperty("serviceTags") @Nullable Map<String, String> serviceTags,
        @JsonProperty("healthCheckInterval") @Nullable Duration healthCheckInterval,
        @JsonProperty("deregisterAfter") @Nullable Duration deregisterAfter
    )
    {
      if (servicePrefix == null || servicePrefix.isEmpty()) {
        throw new IAE("servicePrefix cannot be null or empty");
      }
      this.servicePrefix = servicePrefix;
      this.datacenter = datacenter;
      this.serviceTags = serviceTags == null
                         ? null
                         : Collections.unmodifiableMap(new LinkedHashMap<>(serviceTags));
      this.healthCheckInterval = validatePositive(healthCheckInterval, DEFAULT_HEALTH_CHECK_INTERVAL_MS, "healthCheckInterval");
      this.deregisterAfter = validateNonNegative(deregisterAfter, DEFAULT_DEREGISTER_AFTER_MS, "deregisterAfter");
    }

    @JsonProperty
    public String getServicePrefix()
    {
      return servicePrefix;
    }

    @JsonProperty
    @Nullable
    public String getDatacenter()
    {
      return datacenter;
    }

    @JsonProperty
    @Nullable
    public Map<String, String> getServiceTags()
    {
      return serviceTags == null ? null : Collections.unmodifiableMap(serviceTags);
    }

    @JsonProperty
    public Duration getHealthCheckInterval()
    {
      return healthCheckInterval;
    }

    @JsonProperty
    public Duration getDeregisterAfter()
    {
      return deregisterAfter;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ServiceConfig that = (ServiceConfig) o;
      return Objects.equals(servicePrefix, that.getServicePrefix()) &&
             Objects.equals(datacenter, that.getDatacenter()) &&
             Objects.equals(serviceTags, that.getServiceTags()) &&
             Objects.equals(healthCheckInterval, that.getHealthCheckInterval()) &&
             Objects.equals(deregisterAfter, that.getDeregisterAfter());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(servicePrefix, datacenter, serviceTags, healthCheckInterval, deregisterAfter);
    }

    @Override
    public String toString()
    {
      return "ServiceConfig{servicePrefix='" + servicePrefix + "', datacenter='" + datacenter +
             "', healthCheckInterval=" + healthCheckInterval + ", deregisterAfter=" + deregisterAfter + '}';
    }
  }

  public static class LeaderElectionConfig
  {
    private static final long DEFAULT_LEADER_RETRY_BACKOFF_MAX_MS = 300_000;
    private static final long DEFAULT_LEADER_MAX_ERROR_RETRIES = 20;

    private final String coordinatorLeaderLockPath;
    private final String overlordLeaderLockPath;
    private final Duration leaderSessionTtl;
    private final long leaderMaxErrorRetries;
    private final Duration leaderRetryBackoffMax;

    @JsonCreator
    public LeaderElectionConfig(
        @JsonProperty("coordinatorLeaderLockPath") @Nullable String coordinatorLeaderLockPath,
        @JsonProperty("overlordLeaderLockPath") @Nullable String overlordLeaderLockPath,
        @JsonProperty("leaderSessionTtl") @Nullable Duration leaderSessionTtl,
        @JsonProperty("leaderMaxErrorRetries") @Nullable Long leaderMaxErrorRetries,
        @JsonProperty("leaderRetryBackoffMax") @Nullable Duration leaderRetryBackoffMax,
        @JsonProperty("healthCheckInterval") @Nullable Duration healthCheckInterval
    )
    {
      this.coordinatorLeaderLockPath = coordinatorLeaderLockPath != null
          ? coordinatorLeaderLockPath
          : "druid/leader/coordinator";
      this.overlordLeaderLockPath = overlordLeaderLockPath != null
          ? overlordLeaderLockPath
          : "druid/leader/overlord";
      this.leaderSessionTtl = computeLeaderSessionTtl(leaderSessionTtl, healthCheckInterval);
      this.leaderMaxErrorRetries = (leaderMaxErrorRetries == null || leaderMaxErrorRetries <= 0)
          ? DEFAULT_LEADER_MAX_ERROR_RETRIES
          : leaderMaxErrorRetries;
      this.leaderRetryBackoffMax = validatePositive(
          leaderRetryBackoffMax,
          DEFAULT_LEADER_RETRY_BACKOFF_MAX_MS,
          "leaderRetryBackoffMax"
      );
    }

    private static Duration computeLeaderSessionTtl(Duration leaderSessionTtl, Duration healthCheckInterval)
    {
      Duration ttl = leaderSessionTtl;
      if (ttl == null) {
        long defaultTtlSeconds = 45; // Default TTL when healthCheckInterval is null
        if (healthCheckInterval != null) {
          defaultTtlSeconds = Math.max(45, healthCheckInterval.getStandardSeconds() * 3);
        }
        ttl = Duration.standardSeconds(defaultTtlSeconds);
      }
      if (ttl.getStandardSeconds() < MIN_LEADER_SESSION_TTL_SECONDS) {
        throw new IAE(
            StringUtils.format(
                "leaderSessionTtl [%s] must be at least %d seconds",
                ttl,
                MIN_LEADER_SESSION_TTL_SECONDS
            )
        );
      }
      return ttl;
    }

    @JsonProperty
    public String getCoordinatorLeaderLockPath()
    {
      return coordinatorLeaderLockPath;
    }

    @JsonProperty
    public String getOverlordLeaderLockPath()
    {
      return overlordLeaderLockPath;
    }

    @JsonProperty
    public Duration getLeaderSessionTtl()
    {
      return leaderSessionTtl;
    }

    @JsonProperty
    public long getLeaderMaxErrorRetries()
    {
      return leaderMaxErrorRetries;
    }

    @JsonProperty
    public Duration getLeaderRetryBackoffMax()
    {
      return leaderRetryBackoffMax;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LeaderElectionConfig that = (LeaderElectionConfig) o;
      return leaderMaxErrorRetries == that.getLeaderMaxErrorRetries() &&
             Objects.equals(coordinatorLeaderLockPath, that.getCoordinatorLeaderLockPath()) &&
             Objects.equals(overlordLeaderLockPath, that.getOverlordLeaderLockPath()) &&
             Objects.equals(leaderSessionTtl, that.getLeaderSessionTtl()) &&
             Objects.equals(leaderRetryBackoffMax, that.getLeaderRetryBackoffMax());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(
          coordinatorLeaderLockPath,
          overlordLeaderLockPath,
          leaderSessionTtl,
          leaderMaxErrorRetries,
          leaderRetryBackoffMax
      );
    }

    @Override
    public String toString()
    {
      return "LeaderElectionConfig{coordinatorPath='" + coordinatorLeaderLockPath +
             "', overlordPath='" + overlordLeaderLockPath +
             "', sessionTtl=" + leaderSessionTtl +
             ", maxRetries=" + (leaderMaxErrorRetries == Long.MAX_VALUE ? "unlimited" : leaderMaxErrorRetries) + '}';
    }
  }

  public static class WatchConfig
  {
    private static final long DEFAULT_WATCH_TIMEOUT_MS = 60_000;
    private static final long DEFAULT_WATCH_RETRY_DELAY_MS = 10_000;
    private static final long DEFAULT_CIRCUIT_BREAKER_SLEEP_MS = 120_000; // 2 minutes

    private final Duration watchSeconds;
    private final long maxWatchRetries;
    private final Duration watchRetryDelay;
    private final Duration circuitBreakerSleep;

    @JsonCreator
    public WatchConfig(
        @JsonProperty("watchSeconds") @Nullable Duration watchSeconds,
        @JsonProperty("maxWatchRetries") @Nullable Long maxWatchRetries,
        @JsonProperty("watchRetryDelay") @Nullable Duration watchRetryDelay,
        @JsonProperty("circuitBreakerSleep") @Nullable Duration circuitBreakerSleep
    )
    {
      this.watchSeconds = validatePositive(watchSeconds, DEFAULT_WATCH_TIMEOUT_MS, "watchSeconds");
      this.maxWatchRetries = (maxWatchRetries == null || maxWatchRetries <= 0) ? Long.MAX_VALUE : maxWatchRetries;
      this.watchRetryDelay = validateNonNegative(watchRetryDelay, DEFAULT_WATCH_RETRY_DELAY_MS, "watchRetryDelay");
      this.circuitBreakerSleep = validatePositive(circuitBreakerSleep, DEFAULT_CIRCUIT_BREAKER_SLEEP_MS, "circuitBreakerSleep");
    }

    @JsonProperty
    public Duration getWatchSeconds()
    {
      return watchSeconds;
    }

    @JsonProperty
    public long getMaxWatchRetries()
    {
      return maxWatchRetries;
    }

    @JsonProperty
    public Duration getWatchRetryDelay()
    {
      return watchRetryDelay;
    }

    @JsonProperty
    public Duration getCircuitBreakerSleep()
    {
      return circuitBreakerSleep;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WatchConfig that = (WatchConfig) o;
      return maxWatchRetries == that.getMaxWatchRetries() &&
             Objects.equals(watchSeconds, that.getWatchSeconds()) &&
             Objects.equals(watchRetryDelay, that.getWatchRetryDelay()) &&
             Objects.equals(circuitBreakerSleep, that.getCircuitBreakerSleep());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(watchSeconds, maxWatchRetries, watchRetryDelay, circuitBreakerSleep);
    }

    @Override
    public String toString()
    {
      return "WatchConfig{watchSeconds=" + watchSeconds +
             ", maxRetries=" + (maxWatchRetries == Long.MAX_VALUE ? "unlimited" : maxWatchRetries) +
             ", retryDelay=" + watchRetryDelay +
             ", circuitBreakerSleep=" + circuitBreakerSleep + '}';
    }
  }

  private static Duration validatePositive(Duration value, long defaultMs, String name)
  {
    Duration result = value;
    if (result == null) {
      result = Duration.millis(defaultMs);
    }
    if (result.getMillis() <= 0) {
      throw new IAE(name + " must be positive");
    }
    return result;
  }

  private static Duration validateNonNegative(Duration value, long defaultMs, String name)
  {
    Duration result = value;
    if (result == null) {
      result = Duration.millis(defaultMs);
    }
    if (result.getMillis() < 0) {
      throw new IAE(name + " cannot be negative");
    }
    return result;
  }
}
