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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Announces Druid nodes to Consul and maintains their health status via TTL checks.
 */
@ManageLifecycle
public class ConsulDruidNodeAnnouncer implements DruidNodeAnnouncer
{
  private static final Logger LOGGER = new Logger(ConsulDruidNodeAnnouncer.class);

  private final ConsulApiClient consulApiClient;
  private final ConsulDiscoveryConfig config;
  private final ConcurrentMap<String, DiscoveryDruidNode> announcedNodes = new ConcurrentHashMap<>();
  private final Set<String> registeringNodes = ConcurrentHashMap.newKeySet();
  private final ScheduledExecutorService healthCheckExecutor;
  
  private static final int MAX_FAILURES_BEFORE_REREGISTER = 3;
  private static final long EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 10;
  private final ConcurrentMap<String, AtomicInteger> consecutiveFailures = new ConcurrentHashMap<>();

  @Inject(optional = true)
  @Nullable
  private ServiceEmitter emitter;
  private final LifecycleLock lifecycleLock = new LifecycleLock();

  @Inject
  public ConsulDruidNodeAnnouncer(
      ConsulApiClient consulApiClient,
      ConsulDiscoveryConfig config
  )
  {
    this.consulApiClient = Preconditions.checkNotNull(consulApiClient, "consulApiClient");
    this.config = Preconditions.checkNotNull(config, "config");
    this.healthCheckExecutor = Execs.scheduledSingleThreaded("ConsulHealthCheck-%d");
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start");
    }

    try {
      LOGGER.info("Starting ConsulDruidNodeAnnouncer");

      long intervalMs = config.getService().getHealthCheckInterval().getMillis();
      healthCheckExecutor.scheduleAtFixedRate(
          this::updateHealthChecks,
          0L,
          intervalMs,
          TimeUnit.MILLISECONDS
      );
      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop");
    }

    LOGGER.info("Stopping ConsulDruidNodeAnnouncer");

    healthCheckExecutor.shutdownNow();

    // Wait for health check to finish so we don't deregister while health check is in progress
    try {
      if (!healthCheckExecutor.awaitTermination(EXECUTOR_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        LOGGER.warn("Health check executor did not terminate in time");
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted while waiting for health check termination");
    }

    for (String serviceId : announcedNodes.keySet()) {
      try {
        consulApiClient.deregisterService(serviceId);
      }
      catch (Exception e) {
        LOGGER.error(e, "Failed to deregister service [%s] during shutdown", serviceId);
      }
    }

    announcedNodes.clear();
    lifecycleLock.exitStop();
  }

  @Override
  public void announce(DiscoveryDruidNode discoveryDruidNode)
  {
    if (!lifecycleLock.awaitStarted(1, TimeUnit.SECONDS)) {
      throw new ISE("Announcer not started");
    }

    String serviceId = ConsulServiceIds.serviceId(config, discoveryDruidNode);

    // Prevent concurrent duplicate registrations for the same serviceId
    if (!registeringNodes.add(serviceId)) {
      LOGGER.warn("Registration already in progress for serviceId [%s]", serviceId);
      return;
    }

    try {
      // If already announced, skip duplicate registration
      if (announcedNodes.containsKey(serviceId)) {
        LOGGER.warn("ServiceId [%s] already announced, skipping", serviceId);
        return;
      }

      long registerStart = System.nanoTime();

      // Register in Consul, then track locally atomically in this block
      consulApiClient.registerService(discoveryDruidNode);
      announcedNodes.put(serviceId, discoveryDruidNode);

      long registerLatency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - registerStart);
      ConsulMetrics.emitTimer(emitter, "consul/register/latency", registerLatency,
          "role", discoveryDruidNode.getNodeRole().getJsonName());

      LOGGER.info("Successfully announced serviceId [%s]", serviceId);
      ConsulMetrics.emitCount(
          emitter,
          "consul/announce/success",
          "role",
          discoveryDruidNode.getNodeRole().getJsonName()
      );
    }
    catch (Exception e) {
      // Cleanup partial registration if Consul was updated before failure
      try {
        consulApiClient.deregisterService(serviceId);
      }
      catch (Exception cleanup) {
        LOGGER.debug(cleanup, "Cleanup deregister failed for serviceId [%s] after announce error", serviceId);
      }

      LOGGER.error(e, "Exception during announce for DiscoveryDruidNode[%s]", discoveryDruidNode);
      ConsulMetrics.emitCount(
          emitter,
          "consul/announce/failure",
          "role",
          discoveryDruidNode.getNodeRole().getJsonName()
      );
      throw new RuntimeException("Failed to announce serviceId [" + serviceId + "]", e);
    }
    finally {
      registeringNodes.remove(serviceId);
    }
  }

  @Override
  public void unannounce(DiscoveryDruidNode discoveryDruidNode)
  {
    if (!lifecycleLock.awaitStarted(1, TimeUnit.SECONDS)) {
      throw new ISE("Announcer not started");
    }

    LOGGER.info("Unannouncing DiscoveryDruidNode[%s]", discoveryDruidNode);

    try {
      String serviceId = ConsulServiceIds.serviceId(config, discoveryDruidNode);
      consulApiClient.deregisterService(serviceId);
      announcedNodes.remove(serviceId);

      LOGGER.info("Successfully unannounced DiscoveryDruidNode[%s]", discoveryDruidNode);
      ConsulMetrics.emitCount(emitter, "consul/unannounce/success",
          "role", discoveryDruidNode.getNodeRole().getJsonName());
    }
    catch (Exception e) {
      // Unannouncement happens during shutdown, don't throw
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }

      LOGGER.error(e, "Failed to unannounce DiscoveryDruidNode[%s]", discoveryDruidNode);
      ConsulMetrics.emitCount(emitter, "consul/unannounce/failure",
          "role", discoveryDruidNode.getNodeRole().getJsonName());
    }
  }

  private void updateHealthChecks()
  {
    int nodeCount = announcedNodes.size();
    if (nodeCount == 0) {
      return; // Silent when nothing to do
    }

    LOGGER.debug("Updating health checks for %d nodes", nodeCount);

    int successCount = 0;
    int failureCount = 0;

    for (Map.Entry<String, DiscoveryDruidNode> entry : announcedNodes.entrySet()) {
      String serviceId = entry.getKey();
      try {
        long healthCheckStart = System.nanoTime();

        consulApiClient.passTtlCheck(serviceId, "Druid node is healthy");

        long healthCheckLatency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - healthCheckStart);
        ConsulMetrics.emitTimer(emitter, "consul/healthcheck/latency", healthCheckLatency,
            "serviceId", serviceId);

        successCount++;

        consecutiveFailures.remove(serviceId);
      }
      catch (Exception e) {
        failureCount++;

        int failures = consecutiveFailures
            .computeIfAbsent(serviceId, k -> new AtomicInteger(0))
            .incrementAndGet();

        // Keep WARN for failures - these matter
        LOGGER.warn(e, "Health check failed [%d/%d] for [%s]",
                    failures, MAX_FAILURES_BEFORE_REREGISTER, serviceId);
        ConsulMetrics.emitCount(emitter, "consul/healthcheck/failure",
            "serviceId", serviceId, "consecutiveFailures", String.valueOf(failures));

        if (failures >= MAX_FAILURES_BEFORE_REREGISTER) {
          // Keep WARN for recovery actions - these are important state changes
          LOGGER.warn("Re-registering [%s] after %d failures", serviceId, failures);
          try {
            // Re-fetch from map; node may have been concurrently removed during shutdown
            DiscoveryDruidNode node = announcedNodes.get(serviceId);
            if (node == null) {
              // Node was unannounced (e.g., during shutdown) - skip re-registration
              LOGGER.info("Skipping re-registration for [%s] - node no longer announced", serviceId);
              consecutiveFailures.remove(serviceId);
              continue;
            }
            consulApiClient.registerService(node);
            consulApiClient.passTtlCheck(serviceId, "Re-registered");
            consecutiveFailures.remove(serviceId);
            ConsulMetrics.emitCount(emitter, "consul/healthcheck/reregister",
                "serviceId", serviceId, "totalFailures", String.valueOf(failures));
            LOGGER.info("Successfully re-registered [%s]", serviceId);
          }
          catch (Exception reregEx) {
            LOGGER.error(reregEx, "Re-registration failed for [%s]", serviceId);
            ConsulMetrics.emitCount(emitter, "consul/healthcheck/reregister/failure",
                "serviceId", serviceId, "totalFailures", String.valueOf(failures));
          }
        }
      }
    }

    if (failureCount > 0) {
      LOGGER.info("Health checks: %d/%d failed", failureCount, nodeCount);
    } else {
      LOGGER.debug("Health checks: %d/%d succeeded", successCount, nodeCount);
    }
  }

}
