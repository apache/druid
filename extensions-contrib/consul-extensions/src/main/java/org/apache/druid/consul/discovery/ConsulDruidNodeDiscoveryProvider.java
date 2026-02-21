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
import org.apache.druid.discovery.BaseNodeRoleWatcher;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.DruidNode;

import javax.annotation.Nullable;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Consul-based implementation of {@link DruidNodeDiscoveryProvider}.
 *
 * <p>Each {@link NodeRoleWatcher} performs synchronous (potentially blocking) Consul queries on its own single-thread
 * executor. Listener callbacks are dispatched via a shared single-thread executor to preserve callback ordering.
 *
 * <p>Consul queries can block up to {@code watchSeconds}; avoid invoking lifecycle methods from time-critical threads.
 */
@ManageLifecycle
public class ConsulDruidNodeDiscoveryProvider extends DruidNodeDiscoveryProvider
{
  private static final Logger LOGGER = new Logger(ConsulDruidNodeDiscoveryProvider.class);

  private final ConsulApiClient consulApiClient;
  private final ConsulDiscoveryConfig config;

  @Inject(optional = true)
  @Nullable
  private ServiceEmitter emitter;

  private ScheduledExecutorService listenerExecutor;

  private final ConcurrentHashMap<NodeRole, NodeRoleWatcher> nodeRoleWatchers = new ConcurrentHashMap<>();

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  @Inject
  public ConsulDruidNodeDiscoveryProvider(
      ConsulApiClient consulApiClient,
      ConsulDiscoveryConfig config
  )
  {
    this.consulApiClient = Preconditions.checkNotNull(consulApiClient, "consulApiClient");
    this.config = Preconditions.checkNotNull(config, "config");
  }

  @Override
  public BooleanSupplier getForNode(DruidNode node, NodeRole nodeRole)
  {
    return () -> {
      try {
        List<DiscoveryDruidNode> nodes = consulApiClient.getHealthyServices(nodeRole);
        return nodes.stream()
                    .anyMatch(n -> n.getDruidNode().getHostAndPortToUse().equals(node.getHostAndPortToUse()));
      }
      catch (Exception e) {
        LOGGER.error(e, "Error checking for node [%s] with role [%s]", node, nodeRole);
        return false;
      }
    };
  }

  @Override
  public DruidNodeDiscovery getForNodeRole(NodeRole nodeRole)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    return nodeRoleWatchers.computeIfAbsent(
        nodeRole,
        role -> {
          LOGGER.info("Creating NodeRoleWatcher for role[%s].", role);
          NodeRoleWatcher watcher = new NodeRoleWatcher(
              listenerExecutor,
              role,
              consulApiClient,
              config,
              emitter
          );
          watcher.start();
          LOGGER.info("Created NodeRoleWatcher for role[%s].", role);
          return watcher;
        }
    );
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      LOGGER.info("Starting ConsulDruidNodeDiscoveryProvider");

      // Single-threaded executor ensures listener callbacks execute in-order, preventing race conditions
      listenerExecutor = Execs.scheduledSingleThreaded("ConsulDruidNodeDiscoveryProvider-ListenerExecutor");

      LOGGER.info("Started ConsulDruidNodeDiscoveryProvider");

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
      throw new ISE("can't stop.");
    }

    LOGGER.info("Stopping ConsulDruidNodeDiscoveryProvider");

    for (NodeRoleWatcher watcher : nodeRoleWatchers.values()) {
      watcher.stop();
    }
    nodeRoleWatchers.clear();

    // Watcher threads must finish before shutting down listener executor to avoid RejectedExecutionException
    try {
      listenerExecutor.shutdown();
      if (!listenerExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        LOGGER.warn("Listener executor did not terminate in time");
        listenerExecutor.shutdownNow();
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted while waiting for listener executor termination");
      listenerExecutor.shutdownNow();
    }

    LOGGER.info("Stopped ConsulDruidNodeDiscoveryProvider");
    lifecycleLock.exitStopAndReset();
  }

  static class NodeRoleWatcher implements DruidNodeDiscovery
  {
    private static final Logger LOGGER = new Logger(NodeRoleWatcher.class);

    private final ConsulApiClient consulApiClient;
    private final ConsulDiscoveryConfig config;
    @Nullable
    private final ServiceEmitter emitter;

    private ExecutorService watchExecutor;

    private final LifecycleLock lifecycleLock = new LifecycleLock();

    private final NodeRole nodeRole;
    private final BaseNodeRoleWatcher baseNodeRoleWatcher;

    private final AtomicLong retryCount = new AtomicLong(0);

    /**
     * Creates a watcher for a single {@link NodeRole}. Consul calls are performed on the watch executor. Listener
     * callbacks are dispatched via {@code listenerExecutor}.
     */
    NodeRoleWatcher(
        ScheduledExecutorService listenerExecutor,
        NodeRole nodeRole,
        ConsulApiClient consulApiClient,
        ConsulDiscoveryConfig config,
        @Nullable ServiceEmitter emitter
    )
    {
      this.nodeRole = nodeRole;
      this.consulApiClient = consulApiClient;
      this.config = config;
      this.emitter = emitter;
      this.baseNodeRoleWatcher = BaseNodeRoleWatcher.create(listenerExecutor, nodeRole);
    }

    private void watch()
    {
      boolean cacheInitialized = false;
      long consulIndex = 0;

      if (!lifecycleLock.awaitStarted()) {
        LOGGER.error("Lifecycle not started, Exited Watch for role[%s].", nodeRole);
        return;
      }

      while (lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
        try {
          if (!cacheInitialized) {
            List<DiscoveryDruidNode> nodes = consulApiClient.getHealthyServices(nodeRole);
            Map<String, DiscoveryDruidNode> nodeMap = new HashMap<>();
            for (DiscoveryDruidNode node : nodes) {
              nodeMap.put(node.getDruidNode().getHostAndPortToUse(), node);
            }
            baseNodeRoleWatcher.resetNodes(nodeMap);
            baseNodeRoleWatcher.cacheInitialized();
            cacheInitialized = true;

            LOGGER.info("Cache initialized for role[%s] with [%d] nodes", nodeRole, nodes.size());
          }

          long watchStart = System.nanoTime();

          long watchSeconds = config.getWatch().getWatchSeconds().getStandardSeconds();
          ConsulApiClient.ConsulWatchResult watchResult = consulApiClient.watchServices(
              nodeRole,
              consulIndex,
              watchSeconds
          );

          long watchLatency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - watchStart);
          ConsulMetrics.emitTimer(emitter, "consul/watch/latency", watchLatency, "role", nodeRole.getJsonName());

          long newIndex = watchResult.getConsulIndex();
          if (newIndex != consulIndex) {
            consulIndex = newIndex;

            List<DiscoveryDruidNode> newNodes = watchResult.getNodes();
            Map<String, DiscoveryDruidNode> newNodeMap = new HashMap<>();
            for (DiscoveryDruidNode node : newNodes) {
              newNodeMap.put(node.getDruidNode().getHostAndPortToUse(), node);
            }

            Collection<DiscoveryDruidNode> currentNodes = baseNodeRoleWatcher.getAllNodes();
            Map<String, DiscoveryDruidNode> currentNodeMap = new HashMap<>();
            for (DiscoveryDruidNode node : currentNodes) {
              currentNodeMap.put(node.getDruidNode().getHostAndPortToUse(), node);
            }

            for (Map.Entry<String, DiscoveryDruidNode> entry : newNodeMap.entrySet()) {
              if (!currentNodeMap.containsKey(entry.getKey())) {
                try {
                  baseNodeRoleWatcher.childAdded(entry.getValue());
                  ConsulMetrics.emitCount(emitter, "consul/watch/added", "role", nodeRole.getJsonName());
                }
                catch (RejectedExecutionException e) {
                  LOGGER.debug("Ignoring node add during shutdown for role[%s]", nodeRole);
                }
              }
            }

            for (Map.Entry<String, DiscoveryDruidNode> entry : currentNodeMap.entrySet()) {
              if (!newNodeMap.containsKey(entry.getKey())) {
                try {
                  baseNodeRoleWatcher.childRemoved(entry.getValue());
                  ConsulMetrics.emitCount(emitter, "consul/watch/removed", "role", nodeRole.getJsonName());
                }
                catch (RejectedExecutionException e) {
                  // Expected during shutdown - executor is terminated
                  LOGGER.debug("Ignoring node removal during shutdown for role[%s]", nodeRole);
                }
              }
            }
          }

          retryCount.set(0);
        }
        catch (Exception ex) {
          if (Thread.currentThread().isInterrupted()) {
            LOGGER.info("Watch interrupted during shutdown for role[%s]", nodeRole);
            break;
          }

          if (isSocketTimeout(ex)) {
            LOGGER.debug("Watch timeout for role[%s], re-issuing blocking query.", nodeRole);
            continue;
          }

          LOGGER.warn(ex, "Exception while watching for role[%s], will retry.", nodeRole);

          ConsulMetrics.emitCount(emitter, "consul/watch/error", "role", nodeRole.getJsonName());

          long count = retryCount.incrementAndGet();
          if (config.getWatch().getMaxWatchRetries() != Long.MAX_VALUE && count > config.getWatch().getMaxWatchRetries()) {
            long circuitBreakerSleepMs = config.getWatch().getCircuitBreakerSleep().getMillis();
            LOGGER.error(
                "Max watch retries [%d] exceeded for role[%s]; circuit breaker OPEN, sleeping %s then retry",
                config.getWatch().getMaxWatchRetries(),
                nodeRole,
                config.getWatch().getCircuitBreakerSleep()
            );
            ConsulMetrics.emitCount(emitter, "consul/watch/circuit_breaker",
                "state", "open", "role", nodeRole.getJsonName());

            sleep(circuitBreakerSleepMs);

            retryCount.set(0);
            cacheInitialized = false;
            ConsulMetrics.emitCount(emitter, "consul/watch/circuit_breaker",
                "state", "reset", "role", nodeRole.getJsonName());
            continue;
          }

          long base = Math.max(1L, config.getWatch().getWatchRetryDelay().getMillis());
          int exp = (int) Math.min(6, count);
          long backoff = Math.min(300_000L, base * (1L << exp));
          long jitter = (long) (backoff * (0.5 + ThreadLocalRandom.current().nextDouble()));
          sleep(jitter);
        }
      }

      LOGGER.info("Exited Watch for role[%s].", nodeRole);
    }

    private void sleep(long ms)
    {
      try {
        Thread.sleep(ms);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    /**
     * Checks if the exception is or is caused by a SocketTimeoutException.
     * This is expected behavior for Consul blocking queries that time out.
     */
    private boolean isSocketTimeout(Throwable ex)
    {
      Throwable current = ex;
      while (current != null) {
        if (current instanceof SocketTimeoutException) {
          return true;
        }
        current = current.getCause();
      }
      return false;
    }

    public void start()
    {
      if (!lifecycleLock.canStart()) {
        throw new ISE("can't start.");
      }

      try {
        LOGGER.info("Starting NodeRoleWatcher for role[%s]...", nodeRole);
        this.watchExecutor = Execs.singleThreaded(this.getClass().getName() + nodeRole.getJsonName());
        watchExecutor.submit(this::watch);
        lifecycleLock.started();
        ConsulMetrics.emitCount(
            emitter,
            "consul/watch/lifecycle",
            "role",
            nodeRole.getJsonName(),
            "state",
            "start"
        );
        LOGGER.info("Started NodeRoleWatcher for role[%s].", nodeRole);
      }
      finally {
        lifecycleLock.exitStart();
      }
    }

    public void stop()
    {
      if (!lifecycleLock.canStop()) {
        throw new ISE("can't stop.");
      }

      try {
        LOGGER.info("Stopping NodeRoleWatcher for role[%s]...", nodeRole);
        watchExecutor.shutdownNow();

        if (!watchExecutor.awaitTermination(15, TimeUnit.SECONDS)) {
          LOGGER.warn("Failed to stop watchExecutor for role[%s]", nodeRole);
        }
        ConsulMetrics.emitCount(
            emitter,
            "consul/watch/lifecycle",
            "role",
            nodeRole.getJsonName(),
            "state",
            "stop"
        );
        LOGGER.info("Stopped NodeRoleWatcher for role[%s].", nodeRole);
      }
      catch (Exception ex) {
        LOGGER.error(ex, "Failed to stop NodeRoleWatcher for role[%s].", nodeRole);
      }
      finally {
        // Allow restart and leave lock in a clean state
        lifecycleLock.exitStopAndReset();
      }
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      return baseNodeRoleWatcher.getAllNodes();
    }

    @Override
    public void registerListener(Listener listener)
    {
      baseNodeRoleWatcher.registerListener(listener);
    }
  }
}
