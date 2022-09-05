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

package org.apache.druid.k8s.discovery;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.kubernetes.client.util.Watch;
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
import org.apache.druid.server.DruidNode;
import org.apache.druid.utils.CloseableUtils;

import java.io.Closeable;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

@ManageLifecycle
public class K8sDruidNodeDiscoveryProvider extends DruidNodeDiscoveryProvider
{
  private static final Logger LOGGER = new Logger(K8sDruidNodeDiscoveryProvider.class);

  private final PodInfo podInfo;
  private final K8sDiscoveryConfig discoveryConfig;

  private final K8sApiClient k8sApiClient;

  private ExecutorService listenerExecutor;

  private final ConcurrentHashMap<NodeRole, NodeRoleWatcher> nodeTypeWatchers = new ConcurrentHashMap<>();

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final long watcherErrorRetryWaitMS;

  @Inject
  public K8sDruidNodeDiscoveryProvider(
      PodInfo podInfo,
      K8sDiscoveryConfig discoveryConfig,
      K8sApiClient k8sApiClient
  )
  {
    // at some point, if needed, watcherErrorRetryWaitMS here can be made configurable and maybe some randomization
    // component as well.
    this(podInfo, discoveryConfig, k8sApiClient, 10_000);
  }

  @VisibleForTesting
  K8sDruidNodeDiscoveryProvider(
      PodInfo podInfo,
      K8sDiscoveryConfig discoveryConfig,
      K8sApiClient k8sApiClient,
      long watcherErrorRetryWaitMS
  )
  {
    this.podInfo = podInfo;
    this.discoveryConfig = discoveryConfig;
    this.k8sApiClient = k8sApiClient;
    this.watcherErrorRetryWaitMS = watcherErrorRetryWaitMS;
  }

  @Override
  public BooleanSupplier getForNode(DruidNode node, NodeRole nodeRole)
  {
    return () -> k8sApiClient.listPods(
        podInfo.getPodNamespace(),
        K8sDruidNodeAnnouncer.getLabelSelectorForNode(discoveryConfig, nodeRole, node),
        nodeRole
    ).getDruidNodes().containsKey(node.getHostAndPortToUse());
  }

  @Override
  public DruidNodeDiscovery getForNodeRole(NodeRole nodeType)
  {
    return getForNodeRole(nodeType, true);
  }

  @VisibleForTesting
  NodeRoleWatcher getForNodeRole(NodeRole nodeType, boolean startAfterCreation)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    return nodeTypeWatchers.computeIfAbsent(
        nodeType,
        nType -> {
          LOGGER.info("Creating NodeRoleWatcher for nodeRole [%s].", nType);
          NodeRoleWatcher nodeRoleWatcher = new NodeRoleWatcher(
              listenerExecutor,
              nType,
              podInfo,
              discoveryConfig,
              k8sApiClient,
              watcherErrorRetryWaitMS
          );
          if (startAfterCreation) {
            nodeRoleWatcher.start();
          }
          LOGGER.info("Created NodeRoleWatcher for nodeRole [%s].", nType);
          return nodeRoleWatcher;
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
      LOGGER.info("starting");

      // This is single-threaded to ensure that all listener calls are executed precisely in the oder of add/remove
      // event occurences.
      listenerExecutor = Execs.singleThreaded("K8sDruidNodeDiscoveryProvider-ListenerExecutor");

      LOGGER.info("started");

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

    LOGGER.info("stopping");

    for (NodeRoleWatcher watcher : nodeTypeWatchers.values()) {
      watcher.stop();
    }
    listenerExecutor.shutdownNow();

    LOGGER.info("stopped");
  }

  @VisibleForTesting
  static class NodeRoleWatcher implements DruidNodeDiscovery
  {
    private static final Logger LOGGER = new Logger(NodeRoleWatcher.class);

    private final PodInfo podInfo;
    private final K8sDiscoveryConfig discoveryConfig;

    private final K8sApiClient k8sApiClient;

    private ExecutorService watchExecutor;

    private final LifecycleLock lifecycleLock = new LifecycleLock();

    private final AtomicReference<Closeable> watchRef = new AtomicReference<>();
    private static final Closeable STOP_MARKER = () -> {};

    private final NodeRole nodeRole;
    private final BaseNodeRoleWatcher baseNodeRoleWatcher;

    private final long watcherErrorRetryWaitMS;

    NodeRoleWatcher(
        ExecutorService listenerExecutor,
        NodeRole nodeRole,
        PodInfo podInfo,
        K8sDiscoveryConfig discoveryConfig,
        K8sApiClient k8sApiClient,
        long watcherErrorRetryWaitMS
    )
    {
      this.podInfo = podInfo;
      this.discoveryConfig = discoveryConfig;
      this.k8sApiClient = k8sApiClient;

      this.nodeRole = nodeRole;
      this.baseNodeRoleWatcher = new BaseNodeRoleWatcher(listenerExecutor, nodeRole);

      this.watcherErrorRetryWaitMS = watcherErrorRetryWaitMS;
    }

    private void watch()
    {
      String labelSelector = K8sDruidNodeAnnouncer.getLabelSelectorForNodeRole(discoveryConfig, nodeRole);
      boolean cacheInitialized = false;

      if (!lifecycleLock.awaitStarted()) {
        LOGGER.error("Lifecycle not started, Exited Watch for NodeRole [%s].", nodeRole);
        return;
      }

      while (lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
        try {
          DiscoveryDruidNodeList list = k8sApiClient.listPods(podInfo.getPodNamespace(), labelSelector, nodeRole);
          baseNodeRoleWatcher.resetNodes(list.getDruidNodes());

          if (!cacheInitialized) {
            baseNodeRoleWatcher.cacheInitialized();
            cacheInitialized = true;
          }

          keepWatching(
              podInfo.getPodNamespace(),
              labelSelector,
              list.getResourceVersion()
          );
        }
        catch (Throwable ex) {
          LOGGER.error(ex, "Expection while watching for NodeRole [%s].", nodeRole);

          // Wait a little before trying again.
          sleep(watcherErrorRetryWaitMS);
        }
      }

      LOGGER.info("Exited Watch for NodeRole [%s].", nodeRole);
    }

    private void keepWatching(String namespace, String labelSelector, String resourceVersion)
    {
      String nextResourceVersion = resourceVersion;
      while (lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
        try {
          WatchResult iter =
              k8sApiClient.watchPods(podInfo.getPodNamespace(), labelSelector, nextResourceVersion, nodeRole);

          if (iter == null) {
            // history not available, we need to start from scratch
            return;
          }

          try {
            while (iter.hasNext()) {
              Watch.Response<DiscoveryDruidNodeAndResourceVersion> item = iter.next();
              if (item != null && item.type != null && item.object != null) {
                switch (item.type) {
                  case WatchResult.ADDED:
                    baseNodeRoleWatcher.childAdded(item.object.getNode());
                    break;
                  case WatchResult.DELETED:
                    baseNodeRoleWatcher.childRemoved(item.object.getNode());
                    break;
                  default:
                }

                // This should be updated after the action has been dealt with successfully
                nextResourceVersion = item.object.getResourceVersion();

              } else {
                // Try again by starting the watch from the beginning. This can happen if the
                // watch goes bad.
                LOGGER.debug("Received NULL item while watching node type [%s]. Restarting watch.", this.nodeRole);
                return;
              }
            }
          }
          finally {
            iter.close();
          }

        }
        catch (SocketTimeoutException ex) {
          // socket read timeout can happen normally due to k8s not having anything new to push leading to socket
          // read timeout, so no error log
          sleep(watcherErrorRetryWaitMS);
        }
        catch (Throwable ex) {
          LOGGER.error(ex, "Error while watching node type [%s]", this.nodeRole);
          sleep(watcherErrorRetryWaitMS);
        }
      }
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

    public void start()
    {
      if (!lifecycleLock.canStart()) {
        throw new ISE("can't start.");
      }

      try {
        LOGGER.info("Starting NodeRoleWatcher for [%s]...", nodeRole);
        this.watchExecutor = Execs.singleThreaded(this.getClass().getName() + nodeRole.getJsonName());
        watchExecutor.submit(this::watch);
        lifecycleLock.started();
        LOGGER.info("Started NodeRoleWatcher for [%s].", nodeRole);
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
        LOGGER.info("Stopping NodeRoleWatcher for [%s]...", nodeRole);
        // STOP_MARKER cannot throw exceptions on close(), so this is OK.
        CloseableUtils.closeAndSuppressExceptions(STOP_MARKER, e -> {});
        watchExecutor.shutdownNow();

        if (!watchExecutor.awaitTermination(15, TimeUnit.SECONDS)) {
          LOGGER.warn("Failed to stop watchExecutor for NodeRoleWatcher[%s]", nodeRole);
        }
        LOGGER.info("Stopped NodeRoleWatcher for [%s].", nodeRole);
      }
      catch (Exception ex) {
        LOGGER.error(ex, "Failed to stop NodeRoleWatcher for [%s].", nodeRole);
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
