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

package org.apache.druid.msq.dart.worker;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.dart.worker.http.DartWorkerInfo;
import org.apache.druid.msq.dart.worker.http.GetWorkersResponse;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.apache.druid.msq.rpc.ResourcePermissionMapper;
import org.apache.druid.msq.rpc.WorkerResource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.security.AuthorizerMapper;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@ManageLifecycle
public class DartWorkerRunner
{
  private static final Logger log = new Logger(DartWorkerRunner.class);

  /**
   * Set of active controllers. Ignore requests from others.
   */
  @GuardedBy("this")
  private final Set<String> activeControllerHosts = new HashSet<>();

  /**
   * Query ID -> Worker instance.
   */
  @GuardedBy("this")
  private final Map<String, WorkerHolder> workerMap = new HashMap<>();
  private final DartWorkerFactory workerFactory;
  private final ExecutorService workerExec;
  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final ResourcePermissionMapper permissionMapper;
  private final AuthorizerMapper authorizerMapper;
  private final File baseTempDir;

  public DartWorkerRunner(
      final DartWorkerFactory workerFactory,
      final ExecutorService workerExec,
      final DruidNodeDiscoveryProvider discoveryProvider,
      final ResourcePermissionMapper permissionMapper,
      final AuthorizerMapper authorizerMapper,
      final File baseTempDir
  )
  {
    this.workerFactory = workerFactory;
    this.workerExec = workerExec;
    this.discoveryProvider = discoveryProvider;
    this.permissionMapper = permissionMapper;
    this.authorizerMapper = authorizerMapper;
    this.baseTempDir = baseTempDir;
  }

  /**
   * Start a worker, creating a holder for it. If a worker with this query ID is already started, does nothing.
   * Returns the worker.
   *
   * @throws DruidException if the controllerId does not correspond to a currently-active controller
   */
  public Worker startWorker(
      final String queryId,
      final String controllerHost,
      final QueryContext context
  )
  {
    final WorkerHolder holder;
    final boolean newHolder;

    synchronized (this) {
      if (!activeControllerHosts.contains(controllerHost)) {
        throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                            .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                            .build("Received startWorker request for unknown controller[%s]", controllerHost);
      }

      final WorkerHolder existingHolder = workerMap.get(queryId);
      if (existingHolder != null) {
        holder = existingHolder;
        newHolder = false;
      } else {
        final Worker worker = workerFactory.build(queryId, controllerHost, baseTempDir, context);
        final WorkerResource resource = new WorkerResource(worker, permissionMapper, authorizerMapper);
        holder = new WorkerHolder(worker, controllerHost, resource, DateTimes.nowUtc());
        workerMap.put(queryId, holder);
        this.notifyAll();
        newHolder = true;
      }
    }

    if (newHolder) {
      workerExec.submit(() -> {
        final String originalThreadName = Thread.currentThread().getName();
        try {
          Thread.currentThread().setName(StringUtils.format("%s[%s]", originalThreadName, queryId));
          holder.worker.run();
        }
        catch (Throwable t) {
          if (Thread.interrupted() || MSQFaultUtils.isCanceledException(t)) {
            log.debug(t, "Canceled, exiting thread.");
          } else {
            log.warn(t, "Worker for query[%s] failed and stopped.", queryId);
          }
        }
        finally {
          synchronized (this) {
            workerMap.remove(queryId, holder);
            this.notifyAll();
          }

          Thread.currentThread().setName(originalThreadName);
        }
      });
    }

    return holder.worker;
  }

  /**
   * Stops a worker.
   */
  public void stopWorker(final String queryId)
  {
    final WorkerHolder holder;

    synchronized (this) {
      holder = workerMap.get(queryId);
    }

    if (holder != null) {
      holder.worker.stop();
    }
  }

  /**
   * Get the worker resource handler for a query ID if it exists. Returns null if the worker is not running.
   */
  @Nullable
  public WorkerResource getWorkerResource(final String queryId)
  {
    synchronized (this) {
      final WorkerHolder holder = workerMap.get(queryId);
      if (holder != null) {
        return holder.resource;
      } else {
        return null;
      }
    }
  }

  /**
   * Returns a {@link GetWorkersResponse} with information about all active workers.
   */
  public GetWorkersResponse getWorkersResponse()
  {
    final List<DartWorkerInfo> infos = new ArrayList<>();

    synchronized (this) {
      for (final Map.Entry<String, WorkerHolder> entry : workerMap.entrySet()) {
        final String queryId = entry.getKey();
        final WorkerHolder workerHolder = entry.getValue();
        infos.add(
            new DartWorkerInfo(
                queryId,
                WorkerId.fromString(workerHolder.worker.id()),
                workerHolder.controllerHost,
                workerHolder.acceptTime
            )
        );
      }
    }

    return new GetWorkersResponse(infos);
  }

  @LifecycleStart
  public void start()
  {
    createAndCleanTempDirectory();

    final DruidNodeDiscovery brokers = discoveryProvider.getForNodeRole(NodeRole.BROKER);
    brokers.registerListener(new BrokerListener());
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (this) {
      final Collection<WorkerHolder> holders = workerMap.values();

      for (final WorkerHolder holder : holders) {
        holder.worker.stop();
      }

      for (final WorkerHolder holder : holders) {
        holder.worker.awaitStop();
      }
    }
  }

  /**
   * Method for testing. Waits for the set of queries to match a given predicate.
   */
  @VisibleForTesting
  void awaitQuerySet(Predicate<Set<String>> queryIdsPredicate) throws InterruptedException
  {
    synchronized (this) {
      while (!queryIdsPredicate.test(workerMap.keySet())) {
        wait();
      }
    }
  }

  /**
   * Creates the {@link #baseTempDir}, and removes any items in it that still exist.
   */
  void createAndCleanTempDirectory()
  {
    try {
      FileUtils.mkdirp(baseTempDir);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    final File[] files = baseTempDir.listFiles();

    if (files != null) {
      for (final File file : files) {
        if (file.isDirectory()) {
          try {
            FileUtils.deleteDirectory(file);
            log.info("Removed stale query directory[%s].", file);
          }
          catch (Exception e) {
            log.noStackTrace().warn(e, "Could not remove stale query directory[%s], skipping.", file);
          }
        }
      }
    }
  }

  private static class WorkerHolder
  {
    private final Worker worker;
    private final WorkerResource resource;
    private final String controllerHost;
    private final DateTime acceptTime;

    public WorkerHolder(
        Worker worker,
        String controllerHost,
        WorkerResource resource,
        final DateTime acceptTime
    )
    {
      this.worker = worker;
      this.resource = resource;
      this.controllerHost = controllerHost;
      this.acceptTime = acceptTime;
    }
  }

  /**
   * Listener that cancels work associated with Brokers that have gone away.
   */
  private class BrokerListener implements DruidNodeDiscovery.Listener
  {
    @Override
    public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
    {
      synchronized (DartWorkerRunner.this) {
        for (final DiscoveryDruidNode node : nodes) {
          activeControllerHosts.add(node.getDruidNode().getHostAndPortToUse());
        }
      }
    }

    @Override
    public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
    {
      final Set<String> hostsRemoved =
          nodes.stream().map(node -> node.getDruidNode().getHostAndPortToUse()).collect(Collectors.toSet());

      final List<Worker> workersToNotify = new ArrayList<>();

      synchronized (DartWorkerRunner.this) {
        activeControllerHosts.removeAll(hostsRemoved);

        for (Map.Entry<String, WorkerHolder> entry : workerMap.entrySet()) {
          if (hostsRemoved.contains(entry.getValue().controllerHost)) {
            workersToNotify.add(entry.getValue().worker);
          }
        }
      }

      for (final Worker worker : workersToNotify) {
        worker.controllerFailed();
      }
    }
  }
}
