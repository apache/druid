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

package io.druid.server.coordinator;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.ServerInventoryView;
import io.druid.client.ServerView;
import io.druid.concurrent.Execs;
import io.druid.concurrent.LifecycleLock;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.cost.ClusterCostCache;
import io.druid.timeline.DataSegment;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@ManageLifecycle
public class CachingCostBalancerStrategyFactory implements BalancerStrategyFactory
{
  private static final EmittingLogger LOG = new EmittingLogger(CachingCostBalancerStrategyFactory.class);

  private final ServerInventoryView serverInventoryView;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private volatile boolean initialized = false;
  private final ExecutorService executor = Execs.singleThreaded("CachingCostBalancerStrategy-executor");
  private final ClusterCostCache.Builder clusterCostCacheBuilder = ClusterCostCache.builder();

  @Inject
  public CachingCostBalancerStrategyFactory(ServerInventoryView serverInventoryView)
  {
    this.serverInventoryView = Preconditions.checkNotNull(serverInventoryView);
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("CachingCostBalancerStrategyFactory can not be started");
    }
    try {
      serverInventoryView.registerSegmentCallback(
          executor,
          new ServerView.SegmentCallback()
          {
            @Override
            public ServerView.CallbackAction segmentAdded(
                DruidServerMetadata server, DataSegment segment
            )
            {
              clusterCostCacheBuilder.addSegment(server.getName(), segment);
              return ServerView.CallbackAction.CONTINUE;
            }

            @Override
            public ServerView.CallbackAction segmentRemoved(
                DruidServerMetadata server, DataSegment segment
            )
            {
              clusterCostCacheBuilder.removeSegment(server.getName(), segment);
              return ServerView.CallbackAction.CONTINUE;
            }

            @Override
            public ServerView.CallbackAction segmentViewInitialized()
            {
              initialized = true;
              return ServerView.CallbackAction.CONTINUE;
            }
          }
      );

      serverInventoryView.registerServerRemovedCallback(
          executor,
          server -> {
            clusterCostCacheBuilder.removeServer(server.getName());
            return ServerView.CallbackAction.CONTINUE;
          }
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
      throw new ISE("CachingCostBalancerStrategyFactory can not be stopped");
    }
    executor.shutdownNow();
  }

  @Override
  public BalancerStrategy createBalancerStrategy(final ListeningExecutorService exec)
  {
    if (!lifecycleLock.awaitStarted()) {
      throw new ISE("CachingCostBalancerStrategyFactory is not started");
    }
    if (initialized) {
      try {
        CompletableFuture<CachingCostBalancerStrategy> future = CompletableFuture.supplyAsync(
            () -> new CachingCostBalancerStrategy(clusterCostCacheBuilder.build(), exec),
            executor
        );
        try {
          return future.get(1, TimeUnit.SECONDS);
        }
        catch (CancellationException e) {
          LOG.error("CachingCostBalancerStrategy creation has been cancelled");
        }
        catch (ExecutionException e) {
          LOG.error(e, "Failed to create CachingCostBalancerStrategy");
        }
        catch (TimeoutException e) {
          LOG.error("CachingCostBalancerStrategy creation took more than 1 second!");
        }
        catch (InterruptedException e) {
          LOG.error("CachingCostBalancerStrategy creation has been interrupted");
          Thread.currentThread().interrupt();
        }
      }
      catch (RejectedExecutionException e) {
        LOG.error("CachingCostBalancerStrategy creation has been rejected");
      }
    } else {
      LOG.error("CachingCostBalancerStrategy could not be created as serverView is not initialized yet");
    }
    LOG.info("Fallback to CostBalancerStrategy");
    return new CostBalancerStrategy(exec);
  }
}
