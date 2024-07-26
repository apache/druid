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

package org.apache.druid.server.coordinator.balancer;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.client.ServerView;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * @deprecated This is currently being used only in tests for benchmarking purposes
 * and will be removed in future releases.
 */
@Deprecated
public class CachingCostBalancerStrategyFactory extends BalancerStrategyFactory
{
  private static final EmittingLogger LOG = new EmittingLogger(CachingCostBalancerStrategyFactory.class);

  /** Must be single-threaded, because {@link ClusterCostCache.Builder} and downstream builders are not thread-safe */
  private final ExecutorService executor = Execs.singleThreaded("CachingCostBalancerStrategy-executor");
  private final ClusterCostCache.Builder clusterCostCacheBuilder = ClusterCostCache.builder();

  private final CountDownLatch initialized = new CountDownLatch(1);
  private final CachingCostBalancerStrategyConfig config;

  @JsonCreator
  public CachingCostBalancerStrategyFactory(
      @JacksonInject ServerInventoryView serverInventoryView,
      @JacksonInject Lifecycle lifecycle,
      @JacksonInject CachingCostBalancerStrategyConfig config
  ) throws Exception
  {
    this.config = config;

    // Adding to lifecycle dynamically because couldn't use @ManageLifecycle on the class,
    // see https://github.com/apache/druid/issues/4980
    lifecycle.addMaybeStartManagedInstance(this);

    serverInventoryView.registerSegmentCallback(
        executor,
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            if (server.isSegmentReplicationTarget()) {
              clusterCostCacheBuilder.addSegment(server.getName(), segment);
            }
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            if (server.isSegmentReplicationTarget()) {
              clusterCostCacheBuilder.removeSegment(server.getName(), segment);
            }
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            initialized.countDown();
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
          {
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    serverInventoryView.registerServerRemovedCallback(
        executor,
        server -> {
          if (server.isSegmentReplicationTarget()) {
            clusterCostCacheBuilder.removeServer(server.getName());
          }
          return ServerView.CallbackAction.CONTINUE;
        }
    );
  }

  @LifecycleStart
  public void start()
  {
    // do nothing
  }

  @LifecycleStop
  public void stop()
  {
    executor.shutdownNow();
  }

  private boolean isInitialized()
  {
    return initialized.getCount() == 0;
  }

  @Override
  public BalancerStrategy createBalancerStrategy(int numBalancerThreads)
  {
    final ListeningExecutorService exec = getOrCreateBalancerExecutor(numBalancerThreads);
    LOG.warn(
        "'cachingCost' balancer strategy has been deprecated as it can lead to"
        + " unbalanced clusters. Use 'cost' strategy instead."
    );
    if (!isInitialized() && config.isAwaitInitialization()) {
      try {
        final long startMillis = System.currentTimeMillis();
        LOG.info("Waiting for segment view initialization before creating CachingCostBalancerStrategy.");
        initialized.await();
        LOG.info("Segment view initialized in [%,d] ms.", System.currentTimeMillis() - startMillis);
      }
      catch (InterruptedException e) {
        LOG.error(e, "Segment view initialization has been interrupted.");
        Thread.currentThread().interrupt();
      }
    }

    if (isInitialized()) {
      try {
        // Calling clusterCostCacheBuilder.build() in the same thread (executor's sole thread) where
        // clusterCostCacheBuilder is updated, to avoid problems with concurrent updates
        CompletableFuture<CachingCostBalancerStrategy> future = CompletableFuture.supplyAsync(
            () -> new CachingCostBalancerStrategy(clusterCostCacheBuilder.build(), exec),
            executor
        );
        try {
          return future.get();
        }
        catch (CancellationException e) {
          LOG.error("CachingCostBalancerStrategy creation has been cancelled");
        }
        catch (ExecutionException e) {
          LOG.error(e, "Failed to create CachingCostBalancerStrategy");
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
