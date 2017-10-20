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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.ServerInventoryView;
import io.druid.client.ServerView;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.lifecycle.Lifecycle;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class CachingCostBalancerStrategyFactory implements BalancerStrategyFactory
{
  private static final EmittingLogger LOG = new EmittingLogger(CachingCostBalancerStrategyFactory.class);

  /** Must be single-threaded, because {@link ClusterCostCache.Builder} and downstream builders are not thread-safe */
  private final ExecutorService executor = Execs.singleThreaded("CachingCostBalancerStrategy-executor");
  private final ClusterCostCache.Builder clusterCostCacheBuilder = ClusterCostCache.builder();
  /**
   * Atomic is needed to use compareAndSet(true, true) construction below, that is linearizable with the write made from
   * callback, that ensures visibility of the write made from callback. Neither plain field nor volatile field read
   * ensure such visibility
   */
  private final AtomicBoolean initialized = new AtomicBoolean(false);

  @JsonCreator
  public CachingCostBalancerStrategyFactory(
      @JacksonInject ServerInventoryView serverInventoryView,
      @JacksonInject Lifecycle lifecycle
  ) throws Exception
  {
    // Adding to lifecycle dynamically because couldn't use @ManageLifecycle on the class,
    // see https://github.com/druid-io/druid/issues/4980
    lifecycle.addMaybeStartManagedInstance(this);

    serverInventoryView.registerSegmentCallback(
        executor,
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            clusterCostCacheBuilder.addSegment(server.getName(), segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            clusterCostCacheBuilder.removeSegment(server.getName(), segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            initialized.set(true);
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

  @Override
  public BalancerStrategy createBalancerStrategy(final ListeningExecutorService exec)
  {
    if (initialized.compareAndSet(true, true)) {
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
