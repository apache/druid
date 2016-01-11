/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.autoscaling;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TierRoutingTaskRunner;
import io.druid.indexing.overlord.routing.TierRouteConfig;
import io.druid.indexing.overlord.routing.TierTaskRunnerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TierRoutingManagementStrategy implements ResourceManagementStrategy<TierRoutingTaskRunner>
{
  private static final Logger LOG = new Logger(TierRoutingManagementStrategy.class);
  public static final String ROUTING_TARGET_CONTEXT_KEY = "io.druid.index.tier.target";
  public static final String DEFAULT_ROUTE = "__default";
  private final Supplier<TierRouteConfig> configSupplier;
  private final ConcurrentMap<String, TaskRunner> runnerMap;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final ScheduledExecutorService managementExecutorService;
  private final AtomicLong numberOfUpdates = new AtomicLong(0L);

  public TierRoutingManagementStrategy(
      ConcurrentMap<String, TaskRunner> runnerMap,
      Supplier<TierRouteConfig> configSupplier,
      ScheduledExecutorService managementExecutorService
  )
  {
    this.runnerMap = runnerMap;
    this.configSupplier = configSupplier;
    this.managementExecutorService = managementExecutorService;
  }

  @Override
  // State is communicated via configSupplier and runnerMap
  public synchronized void startManagement(TierRoutingTaskRunner unused)
  {
    if (!started.compareAndSet(false, true)) {
      throw new ISE("Already started");
    }
    if (managementExecutorService.isShutdown()) {
      started.set(false);
      throw new ISE("Already stopped");
    }
    managementExecutorService.scheduleWithFixedDelay(
        new Runnable()
        {
          final AtomicReference<TierRouteConfig> priorConfig = new AtomicReference<>(null);

          @Override
          public void run()
          {
            try {
              // Local management monitors for config changes.
              final TierRouteConfig config = configSupplier.get();
              if (config == null) {
                throw new ISE("No config found");
              }

              final TierRouteConfig prior = priorConfig.get();
              if (prior == config) {
                LOG.debug("No change in config since last check, skipping update");
                return;
              }

              if (!priorConfig.compareAndSet(prior, config)) {
                LOG.warn(
                    "Tier routing config was updated in a racy way... leaving config [%s] and skipping update",
                    prior
                );
                return;
              }

              for (String tier : config.getTiers()) {
                if (runnerMap.containsKey(tier)) {
                  LOG.debug("Tier [%s] already in map", tier);
                  continue;
                }
                final TierTaskRunnerFactory runnerFactory = config.getRouteFactory(tier);
                final TaskRunner runner = runnerFactory.build();
                if (runnerMap.putIfAbsent(tier, runner) != null) {
                  LOG.warn("Tier [%s] lost a race condition, ignoring runner already in map", tier);
                  continue;
                }
                try {
                  synchronized (TierRoutingManagementStrategy.this) {
                    if (started.get()) {
                      runner.start();
                    } else {
                      LOG.warn("Tier [%s] trying to start after shutdown", tier);
                      if (!runnerMap.remove(tier, runner)) {
                        // This shouldn't happen, but is here as a super safeguard
                        LOG.warn("Someone else will have to cleanup the runner for tier [%s], they won a race", tier);
                      }
                    }
                  }
                }
                catch (Exception e) {
                  LOG.error(e, "Error starting tier [%s], continuing", tier);
                }
              }
              // TODO: what about tiers that vanish from config? I'm inclined to leave them running in case the vanishing was an error
              // Restarting JVM should take care of such a case
            }
            catch (Exception e) {
              LOG.error(e, "Tier routing management encountered exception. Trying again");
            }
            finally {
              // Used in unit tests
              synchronized (numberOfUpdates) {
                numberOfUpdates.incrementAndGet();
                numberOfUpdates.notifyAll();
              }
            }
          }
        },
        0,
        10, // TODO: make this configurable
        TimeUnit.SECONDS
    );
  }

  @VisibleForTesting
  void waitForUpdate() throws InterruptedException
  {
    final long startingUpdates = numberOfUpdates.get();
    while (startingUpdates == numberOfUpdates.get()) {
      synchronized (numberOfUpdates) {
        numberOfUpdates.wait();
      }
    }
  }

  @Override
  public synchronized void stopManagement()
  {
    if (!started.compareAndSet(true, false)) {
      LOG.warn("Ignoring repeated stop request");
      return;
    }
    managementExecutorService.shutdown();
    try {
      if (!managementExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Could not shut down all tasks. Continuing anyways");
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error(e, "Interrupted");
    }
    for (String tier : runnerMap.keySet()) {
      final TaskRunner runner = runnerMap.get(tier);
      if (runner == null) {
        LOG.warn("Race condition for tier [%s]", tier);
        continue;
      }
      try {
        runner.stop();
      }
      catch (Exception e) {
        LOG.error(e, "Error shutting down runner for tier [%s]", tier);
      }
      runnerMap.remove(tier);
    }
  }

  @Override
  public synchronized ScalingStats getStats()
  {
    final ScalingStats stats = new ScalingStats(0);
    final AtomicBoolean foundSomething = new AtomicBoolean(false);
    stats.addAllEvents(ImmutableList.copyOf(
        FluentIterable
            .from(runnerMap.values())
            .transformAndConcat(new Function<TaskRunner, List<ScalingStats.ScalingEvent>>()
            {
              @Nullable
              @Override
              public List<ScalingStats.ScalingEvent> apply(@Nullable TaskRunner runner)
              {
                if (runner == null) {
                  return ImmutableList.of();
                }
                final Optional<ScalingStats> stats = runner.getScalingStats();
                if (stats.isPresent()) {
                  foundSomething.set(true);
                  return stats.get().toList();
                } else {
                  return ImmutableList.of();
                }
              }
            })
    ));
    return foundSomething.get() ? stats : null;
  }

  public TaskRunner getRunner(Task task)
  {
    final Object tierobj = task.getContextValue(ROUTING_TARGET_CONTEXT_KEY);
    final String tier;
    if (tierobj == null) {
      LOG.debug("No route context found for task [%s]. Using default [%s]", task.getId(), DEFAULT_ROUTE);
      tier = DEFAULT_ROUTE;
    } else {
      tier = tierobj.toString();
    }

    LOG.info("Using tier [%s] for task [%s]", tier, task.getId());

    return runnerMap.get(tier);
  }
}
