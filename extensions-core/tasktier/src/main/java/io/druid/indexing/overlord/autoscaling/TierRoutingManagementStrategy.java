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
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.logger.Logger;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TierRoutingTaskRunner;
import io.druid.indexing.overlord.routing.TierRouteConfig;
import io.druid.indexing.overlord.routing.TierTaskRunnerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TierRoutingManagementStrategy implements ResourceManagementStrategy<TierRoutingTaskRunner>
{
  private static final Logger LOG = new Logger(TierRoutingManagementStrategy.class);
  public static final String ROUTING_TARGET_CONTEXT_KEY = "io.druid.index.tier.target";
  public static final String DEFAULT_ROUTE = "__default";
  private final ReadWriteLock startStopStateLock = new ReentrantReadWriteLock(true);
  private final Supplier<TierRouteConfig> configSupplier;
  private final ScheduledExecutorFactory managementExecutorServiceFactory;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicLong numberOfUpdates = new AtomicLong(0L);
  private final AtomicLong managementEpoch = new AtomicLong(0L);
  private volatile TierRoutingTaskRunner runner = null;
  private volatile ListeningScheduledExecutorService managementExecutorService = null;

  public TierRoutingManagementStrategy(
      Supplier<TierRouteConfig> configSupplier,
      ScheduledExecutorFactory managementExecutorServiceFactory
  )
  {
    this.configSupplier = configSupplier;
    this.managementExecutorServiceFactory = managementExecutorServiceFactory;
  }

  @Override
  // State is communicated via configSupplier and runnerMap
  public void startManagement(final TierRoutingTaskRunner runner)
  {
    try {
      startStopStateLock.writeLock().lockInterruptibly();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    try {
      if (!started.compareAndSet(false, true)) {
        throw new ISE("Already started");
      }
      this.runner = runner;
      managementExecutorService = MoreExecutors.listeningDecorator(managementExecutorServiceFactory.create(
          1,
          "TierRoutingManagement--%d"
      ));
      final ListenableFuture future = managementExecutorService.scheduleWithFixedDelay(
          new Runnable()
          {
            final AtomicReference<TierRouteConfig> priorConfig = new AtomicReference<>(null);

            @Override
            public void run()
            {
              try {
                startStopStateLock.readLock().lockInterruptibly();
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
              }
              try {
                if (!started.get()) {
                  LOG.debug("Management not started, returning");
                  return;
                }
                // Local management monitors for config changes.
                final TierRouteConfig config = configSupplier.get();

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


                final ConcurrentMap<String, TaskRunner> runnerMap = runner.getRunnerMap();

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
                    runner.start();
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
                startStopStateLock.readLock().unlock();
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

      Futures.addCallback(future, new FutureCallback()
      {
        @Override
        public void onSuccess(@Nullable Object result)
        {
          LOG.info("Success");
        }

        @Override
        public void onFailure(Throwable t)
        {
          if (t instanceof CancellationException) {
            LOG.debug("Management thread cancelled");
          } else {
            LOG.error(t, "Unhandled exception in management thread for runner %s", runner);
          }
        }
      });
      LOG.info("Started management of %s", runner);
    }
    finally {
      startStopStateLock.writeLock().unlock();
    }
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
  public void stopManagement()
  {
    try {
      startStopStateLock.writeLock().lockInterruptibly();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    try {
      if (!started.compareAndSet(true, false)) {
        LOG.warn("Ignoring repeated stop request");
        return;
      }
      managementExecutorService.shutdownNow();
      try {
        if (!managementExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
          LOG.warn("Could not shut down all management tasks! Continuing anyways");
        }
        managementExecutorService = null;
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error(e, "Interrupted");
      }
      final ConcurrentMap<String, TaskRunner> runnerMap = runner.getRunnerMap();

      for (String tier : runnerMap.keySet()) {
        final TaskRunner runner = runnerMap.remove(tier);
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
      }
      LOG.info("Stopped management");
    }
    finally {
      startStopStateLock.writeLock().unlock();
    }
  }

  @Override
  public ScalingStats getStats()
  {
    try {
      startStopStateLock.readLock().lockInterruptibly();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    try {
      if (!started.get()) {
        throw new IllegalStateException("Management not started");
      }
      final ScalingStats stats = new ScalingStats(0);
      final AtomicBoolean foundSomething = new AtomicBoolean(false);
      stats.addAllEvents(ImmutableList.copyOf(
          FluentIterable
              .from(runner.getRunnerMap().values())
              .transformAndConcat(new Function<TaskRunner, List<ScalingStats.ScalingEvent>>()
              {
                @Nullable
                @Override
                public List<ScalingStats.ScalingEvent> apply(@Nullable TaskRunner otherRunner)
                {
                  if (otherRunner == null) {
                    return ImmutableList.of();
                  }
                  final Optional<ScalingStats> stats = otherRunner.getScalingStats();
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
    finally {
      startStopStateLock.readLock().unlock();
    }
  }

  public TaskRunner getRunner(Task task)
  {
    try {
      startStopStateLock.readLock().lockInterruptibly();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    try {
      if (!started.get()) {
        throw new IllegalStateException("Management not started");
      }
      final Object tierobj = task.getContextValue(ROUTING_TARGET_CONTEXT_KEY);
      final String tier;
      if (tierobj == null) {
        LOG.debug("No route context found for task [%s]. Using default [%s]", task.getId(), DEFAULT_ROUTE);
        tier = DEFAULT_ROUTE;
      } else {
        tier = tierobj.toString();
      }

      LOG.info("Using tier [%s] for task [%s]", tier, task.getId());
      return runner.getRunnerMap().get(tier);
    }
    finally {
      startStopStateLock.readLock().unlock();
    }
  }
}
