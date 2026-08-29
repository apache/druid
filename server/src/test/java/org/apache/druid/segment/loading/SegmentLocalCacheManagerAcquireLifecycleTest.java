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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.TestSegmentUtils;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lifecycle-focused tests for {@link SegmentLocalCacheManager#acquireSegment}'s hold-handoff choreography: the
 * pre-placed reservation hold must be released exactly once on every path (cancel-while-queued, cancel-mid-load,
 * delivery losing the race with close, empty delivery, normal release).
 */
class SegmentLocalCacheManagerAcquireLifecycleTest
{
  private static final long SEGMENT_SIZE = 1000L;

  /**
   * Per-segment-name gate controlling {@link GatedLoadSpec#loadSegment}. Static because Jackson materializes fresh
   * {@link GatedLoadSpec} instances from the load spec map on every acquire.
   */
  static class Gate
  {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    final CountDownLatch exited = new CountDownLatch(1);
    final AtomicInteger loadCount = new AtomicInteger();
    final AtomicBoolean sawInterrupt = new AtomicBoolean();
    volatile boolean blockUninterruptibly = false;
  }

  private static final Map<String, Gate> GATES = new ConcurrentHashMap<>();

  private static Gate gate(String name)
  {
    return GATES.computeIfAbsent(name, ignored -> new Gate());
  }

  @JsonTypeName("gated")
  public static class GatedLoadSpec implements LoadSpec
  {
    private final int size;
    private final String name;

    @JsonCreator
    public GatedLoadSpec(@JsonProperty("size") int size, @JsonProperty("name") String name)
    {
      this.size = size;
      this.name = name;
    }

    @Override
    public LoadSpecResult loadSegment(File destDir) throws SegmentLoadingException
    {
      final Gate gate = gate(name);
      gate.loadCount.incrementAndGet();
      gate.entered.countDown();
      try {
        if (gate.blockUninterruptibly) {
          Uninterruptibles.awaitUninterruptibly(gate.proceed);
        } else {
          try {
            gate.proceed.await();
          }
          catch (InterruptedException e) {
            gate.sawInterrupt.set(true);
            throw new SegmentLoadingException(e, "interrupted while loading[%s]", name);
          }
        }
        return new TestSegmentUtils.TestLoadSpec(size, name).loadSegment(destDir);
      }
      finally {
        gate.exited.countDown();
      }
    }
  }

  /**
   * Executor that holds every submitted task instead of running it, until the test calls {@link #dispatchAll()}.
   * Lets tests deterministically construct the submitted-but-not-yet-started state that a real pool (which starts
   * tasks immediately, especially in the virtual-thread mode) cannot guarantee.
   */
  private static class DeferredDispatchExecutorService extends AbstractExecutorService
  {
    private final List<Runnable> held = new ArrayList<>();

    @Override
    public synchronized void execute(Runnable command)
    {
      held.add(command);
    }

    synchronized void dispatchAll()
    {
      // run inline: a cancelled FutureTask's run() is a no-op, so this is safe on the test thread
      for (Runnable runnable : held) {
        runnable.run();
      }
      held.clear();
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public List<Runnable> shutdownNow()
    {
      synchronized (this) {
        final List<Runnable> remaining = new ArrayList<>(held);
        held.clear();
        return remaining;
      }
    }

    @Override
    public boolean isShutdown()
    {
      return false;
    }

    @Override
    public boolean isTerminated()
    {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
    {
      return true;
    }
  }

  @TempDir
  File tempDir;

  private ObjectMapper jsonMapper;
  private SegmentLocalCacheManager manager;
  private StorageLocation location;

  @BeforeEach
  void setUp() throws IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    GATES.clear();
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.registerSubtypes(GatedLoadSpec.class);
    jsonMapper.registerSubtypes(TestSegmentUtils.TestLoadSpec.class);
    jsonMapper.registerSubtypes(TestSegmentUtils.TestSegmentizerFactory.class);

    final StorageLocationConfig locationConfig = new StorageLocationConfig(
        new File(tempDir, "cache"),
        100_000L,
        null
    );
    final SegmentLoaderConfig loaderConfig = SegmentLoaderConfig.builder()
                                                                .locations(locationConfig)
                                                                .virtualStorage(true)
                                                                .virtualStorageLoadThreads(1)
                                                                .infoDir(new File(tempDir, "info"))
                                                                .build();
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    location = storageLocations.get(0);
    manager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        StorageLoadingThreadPool.createFromConfig(loaderConfig),
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestIndex.INDEX_IO,
        jsonMapper
    );
    manager.getCachedSegments();
  }

  @AfterEach
  void tearDown()
  {
    // open all gates so any still-blocked load task can unwind before the executor is torn down
    for (Gate gate : GATES.values()) {
      gate.proceed.countDown();
    }
    manager.shutdown();
  }

  private DataSegment makeSegment(String name)
  {
    return DataSegment.builder()
                      .dataSource("test_ds")
                      .interval(Intervals.of("2024-01-01/2024-01-02"))
                      .version("v1")
                      .loadSpec(ImmutableMap.of("type", "gated", "size", (int) SEGMENT_SIZE, "name", name))
                      .dimensions(ImmutableList.of())
                      .metrics(ImmutableList.of())
                      .shardSpec(new NumberedShardSpec(Integer.parseInt(name.substring(name.length() - 1)), 0))
                      .binaryVersion(9)
                      .size(SEGMENT_SIZE)
                      .build();
  }

  private void awaitNoWeakHolds() throws InterruptedException
  {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (location.getWeakStats().getHoldCount() > 0) {
      if (System.currentTimeMillis() > deadline) {
        Assertions.assertEquals(0, location.getWeakStats().getHoldCount(), "holds did not drain");
      }
      Thread.sleep(5);
    }
  }

  @Test
  void testCloseBeforeTaskRunsReleasesPreplacedHold() throws Exception
  {
    // The submitted-but-not-yet-started state cannot be forced through a real pool (virtual-thread mode starts tasks
    // immediately), so this test uses a dedicated manager whose pool defers task dispatch until the test releases it.
    // Everything up to the dispatch is then synchronous and deterministic.
    final DeferredDispatchExecutorService deferredExec = new DeferredDispatchExecutorService();
    final StorageLocationConfig deferredLocationConfig = new StorageLocationConfig(
        new File(tempDir, "cache-deferred"),
        100_000L,
        null
    );
    final SegmentLoaderConfig deferredLoaderConfig = SegmentLoaderConfig.builder()
                                                                        .locations(deferredLocationConfig)
                                                                        .virtualStorage(true)
                                                                        .virtualStorageLoadThreads(1)
                                                                        .infoDir(new File(tempDir, "info-deferred"))
                                                                        .build();
    final List<StorageLocation> deferredLocations = deferredLoaderConfig.toStorageLocations();
    final StorageLocation deferredLocation = deferredLocations.get(0);
    final SegmentLocalCacheManager deferredManager = new SegmentLocalCacheManager(
        deferredLocations,
        deferredLoaderConfig,
        new StorageLoadingThreadPool(MoreExecutors.listeningDecorator(deferredExec), new Semaphore(1)),
        new LeastBytesUsedStorageLocationSelectorStrategy(deferredLocations),
        TestIndex.INDEX_IO,
        jsonMapper
    );
    try {
      final DataSegment blocked = makeSegment("queued1");

      // acquire places the hold and submits the load task, but the deferred executor never dispatches it
      final AcquireSegmentAction blockedAction = deferredManager.acquireSegment(blocked, AcquireMode.FULL);
      Assertions.assertEquals(1, deferredLocation.getWeakStats().getHoldCount());

      // close before the task can run: the canceler cancels the queued task, claims the pre-placed hold, and
      // releases it synchronously; the hold release removes the never-mounted weak entry from the cache
      blockedAction.close();
      Assertions.assertEquals(0, deferredLocation.getWeakStats().getHoldCount());
      Assertions.assertNull(
          deferredLocation.getCacheEntry(new SegmentCacheEntryIdentifier(blocked.getId())),
          "never-mounted weak entry must be removed when the pre-placed hold is released"
      );

      // dispatching the (cancelled) task afterwards must be a no-op: the load never runs
      deferredExec.dispatchAll();
      Assertions.assertEquals(0, gate("queued1").loadCount.get(), "cancelled queued load must never have started");
      Assertions.assertEquals(0, deferredLocation.getWeakStats().getHoldCount());
    }
    finally {
      deferredManager.shutdown();
    }
  }

  @Test
  void testCloseDuringLoadDoesNotInterruptRunningTaskAndReleasesHoldsWhenItFinishes() throws Exception
  {
    final DataSegment segment = makeSegment("running0");
    final AcquireSegmentAction action = manager.acquireSegment(segment, AcquireMode.FULL);
    Assertions.assertTrue(gate("running0").entered.await(10, TimeUnit.SECONDS));

    // close mid-load: cancel(false) must NOT interrupt a running task (interrupting mid-NIO / a deduped mount is
    // unsafe). The task keeps running.
    action.close();

    // let the load finish; it must not have observed an interrupt
    gate("running0").proceed.countDown();
    Assertions.assertTrue(gate("running0").exited.await(10, TimeUnit.SECONDS));
    Assertions.assertFalse(gate("running0").sawInterrupt.get(), "a running load must not be interrupted by close");

    // the finished task loses the set() race and closes its own orphaned result, releasing every hold
    awaitNoWeakHolds();
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(segment.getId());
    final long deadline = System.currentTimeMillis() + 10_000;
    while (location.getCacheEntry(id) != null && System.currentTimeMillis() < deadline) {
      location.removeUnheldWeakEntry(id);
      Thread.sleep(5);
    }
    Assertions.assertNull(location.getCacheEntry(id), "orphan-closed entry must be unheld and evictable");
  }

  @Test
  void testSetLosesRaceWithCloseClosesOrphanedResult() throws Exception
  {
    final DataSegment segment = makeSegment("orphan0");
    gate("orphan0").blockUninterruptibly = true;

    final AcquireSegmentAction action = manager.acquireSegment(segment, AcquireMode.FULL);
    Assertions.assertTrue(gate("orphan0").entered.await(10, TimeUnit.SECONDS));

    // close does not stop the running load; the task completes and loses the delivery race with close
    action.close();
    gate("orphan0").proceed.countDown();
    Assertions.assertTrue(gate("orphan0").exited.await(10, TimeUnit.SECONDS));

    // set() returns false to the producer, which closes the orphaned result: segment reference + hold released
    awaitNoWeakHolds();

    // with no outstanding hold or reference, the mounted weak entry is now evictable
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(segment.getId());
    final long deadline = System.currentTimeMillis() + 10_000;
    while (location.getCacheEntry(id) != null && System.currentTimeMillis() < deadline) {
      location.removeUnheldWeakEntry(id);
      Thread.sleep(5);
    }
    Assertions.assertNull(location.getCacheEntry(id), "orphan-closed entry must be unheld and evictable");
  }

  @Test
  void testReleaseVersusCloseMatrix() throws Exception
  {
    final DataSegment segment = makeSegment("matrix0");
    gate("matrix0").proceed.countDown();

    // on-demand load, normal release: close after release is a no-op
    final AcquireSegmentAction action = manager.acquireSegment(segment, AcquireMode.FULL);
    action.await();
    final AcquireSegmentResult result = action.release();
    action.close();
    Assertions.assertThrows(DruidException.class, action::close, "double close must throw");
    try (Segment theSegment = result.getSegment().orElseThrow()) {
      Assertions.assertEquals(segment.getId(), theSegment.getId());
    }
    awaitNoWeakHolds();

    // second acquire hits the mounted fast path (completed action, no executor hop): close without release closes
    // the delivered result, releasing reference + hold
    final AcquireSegmentAction fastPath = manager.acquireSegment(segment, AcquireMode.FULL);
    Assertions.assertTrue(fastPath.isReady());
    Assertions.assertTrue(location.getWeakStats().getHoldCount() > 0);
    fastPath.close();
    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    Assertions.assertThrows(DruidException.class, fastPath::release, "release after close must throw");
  }

  @Test
  void testMissingSegmentDeliversEmptyWithoutHolds()
  {
    final AcquireSegmentAction action = AcquireSegmentAction.missingSegment();
    Assertions.assertTrue(action.isReady());
    final AcquireSegmentResult result = action.release();
    Assertions.assertTrue(result.getSegment().isEmpty());
    result.close();
    action.close();
  }

  @Test
  void testEmptyDeliveryFromFoldClosesExtraOnClose()
  {
    // unit-level coverage of the fold contract: acquireReference(extraOnClose) owns the extra closeable on a miss
    final AtomicInteger closes = new AtomicInteger();
    final Closeable hold = closes::incrementAndGet;
    final SegmentCacheEntry unmounted = new SegmentCacheEntry()
    {
      @Override
      public SegmentId getSegmentId()
      {
        return makeSegment("empty0").getId();
      }

      @Override
      public Optional<Segment> acquireReference()
      {
        return Optional.empty();
      }

      @Override
      public void setOnUnmount(Runnable hook)
      {
      }

      @Override
      public boolean isFullyDownloaded()
      {
        return false;
      }

      @Override
      public CacheEntryIdentifier getId()
      {
        return new SegmentCacheEntryIdentifier(getSegmentId());
      }

      @Override
      public long getSize()
      {
        return 0;
      }

      @Override
      public boolean isMounted()
      {
        return false;
      }

      @Override
      public void mount(StorageLocation location)
      {
      }

      @Override
      public void unmount()
      {
      }
    };
    Assertions.assertTrue(unmounted.acquireReference(hold).isEmpty());
    Assertions.assertEquals(1, closes.get(), "fold must close extraOnClose on a miss");
  }

  @Test
  void testOnDemandLoadMetrics() throws Exception
  {
    final DataSegment segment = makeSegment("metrics0");
    gate("metrics0").proceed.countDown();

    final AcquireSegmentAction action = manager.acquireSegment(segment, AcquireMode.FULL);
    action.await();
    final AcquireSegmentResult result = action.release();
    Assertions.assertEquals(SEGMENT_SIZE, result.getLoadSizeBytes());
    Assertions.assertTrue(result.getLoadTimeNanos() > 0, "on-demand load must report load time");
    Assertions.assertTrue(result.getWaitTimeNanos() >= 0);
    result.close();
    awaitNoWeakHolds();

    // mounted fast path reports zero metrics
    final AcquireSegmentAction fastPath = manager.acquireSegment(segment, AcquireMode.FULL);
    Assertions.assertTrue(fastPath.isReady());
    final AcquireSegmentResult fastResult = fastPath.release();
    Assertions.assertEquals(0, fastResult.getLoadSizeBytes());
    Assertions.assertEquals(0, fastResult.getLoadTimeNanos());
    Assertions.assertEquals(0, fastResult.getWaitTimeNanos());
    fastResult.close();
    fastPath.close();
  }
}
