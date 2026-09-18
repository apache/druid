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

package org.apache.druid.java.util.http.client.pool;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The contract every {@link ResourcePool.Implementation} owes its callers, followed by the promises each
 * implementation makes on its own.
 */
public class ResourcePoolTest
{
  private static final long NEVER_EXPIRES = TimeUnit.MINUTES.toMillis(5);
  private static final long EXPIRES_QUICKLY = 100;

  private TestResourceFactory factory;

  @BeforeEach
  public void setUp()
  {
    factory = new TestResourceFactory();
  }

  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testReturnedResourceIsReused(ResourcePool.Implementation implementation)
  {
    final ResourcePool<String, String> pool = createPool(implementation, 2, NEVER_EXPIRES, false);

    final ResourceContainer<String> first = pool.take("billy");
    Assertions.assertEquals("billy#0", first.get());
    first.returnResource();

    final ResourceContainer<String> second = pool.take("billy");
    Assertions.assertEquals("billy#0", second.get(), "a returned resource is handed out again");
    second.returnResource();

    Assertions.assertEquals(List.of("billy#0"), factory.opened(), "no second connection is opened");
    Assertions.assertEquals(List.of(), factory.closed());
  }

  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testEagerInitializationFillsAKeyOnItsFirstTake(ResourcePool.Implementation implementation)
  {
    final ResourcePool<String, String> pool = createPool(implementation, 2, NEVER_EXPIRES, true);
    Assertions.assertEquals(List.of(), factory.opened(), "an untouched key costs nothing");

    final ResourceContainer<String> billy = pool.take("billy");
    Assertions.assertEquals(List.of("billy#0", "billy#1"), factory.opened());
    Assertions.assertEquals("billy#0", billy.get());
    billy.returnResource();
  }

  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testTakeBlocksWhileEveryResourceIsLent(ResourcePool.Implementation implementation) throws Exception
  {
    final ResourcePool<String, String> pool = createPool(implementation, 2, NEVER_EXPIRES, false);

    final ResourceContainer<String> first = pool.take("billy");
    final ResourceContainer<String> second = pool.take("billy");

    final BackgroundTake third = BackgroundTake.start(pool, "billy");
    Assertions.assertTrue(third.isBlocked(), "a third take while both resources are lent");

    first.returnResource();
    Assertions.assertEquals("billy#0", third.awaitResource(), "the returned resource unblocks the waiting take");
    third.release();
    second.returnResource();

    Assertions.assertEquals(2, factory.opened().size(), "the pool never opens more than maxPerKey");
  }

  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testUnhealthyResourceIsClosedAndReplaced(ResourcePool.Implementation implementation)
  {
    final ResourcePool<String, String> pool = createPool(implementation, 2, NEVER_EXPIRES, false);
    pool.take("billy").returnResource();

    factory.markUnhealthy("billy#0");

    final ResourceContainer<String> replacement = pool.take("billy");
    Assertions.assertEquals("billy#1", replacement.get());
    Assertions.assertEquals(List.of("billy#0"), factory.closed());
    replacement.returnResource();
  }

  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testExpiredResourceIsDiscardedWithoutTouchingOtherKeys(ResourcePool.Implementation implementation)
      throws Exception
  {
    final ResourcePool<String, String> pool = createPool(implementation, 2, EXPIRES_QUICKLY, false);
    pool.take("billy").returnResource();
    awaitExpiry();
    pool.take("sally").returnResource();

    final ResourceContainer<String> billy = pool.take("billy");
    Assertions.assertEquals("billy#1", billy.get(), "the expired resource must not be handed out");
    billy.returnResource();

    final ResourceContainer<String> sally = pool.take("sally");
    Assertions.assertEquals("sally#0", sally.get(), "expiry of one key must not touch another key");
    sally.returnResource();

    Assertions.assertEquals(List.of("billy#0"), factory.closed());
  }

  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testCloseDiscardsIdleResourcesAndRefusesFurtherTakes(ResourcePool.Implementation implementation)
  {
    final ResourcePool<String, String> pool = createPool(implementation, 2, NEVER_EXPIRES, false);
    pool.take("billy").returnResource();

    pool.close();

    Assertions.assertEquals(List.of("billy#0"), factory.closed());
    Assertions.assertNull(pool.take("billy"), "take after close");
  }

  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testCloseUnblocksAWaitingTake(ResourcePool.Implementation implementation) throws Exception
  {
    final ResourcePool<String, String> pool = createPool(implementation, 1, NEVER_EXPIRES, false);
    final ResourceContainer<String> lent = pool.take("billy");

    final BackgroundTake waiter = BackgroundTake.start(pool, "billy");
    Assertions.assertTrue(waiter.isBlocked(), "a take while the only resource is lent");

    pool.close();
    Assertions.assertNull(waiter.awaitResource(), "a take unblocked by close() hands out nothing");
    waiter.release();

    lent.returnResource();
    Assertions.assertEquals(List.of("billy#0"), factory.closed(), "a resource returned after close is closed");
  }

  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testReturningTwiceRepaysOneSlotOnly(ResourcePool.Implementation implementation) throws Exception
  {
    final ResourcePool<String, String> pool = createPool(implementation, 1, NEVER_EXPIRES, false);
    final ResourceContainer<String> billy = pool.take("billy");
    billy.returnResource();
    billy.returnResource();

    Assertions.assertThrows(IllegalStateException.class, billy::get, "a returned container cannot be read again");

    final ResourceContainer<String> reused = pool.take("billy");
    final BackgroundTake extra = BackgroundTake.start(pool, "billy");
    Assertions.assertTrue(extra.isBlocked(), "the second return must not have created a second slot");

    reused.returnResource();
    Assertions.assertEquals("billy#0", extra.awaitResource());
    extra.release();
  }

  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testFailedGenerateDoesNotConsumeASlot(ResourcePool.Implementation implementation) throws Exception
  {
    final ResourcePool<String, String> pool = createPool(implementation, 1, NEVER_EXPIRES, false);
    factory.failGenerate(new ISE("no more billies"));

    Assertions.assertThrows(ISE.class, () -> pool.take("billy"));

    factory.healGenerate();
    final BackgroundTake next = BackgroundTake.start(pool, "billy");
    Assertions.assertEquals("billy#0", next.awaitResource(), "the failed take must give its slot back");
    next.release();
  }

  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testFailedHealthCheckDoesNotConsumeASlot(ResourcePool.Implementation implementation) throws Exception
  {
    final ResourcePool<String, String> pool = createPool(implementation, 1, NEVER_EXPIRES, false);
    pool.take("billy").returnResource();
    factory.failHealthCheck("billy#0", new ISE("health check blew up"));

    Assertions.assertThrows(ISE.class, () -> pool.take("billy"));

    final BackgroundTake next = BackgroundTake.start(pool, "billy");
    Assertions.assertEquals("billy#1", next.awaitResource(), "the failed take must give its slot back");
    next.release();
  }

  /**
   * Under contention, with expiry constantly evicting resources, the pool never lends one resource to two callers,
   * never exceeds maxPerKey, never hands out a closed resource and never loses one.
   */
  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testConcurrentTakersNeverShareAResource(ResourcePool.Implementation implementation) throws Exception
  {
    final int maxPerKey = 4;
    final int takers = 8;
    final ResourcePool<String, String> pool = createPool(implementation, maxPerKey, 5, false);

    final Set<String> lent = ConcurrentHashMap.newKeySet();
    final AtomicInteger lentCount = new AtomicInteger();
    final AtomicReference<String> misuse = new AtomicReference<>();

    final ExecutorService exec = Execs.multiThreaded(takers, "resource-pool-stress-%d");
    final List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < takers; i++) {
      futures.add(exec.submit(() -> {
        for (int j = 0; j < 200; j++) {
          final ResourceContainer<String> container = pool.take("billy");
          final String resource = container.get();
          if (!lent.add(resource)) {
            misuse.compareAndSet(null, "lent to two takers at once: " + resource);
          }
          if (lentCount.incrementAndGet() > maxPerKey) {
            misuse.compareAndSet(null, "more than " + maxPerKey + " lent at once");
          }
          if (factory.isClosed(resource)) {
            misuse.compareAndSet(null, "lent after being closed: " + resource);
          }
          lentCount.decrementAndGet();
          lent.remove(resource);
          container.returnResource();
        }
      }));
    }
    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    exec.shutdown();
    pool.close();

    Assertions.assertNull(misuse.get());
    Assertions.assertEquals(Set.copyOf(factory.opened()), Set.copyOf(factory.closed()), "every resource is closed");
    Assertions.assertEquals(factory.opened().size(), factory.closed().size(), "no resource is closed twice");
  }

  /**
   * One take() discards every expired resource it walks past and opens a single replacement, so a pool that ran hot
   * shrinks back to what the traffic needs instead of re-handshaking up to maxPerKey one take at a time.
   */
  @Test
  public void testAdaptivePurgesEveryExpiredResourceInOneTake() throws Exception
  {
    final ResourcePool<String, String> pool = createPool(ResourcePool.Implementation.ADAPTIVE, 3, EXPIRES_QUICKLY, true);
    pool.take("billy").returnResource();
    awaitExpiry();

    final ResourceContainer<String> fresh = pool.take("billy");
    Assertions.assertEquals("billy#3", fresh.get());
    fresh.returnResource();

    Assertions.assertEquals(Set.of("billy#0", "billy#1", "billy#2"), Set.copyOf(factory.closed()));
    Assertions.assertEquals(4, factory.opened().size(), "exactly one replacement is opened");
  }

  /**
   * A peer restart kills every pooled resource at once, long before any of them expires. One take() must clear them
   * all rather than discovering them one take - and one handshake on a caller's thread - at a time.
   */
  @Test
  public void testAdaptivePurgesEveryDeadResourceInOneTake()
  {
    final ResourcePool<String, String> pool = createPool(ResourcePool.Implementation.ADAPTIVE, 3, NEVER_EXPIRES, true);
    pool.take("billy").returnResource();
    factory.markUnhealthy("billy#0", "billy#1", "billy#2");

    final ResourceContainer<String> fresh = pool.take("billy");
    Assertions.assertEquals("billy#3", fresh.get());
    fresh.returnResource();

    Assertions.assertEquals(Set.of("billy#0", "billy#1", "billy#2"), Set.copyOf(factory.closed()));
  }

  /**
   * A resource that broke while it was lent out is closed on its way back instead of being parked for the next
   * taker, whose health check would only discover it a moment later.
   */
  @Test
  public void testAdaptiveDiscardsABrokenResourceOnGiveBack()
  {
    final ResourcePool<String, String> pool = createPool(ResourcePool.Implementation.ADAPTIVE, 1, NEVER_EXPIRES, false);
    final ResourceContainer<String> lent = pool.take("billy");
    factory.markUnhealthy("billy#0");

    lent.returnResource();
    Assertions.assertEquals(List.of("billy#0"), factory.closed());

    final ResourceContainer<String> fresh = pool.take("billy");
    Assertions.assertEquals("billy#1", fresh.get(), "the discarded resource repaid its slot");
    fresh.returnResource();
  }

  /**
   * A creation that arrives broken is retried, so a caller does not pay for a peer that was briefly unreachable.
   */
  @Test
  public void testAdaptiveRetriesUntilACreatedResourceIsGood()
  {
    final ResourcePool<String, String> pool = createPool(ResourcePool.Implementation.ADAPTIVE, 2, NEVER_EXPIRES, false);
    factory.markUnhealthy("billy#0", "billy#1");

    final ResourceContainer<String> fresh = pool.take("billy");
    Assertions.assertEquals("billy#2", fresh.get());
    fresh.returnResource();

    Assertions.assertEquals(List.of("billy#0", "billy#1"), factory.closed());
  }

  /**
   * With every attempt broken the caller still gets the last one, as it always did, rather than a failing take.
   */
  @Test
  public void testAdaptiveHandsOverAPossiblyBadResourceWhenEveryAttemptFails()
  {
    final ResourcePool<String, String> pool = createPool(ResourcePool.Implementation.ADAPTIVE, 2, NEVER_EXPIRES, false);
    factory.markUnhealthy("billy#0", "billy#1", "billy#2");

    final ResourceContainer<String> lent = pool.take("billy");
    Assertions.assertEquals("billy#2", lent.get());
    Assertions.assertEquals(List.of("billy#0", "billy#1"), factory.closed(), "the handed over resource stays open");

    lent.returnResource();
    Assertions.assertTrue(factory.isClosed("billy#2"), "and is discarded once it comes back");
  }

  /**
   * Strict validation turns that last possibly bad resource into a failure, and the failed take still repays its
   * slot - the next take must not have to wait for it.
   */
  @Test
  public void testAdaptiveStrictValidationFailsTheTake()
  {
    final ResourcePool<String, String> pool =
        createPool(ResourcePool.Implementation.ADAPTIVE, 1, NEVER_EXPIRES, false, true);
    factory.markUnhealthy("billy#0", "billy#1", "billy#2");

    Assertions.assertThrows(ISE.class, () -> pool.take("billy"));
    Assertions.assertEquals(List.of("billy#0", "billy#1", "billy#2"), factory.closed(), "no attempt is leaked");

    final ResourceContainer<String> fresh = pool.take("billy");
    Assertions.assertEquals("billy#3", fresh.get());
    fresh.returnResource();
  }

  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testStatsRecordWhatHappenedToTheResourcesOfAKey(ResourcePool.Implementation implementation)
      throws Exception
  {
    final ResourcePool<String, String> pool = createPool(implementation, 1, EXPIRES_QUICKLY, false);
    pool.take("billy").returnResource();
    awaitExpiry();
    pool.take("billy").returnResource();
    pool.take("sally").returnResource();

    final Map<String, ResourcePool.Stats> stats = pool.drainStats();
    Assertions.assertEquals(Set.of("billy", "sally"), stats.keySet(), "one entry per key");

    final ResourcePool.Stats billy = stats.get("billy");
    Assertions.assertEquals(2, billy.opened(), "opened");
    Assertions.assertEquals(1, billy.closed(), "closed");
    Assertions.assertEquals(1, billy.timedOut(), "timed out");
    Assertions.assertEquals(0, billy.errored(), "errored");
    Assertions.assertEquals(2, billy.taken(), "taken");
    Assertions.assertEquals(2, billy.returned(), "returned");
    Assertions.assertEquals(0, billy.used(), "nothing is lent out");
    Assertions.assertEquals(1, billy.idle(), "the returned resource waits for the next caller");

    Assertions.assertEquals(1, stats.get("sally").opened(), "keys are counted apart");
  }

  /**
   * The counted events are drained as they are read, so an idle key reports zeroes rather than the totals it
   * reported before, while what it holds keeps being reported.
   */
  @ParameterizedTest
  @EnumSource(ResourcePool.Implementation.class)
  public void testDrainedStatsCoverOnlyWhatHappenedSinceThePreviousDrain(ResourcePool.Implementation implementation)
  {
    final ResourcePool<String, String> pool = createPool(implementation, 2, NEVER_EXPIRES, false);
    final ResourceContainer<String> lent = pool.take("billy");
    pool.drainStats();

    final ResourcePool.Stats idle = pool.drainStats().get("billy");
    Assertions.assertEquals(0, idle.opened(), "opened");
    Assertions.assertEquals(0, idle.taken(), "taken");
    Assertions.assertEquals(1, idle.used(), "the lent resource is still out");
    Assertions.assertEquals(0, idle.idle(), "and is not parked");

    lent.returnResource();
    final ResourcePool.Stats afterReturn = pool.drainStats().get("billy");
    Assertions.assertEquals(1, afterReturn.returned(), "returned");
    Assertions.assertEquals(0, afterReturn.used(), "used");
    Assertions.assertEquals(1, afterReturn.idle(), "idle");
  }

  @Test
  public void testStatsRecordAFailingHealthCheckAsAnError()
  {
    final ResourcePool<String, String> pool = createPool(ResourcePool.Implementation.ADAPTIVE, 2, NEVER_EXPIRES, false);
    pool.take("billy").returnResource();
    factory.failHealthCheck("billy#0", new ISE("health check blew up"));

    Assertions.assertThrows(ISE.class, () -> pool.take("billy"));

    final ResourcePool.Stats stats = pool.drainStats().get("billy");
    Assertions.assertEquals(1, stats.errored(), "errored");
    Assertions.assertEquals(1, stats.closed(), "the resource whose check threw is closed");
  }

  /**
   * A resource taken off the queue and handed to nobody is closed rather than leaked.
   */
  @Test
  public void testAdaptiveClosesTheResourceWhoseHealthCheckThrows()
  {
    final ResourcePool<String, String> pool = createPool(ResourcePool.Implementation.ADAPTIVE, 2, NEVER_EXPIRES, false);
    pool.take("billy").returnResource();
    factory.failHealthCheck("billy#0", new ISE("health check blew up"));

    Assertions.assertThrows(ISE.class, () -> pool.take("billy"));
    Assertions.assertEquals(List.of("billy#0"), factory.closed());
  }

  /**
   * A resource that fails to close while being evicted does not fail the take that evicted it, and does not strand
   * the resources queued behind it.
   */
  @Test
  public void testAdaptiveSurvivesAFailingClose() throws Exception
  {
    final ResourcePool<String, String> pool = createPool(ResourcePool.Implementation.ADAPTIVE, 2, EXPIRES_QUICKLY, true);
    pool.take("billy").returnResource();
    awaitExpiry();
    // billy#1 is at the head of the queue, so it is the first one evicted.
    factory.failClose("billy#1", new ISE("close blew up"));

    final ResourceContainer<String> fresh = pool.take("billy");
    Assertions.assertEquals("billy#2", fresh.get());
    fresh.returnResource();

    Assertions.assertEquals(Set.of("billy#0", "billy#1"), Set.copyOf(factory.closed()));
  }

  /**
   * Resources opened before a failing eager initialization are closed - the half-built holder never reaches the
   * cache, so nothing can ever reach them again.
   */
  @Test
  public void testAdaptiveClosesResourcesOpenedBeforeAFailedEagerInitialization()
  {
    final ResourcePool<String, String> pool = createPool(ResourcePool.Implementation.ADAPTIVE, 3, NEVER_EXPIRES, true);
    factory.failGenerateAfter(2, new ISE("no more billies"));

    final Exception thrown = Assertions.assertThrows(UncheckedExecutionException.class, () -> pool.take("billy"));
    Assertions.assertEquals("no more billies", thrown.getCause().getMessage());
    Assertions.assertEquals(List.of("billy#0", "billy#1"), factory.closed());
  }

  /**
   * A null from {@link ResourceFactory#generate} fails the take instead of producing a container that blows up on
   * {@link ResourceContainer#returnResource} and never repays its lent slot.
   */
  @Test
  public void testAdaptiveRejectsANullResource()
  {
    final ResourcePool<String, String> pool = createPool(ResourcePool.Implementation.ADAPTIVE, 2, NEVER_EXPIRES, false);
    factory.generateNull();

    Assertions.assertThrows(NullPointerException.class, () -> pool.take("billy"));
  }

  /**
   * The retaining pool keeps its high-water mark: an expired resource is replaced one for one, and the resources
   * queued behind it stay parked until their own turn comes.
   */
  @Test
  public void testRetainingReplacesAnExpiredResourceOneForOne() throws Exception
  {
    final ResourcePool<String, String> pool =
        createPool(ResourcePool.Implementation.RETAINING, 2, EXPIRES_QUICKLY, true);
    pool.take("billy").returnResource();
    awaitExpiry();

    final ResourceContainer<String> fresh = pool.take("billy");
    Assertions.assertEquals("billy#2", fresh.get());
    fresh.returnResource();

    Assertions.assertEquals(List.of("billy#1"), factory.closed(), "only the resource at the head is discarded");
    Assertions.assertEquals(3, factory.opened().size(), "the expired resource is replaced one for one");
  }

  private ResourcePool<String, String> createPool(
      ResourcePool.Implementation implementation,
      int maxPerKey,
      long unusedResourceTimeoutMillis,
      boolean eagerInitialization
  )
  {
    return createPool(implementation, maxPerKey, unusedResourceTimeoutMillis, eagerInitialization, false);
  }

  private ResourcePool<String, String> createPool(
      ResourcePool.Implementation implementation,
      int maxPerKey,
      long unusedResourceTimeoutMillis,
      boolean eagerInitialization,
      boolean strictConnectionValidation
  )
  {
    return new ResourcePool<>(
        factory,
        new ResourcePoolConfig(maxPerKey, unusedResourceTimeoutMillis, implementation, strictConnectionValidation),
        eagerInitialization
    );
  }

  /**
   * Waits until every idle resource of an {@link #EXPIRES_QUICKLY} pool has gone stale.
   */
  private static void awaitExpiry() throws InterruptedException
  {
    Thread.sleep(EXPIRES_QUICKLY * 3);
  }

  /**
   * A take() running on its own thread, so a test can watch it block and decide when it gives its resource back.
   */
  private static class BackgroundTake
  {
    private final CountDownLatch taken = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);
    private final Thread thread;

    private volatile ResourceContainer<String> container;
    private volatile Throwable failure;

    static BackgroundTake start(ResourcePool<String, String> pool, String key)
    {
      return new BackgroundTake(pool, key);
    }

    private BackgroundTake(ResourcePool<String, String> pool, String key)
    {
      this.thread = new Thread(
          () -> {
            try {
              container = pool.take(key);
              taken.countDown();
              released.await();
              if (container != null) {
                container.returnResource();
              }
            }
            catch (Throwable t) {
              failure = t;
            }
            finally {
              taken.countDown();
            }
          },
          "background-take-" + key
      );
      thread.setDaemon(true);
      thread.start();
    }

    boolean isBlocked() throws InterruptedException
    {
      return !taken.await(200, TimeUnit.MILLISECONDS);
    }

    /**
     * The resource taken, or null if the pool handed out nothing. Fails if the take never completes - which is what
     * a leaked slot looks like, since the pool blocks once every slot is handed out.
     */
    @Nullable
    String awaitResource() throws InterruptedException
    {
      Assertions.assertTrue(taken.await(5, TimeUnit.SECONDS), "the take completed");
      return container == null ? null : container.get();
    }

    /**
     * Gives the resource back and asserts that neither taking nor returning threw.
     */
    void release() throws InterruptedException
    {
      released.countDown();
      thread.join(TimeUnit.SECONDS.toMillis(5));
      Assertions.assertNull(failure, "the background take failed");
    }
  }

  /**
   * Hands out resources named {@code key#n} and records what the pool did with them. Faults are injected per
   * resource, so a test names the resource it wants broken rather than scripting a sequence of calls.
   */
  private static class TestResourceFactory implements ResourceFactory<String, String>
  {
    private final ConcurrentMap<String, AtomicInteger> sequences = new ConcurrentHashMap<>();
    private final List<String> opened = Collections.synchronizedList(new ArrayList<>());
    private final List<String> closed = Collections.synchronizedList(new ArrayList<>());
    private final Set<String> unhealthy = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, RuntimeException> healthCheckFaults = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, RuntimeException> closeFaults = new ConcurrentHashMap<>();
    private final AtomicInteger generatesBeforeFault = new AtomicInteger();

    private volatile RuntimeException generateFault;
    private volatile boolean generateNull;

    @Override
    public String generate(String key)
    {
      if (generateFault != null && generatesBeforeFault.getAndDecrement() <= 0) {
        throw generateFault;
      }
      if (generateNull) {
        return null;
      }
      final String resource = key + "#" + sequences.computeIfAbsent(key, k -> new AtomicInteger()).getAndIncrement();
      opened.add(resource);
      return resource;
    }

    @Override
    public boolean isGood(String resource)
    {
      final RuntimeException fault = healthCheckFaults.get(resource);
      if (fault != null) {
        throw fault;
      }
      return !unhealthy.contains(resource);
    }

    @Override
    public void close(String resource)
    {
      closed.add(resource);
      final RuntimeException fault = closeFaults.get(resource);
      if (fault != null) {
        throw fault;
      }
    }

    /**
     * Every resource ever opened, in the order they were opened.
     */
    List<String> opened()
    {
      return List.copyOf(opened);
    }

    /**
     * Every close, in the order they happened - a resource closed twice appears twice.
     */
    List<String> closed()
    {
      return List.copyOf(closed);
    }

    boolean isClosed(String resource)
    {
      return closed.contains(resource);
    }

    void markUnhealthy(String... resources)
    {
      unhealthy.addAll(List.of(resources));
    }

    void failHealthCheck(String resource, RuntimeException fault)
    {
      healthCheckFaults.put(resource, fault);
    }

    void failClose(String resource, RuntimeException fault)
    {
      closeFaults.put(resource, fault);
    }

    void failGenerate(RuntimeException fault)
    {
      failGenerateAfter(0, fault);
    }

    void failGenerateAfter(int successfulGenerates, RuntimeException fault)
    {
      generatesBeforeFault.set(successfulGenerates);
      generateFault = fault;
    }

    void healGenerate()
    {
      generateFault = null;
    }

    void generateNull()
    {
      generateNull = true;
    }
  }
}
