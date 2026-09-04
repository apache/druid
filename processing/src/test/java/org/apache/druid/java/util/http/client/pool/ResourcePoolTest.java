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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 */
@RunWith(Parameterized.class)
public class ResourcePoolTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return Arrays.stream(ResourcePool.Implementation.values())
                 .map(implementation -> new Object[]{implementation})
                 .collect(Collectors.toList());
  }

  private final ResourcePool.Implementation poolImplementation;

  ResourceFactory<String, String> resourceFactory;
  ResourcePool<String, String> pool;

  public ResourcePoolTest(ResourcePool.Implementation poolImplementation)
  {
    this.poolImplementation = poolImplementation;
  }

  @Before
  public void setUp()
  {
    setUpPool(true);
  }

  public void setUpPoolWithoutEagerInitialization()
  {
    setUpPool(false);
  }

  public void setUpPool(boolean eagerInitialization)
  {
    resourceFactory = (ResourceFactory<String, String>) EasyMock.createMock(ResourceFactory.class);

    EasyMock.replay(resourceFactory);
    pool = createPool(2, TimeUnit.MINUTES.toMillis(4), eagerInitialization);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  private ResourcePool<String, String> createPool(
      int maxPerKey,
      long unusedConnectionTimeoutMillis,
      boolean eagerInitialization
  )
  {
    return new ResourcePool<>(
        resourceFactory,
        new ResourcePoolConfig(maxPerKey, unusedConnectionTimeoutMillis, poolImplementation),
        eagerInitialization
    );
  }

  private boolean isAdaptivePool()
  {
    return poolImplementation == ResourcePool.Implementation.ADAPTIVE;
  }

  /**
   * Skips a test that pins behaviour only {@link ResourcePool.Implementation#ADAPTIVE} provides.
   */
  private void assumeAdaptivePool()
  {
    Assume.assumeTrue("only the adaptive pool satisfies this", isAdaptivePool());
  }

  @Test
  public void testSanity()
  {
    primePool();
    EasyMock.replay(resourceFactory);
  }

  @Test
  public void testTakeOnce_lazy()
  {
    setUpPoolWithoutEagerInitialization();

    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billyString = pool.take("billy");
    Assert.assertEquals("billy0", billyString.get());

    billyString.returnResource();
  }

  @Test
  public void testTakeAfterReturn_lazy()
  {
    setUpPoolWithoutEagerInitialization();

    // Generate and check before return
    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    // Only check since there's no need to generate after return
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billyString = pool.take("billy");
    Assert.assertEquals("billy0", billyString.get());

    billyString.returnResource();

    billyString = pool.take("billy");
    Assert.assertEquals("billy0", billyString.get());

    billyString.returnResource();
  }

  @Test
  public void testTakeAfterFailure()
  {
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy0");
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy1");

    EasyMock.expect(resourceFactory.isGood("billy0")).andThrow(new RuntimeException("blah"));
    resourceFactory.close("billy0");
    EasyMock.expectLastCall();

    EasyMock.expect(resourceFactory.isGood("billy1")).andThrow(new RuntimeException("blah"));
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();

    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2");
    EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true);

    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy3");
    EasyMock.expect(resourceFactory.isGood("billy3")).andReturn(true);

    EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true);

    EasyMock.expect(resourceFactory.isGood("billy3")).andReturn(true);

    EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true);

    EasyMock.replay(resourceFactory);
    // numLentResources == 0, resourceHolderList.size() == 2

    try {
      pool.take("billy");
    }
    catch (Exception e) {
    }
    // numLentResources == 0, resourceHolderList.size() == 1

    try {
      pool.take("billy");
    }
    catch (Exception e) {
    }
    // numLentResources == 0, resourceHolderList.size() == 0

    ResourceContainer<String> a = pool.take("billy");
    // numLentResources == 1, resourceHolderList.size() == 0

    ResourceContainer<String> b = pool.take("billy");
    // numLentResources == 2, resourceHolderList.size() == 0

    a.returnResource();
    // numLentResources = 1, resourceHolderList.size() == 1

    a = pool.take("billy");
    // numLentResources = 2, resourceHolderList.size() == 0

    b.returnResource();
    // numLentResources = 1, resourceHolderList.size() == 1

    a.returnResource();
    // numLentResources = 0, resourceHolderList.size() == 2
  }

  @Test
  public void testTakeAfterFailure_lazy()
  {
    setUpPoolWithoutEagerInitialization();

    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy0");
    EasyMock.expect(resourceFactory.isGood("billy0")).andThrow(new RuntimeException("blah"));
    resourceFactory.close("billy0");
    EasyMock.expectLastCall();

    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy1");
    EasyMock.expect(resourceFactory.isGood("billy1")).andThrow(new RuntimeException("blah"));
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();

    EasyMock.expect(resourceFactory.generate("billy")).andThrow(new RuntimeException("blah"));

    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy3");
    EasyMock.expect(resourceFactory.isGood("billy3")).andReturn(true);

    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy4");
    EasyMock.expect(resourceFactory.isGood("billy4")).andReturn(true);

    EasyMock.expect(resourceFactory.isGood("billy3")).andReturn(true);

    EasyMock.expect(resourceFactory.isGood("billy4")).andReturn(true);

    EasyMock.expect(resourceFactory.isGood("billy3")).andThrow(new RuntimeException("blah"));
    resourceFactory.close("billy3");
    EasyMock.expectLastCall();

    EasyMock.replay(resourceFactory);
    // numLentResources == 0, resourceHolderList.size() == 0

    try {
      pool.take("billy");
    }
    catch (Exception e) {
    }
    // numLentResources == 0, resourceHolderList.size() == 0

    try {
      pool.take("billy");
    }
    catch (Exception e) {
    }
    // numLentResources == 0, resourceHolderList.size() == 0

    try {
      pool.take("billy");
    }
    catch (Exception e) {
    }
    // numLentResources == 0, resourceHolderList.size() == 0

    ResourceContainer<String> a = pool.take("billy");
    // numLentResources == 1, resourceHolderList.size() == 0

    ResourceContainer<String> b = pool.take("billy");
    // numLentResources == 2, resourceHolderList.size() == 0

    a.returnResource();
    // numLentResources == 1, resourceHolderList.size() == 1

    try {
      pool.take("billy");
    }
    catch (Exception e) {
    }
    // numLentResources = 1, resourceHolderList.size() == 0

    b.returnResource();
    // numLentResources = 0, resourceHolderList.size() == 1
  }

  private void primePool()
  {
    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(2);
    EasyMock.expect(resourceFactory.generate("sally")).andAnswer(new StringIncrementingAnswer("sally")).times(2);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isGood("sally0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billyString = pool.take("billy");
    ResourceContainer<String> sallyString = pool.take("sally");
    Assert.assertEquals("billy0", billyString.get());
    Assert.assertEquals("sally0", sallyString.get());

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    billyString.returnResource();
    sallyString.returnResource();
  }

  @Test
  public void testFailedResource()
  {
    primePool();

    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(false).times(1);
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();
    if (isAdaptivePool()) {
      // The next idle resource is tried before opening a new connection.
      EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    } else {
      EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    }
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals(isAdaptivePool() ? "billy0" : "billy2", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  @Test
  public void testFaultyFailedResourceReplacement()
  {
    primePool();

    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(false).times(1);
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();
    EasyMock.expect(resourceFactory.generate("billy")).andThrow(new ISE("where's billy?")).times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(false).times(1);
    resourceFactory.close("billy0");
    EasyMock.expectLastCall();
    EasyMock.expect(resourceFactory.generate("billy")).andThrow(new ISE("where's billy?")).times(1);
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    IllegalStateException e1 = null;
    try {
      pool.take("billy");
    }
    catch (IllegalStateException e) {
      e1 = e;
    }
    Assert.assertNotNull("exception", e1);
    Assert.assertEquals("where's billy?", e1.getMessage());

    IllegalStateException e2 = null;
    try {
      pool.take("billy");
    }
    catch (IllegalStateException e) {
      e2 = e;
    }
    Assert.assertNotNull("exception", e2);
    Assert.assertEquals("where's billy?", e2.getMessage());

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy2", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  @Test
  public void testTakeMoreThanAllowed() throws Exception
  {
    primePool();
    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    CountDownLatch latch3 = new CountDownLatch(1);

    MyThread billy1Thread = new MyThread(latch1, "billy");
    billy1Thread.start();
    billy1Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    MyThread billy0Thread = new MyThread(latch2, "billy");
    billy0Thread.start();
    billy0Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    MyThread blockedThread = new MyThread(latch3, "billy");
    blockedThread.start();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    latch2.countDown();
    blockedThread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    latch1.countDown();
    latch3.countDown();

    Assert.assertEquals("billy1", billy1Thread.getValue());
    Assert.assertEquals("billy0", billy0Thread.getValue());
    Assert.assertEquals("billy0", blockedThread.getValue());
  }

  @Test
  public void testCloseUnblocks() throws InterruptedException
  {
    primePool();
    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    resourceFactory.close("sally1");
    EasyMock.expectLastCall().times(1);
    resourceFactory.close("sally0");
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(resourceFactory);
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    CountDownLatch latch3 = new CountDownLatch(1);

    MyThread billy1Thread = new MyThread(latch1, "billy");
    billy1Thread.start();
    billy1Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    MyThread billy0Thread = new MyThread(latch2, "billy");
    billy0Thread.start();
    billy0Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    MyThread blockedThread = new MyThread(latch3, "billy");
    blockedThread.start();
    blockedThread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    pool.close();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
    // billy0Thread calling ResourceContainer.returnResource() will result
    // in a call to resourceFactory.close() when latch2 is triggered
    resourceFactory.close("billy0");
    EasyMock.expectLastCall().once();
    EasyMock.replay(resourceFactory);

    latch2.countDown();
    blockedThread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    // wait for billy0Thread to have called resourceFactory.close() to avoid race
    // between billy0Thread calling it and verify() checking for the call
    billy0Thread.join();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    latch1.countDown();
    latch3.countDown();

    Assert.assertEquals("billy1", billy1Thread.getValue());
    Assert.assertEquals("billy0", billy0Thread.getValue());
    blockedThread.join();
    // pool returns null after close
    Assert.assertEquals(null, blockedThread.getValue());
  }

  @Test
  public void testTimedOutResource() throws Exception
  {
    resourceFactory = (ResourceFactory<String, String>) EasyMock.createMock(ResourceFactory.class);

    pool = createPool(2, TimeUnit.MILLISECONDS.toMillis(10), true);

    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(2);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billyString = pool.take("billy");
    Assert.assertEquals("billy0", billyString.get());

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    billyString.returnResource();

    //make sure resources have been timed out.
    Thread.sleep(100);

    if (isAdaptivePool()) {
      // Both parked resources (billy0, billy1) are stale, so a single take() purges both before opening one
      // validated replacement.
      resourceFactory.close("billy0");
      EasyMock.expectLastCall();
      resourceFactory.close("billy1");
      EasyMock.expectLastCall();
      EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
      EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true).times(1);
    } else {
      // Only the resource at the front of the queue is discarded, and it is replaced one for one.
      resourceFactory.close("billy1");
      EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy1").times(1);
    }
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals(isAdaptivePool() ? "billy2" : "billy1", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * A single take() purges every idle resource that has outlived the timeout - not just the one at the front -
   * and opens at most one replacement, letting the pool shrink instead of reconnecting one-for-one. A warm
   * survivor is then reused directly, so no handshake is paid on the caller's thread.
   */
  @Test
  public void testExpiredResourcesArePurgedAndPoolShrinks() throws Exception
  {
    assumeAdaptivePool();
    resourceFactory = (ResourceFactory<String, String>) EasyMock.createMock(ResourceFactory.class);
    pool = createPool(2, TimeUnit.MILLISECONDS.toMillis(100), true);

    // Park two warm resources (billy0, billy1).
    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(2);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> first = pool.take("billy");
    ResourceContainer<String> second = pool.take("billy");
    Assert.assertEquals("billy0", first.get());
    Assert.assertEquals("billy1", second.get());
    first.returnResource();
    second.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    // Let both parked resources go stale.
    Thread.sleep(500);

    // One take() closes BOTH stale resources and opens a single validated replacement - the pool shrinks to the
    // one connection actually needed rather than eagerly re-handshaking back up to maxPerKey.
    resourceFactory.close("billy0");
    EasyMock.expectLastCall();
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> a = pool.take("billy");
    Assert.assertEquals("billy2", a.get());
    a.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    // The lone warm survivor is reused directly - no close(), no generate(), no handshake.
    EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> b = pool.take("billy");
    Assert.assertEquals("billy2", b.get());
    b.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * A single take() discards every parked resource that fails {@link ResourceFactory#isGood}, not only the one at the
   * head of the queue. Otherwise a peer restart - which kills all pooled resources at once, long before any of them
   * expires - is discovered one take() at a time, each paying a fresh handshake on a caller's thread.
   */
  @Test
  public void testDeadResourcesArePurgedInOneTake()
  {
    assumeAdaptivePool();
    primePool();

    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(false).anyTimes();
    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(false).anyTimes();
    resourceFactory.close("billy0");
    EasyMock.expectLastCall();
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true).anyTimes();
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy2", billy.get());
    billy.returnResource();

    // Both dead resources must already be closed: one left parked would not have been.
    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * A resource whose {@link ResourceFactory#isGood} throws is closed rather than dropped on the floor, and the failed
   * take() gives its lent slot back.
   */
  @Test
  public void testResourceIsClosedWhenIsGoodThrows() throws Exception
  {
    assumeAdaptivePool();
    primePool();

    EasyMock.expect(resourceFactory.isGood("billy1")).andThrow(new ISE("health check blew up")).times(1);
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();
    EasyMock.replay(resourceFactory);

    ISE thrown = null;
    try {
      pool.take("billy");
    }
    catch (ISE e) {
      thrown = e;
    }
    Assert.assertNotNull("exception", thrown);

    // billy1 was taken off the queue and handed to nobody; without close() it is leaked.
    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    CountDownLatch heldUntil = new CountDownLatch(1);
    Assert.assertEquals("billy0", takeOnAnotherThread(heldUntil, "billy"));
    Assert.assertEquals("billy2", takeOnAnotherThread(heldUntil, "billy"));
    heldUntil.countDown();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * A resource that fails to close while being evicted for expiry does not fail the take() that evicted it, does not
   * strand the resources queued behind it, and does not consume a lent slot.
   */
  @Test
  public void testCloseFailureWhileEvictingExpiredResources() throws Exception
  {
    assumeAdaptivePool();
    resourceFactory = (ResourceFactory<String, String>) EasyMock.createMock(ResourceFactory.class);

    pool = createPool(2, TimeUnit.SECONDS.toMillis(1), true);

    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(2);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> warm = pool.take("billy");
    Assert.assertEquals("billy0", warm.get());
    warm.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    Thread.sleep(1500);

    // billy1 sits at the head of the queue, so its close() fails first - billy0 behind it must still be closed.
    resourceFactory.close("billy1");
    EasyMock.expectLastCall().andThrow(new ISE("close blew up"));
    resourceFactory.close("billy0");
    EasyMock.expectLastCall();
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy2", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy3").times(1);
    EasyMock.expect(resourceFactory.isGood("billy3")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    CountDownLatch heldUntil = new CountDownLatch(1);
    Assert.assertEquals("billy2", takeOnAnotherThread(heldUntil, "billy"));
    Assert.assertEquals("billy3", takeOnAnotherThread(heldUntil, "billy"));
    heldUntil.countDown();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * Resources created before an eager initialization fails are closed. The half-built holder never reaches the cache,
   * so nothing else can ever reach them again.
   */
  @Test
  public void testEagerInitializationFailureClosesAlreadyCreatedResources()
  {
    assumeAdaptivePool();
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy0").times(1);
    EasyMock.expect(resourceFactory.generate("billy")).andThrow(new ISE("no more billies")).times(1);
    resourceFactory.close("billy0");
    EasyMock.expectLastCall();
    EasyMock.replay(resourceFactory);

    Exception thrown = null;
    try {
      pool.take("billy");
    }
    catch (Exception e) {
      thrown = e;
    }
    Assert.assertNotNull("exception", thrown);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * A lazily created resource that comes back null fails the take rather than being handed out. Eager initialization
   * rejects a null from {@link ResourceFactory#generate} outright; the lazy path cannot be more permissive, since the
   * container it would produce blows up on {@link ResourceContainer#returnResource} and never repays its lent slot.
   */
  @Test
  public void testNullGeneratedResourceFailsTheTake_lazy() throws Exception
  {
    assumeAdaptivePool();
    setUpPoolWithoutEagerInitialization();

    EasyMock.expect(resourceFactory.generate("billy")).andReturn(null).times(1);
    EasyMock.replay(resourceFactory);

    Exception thrown = null;
    try {
      pool.take("billy");
    }
    catch (Exception e) {
      thrown = e;
    }
    Assert.assertNotNull("a null resource must fail the take", thrown);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy1").times(1);
    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    CountDownLatch heldUntil = new CountDownLatch(1);
    Assert.assertEquals("billy1", takeOnAnotherThread(heldUntil, "billy"));
    Assert.assertEquals("billy2", takeOnAnotherThread(heldUntil, "billy"));
    heldUntil.countDown();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * A take interrupted while waiting for a free slot does not hand back a container that throws when returned.
   */
  @Test
  public void testInterruptedTakeDoesNotHandOutABrokenContainer() throws Exception
  {
    primePool();

    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    final ResourceContainer<String> first = pool.take("billy");
    final ResourceContainer<String> second = pool.take("billy");
    Assert.assertEquals("billy1", first.get());
    Assert.assertEquals("billy0", second.get());

    TakeAndReturnThread waiter = new TakeAndReturnThread("billy");
    waiter.start();
    waitUntilParked(waiter);
    waiter.interrupt();

    Assert.assertNull("returning an interrupted take's container", waiter.failureFromTakeAndReturn());

    keepLent(first, second);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * A take unblocked by {@link ResourcePool#close} does not hand back a container that throws when returned.
   */
  @Test
  public void testTakeUnblockedByCloseDoesNotHandOutABrokenContainer() throws Exception
  {
    primePool();

    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    resourceFactory.close("sally0");
    EasyMock.expectLastCall();
    resourceFactory.close("sally1");
    EasyMock.expectLastCall();
    EasyMock.replay(resourceFactory);

    final ResourceContainer<String> first = pool.take("billy");
    final ResourceContainer<String> second = pool.take("billy");
    Assert.assertEquals("billy1", first.get());
    Assert.assertEquals("billy0", second.get());

    TakeAndReturnThread waiter = new TakeAndReturnThread("billy");
    waiter.start();
    waitUntilParked(waiter);
    pool.close();

    Assert.assertNull("returning the container of a take unblocked by close()", waiter.failureFromTakeAndReturn());

    keepLent(first, second);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * Returning a resource twice is ignored rather than repaid twice, and a returned container cannot be read again.
   * A second repayment would let the pool lend more than {@link ResourcePoolConfig#getMaxPerKey()} at once.
   */
  @Test
  public void testDoubleReturnIsIgnored() throws Exception
  {
    primePool();

    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(true).times(2);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy1", billy.get());
    billy.returnResource();
    billy.returnResource();

    Assert.assertThrows(IllegalStateException.class, billy::get);

    CountDownLatch heldUntil = new CountDownLatch(1);
    Assert.assertEquals("billy0", takeOnAnotherThread(heldUntil, "billy"));
    Assert.assertEquals("billy1", takeOnAnotherThread(heldUntil, "billy"));
    Assert.assertNull("pool must not lend more than maxPerKey", takeOnAnotherThread(heldUntil, "billy", 1));
    heldUntil.countDown();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * The replacement generated for a resource that failed its health check is handed out without being checked itself.
   */
  @Test
  public void testReplacementForAnUnhealthyResourceIsNotChecked_lazy()
  {
    setUpPoolWithoutEagerInitialization();

    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy0").times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(false).times(1);
    resourceFactory.close("billy0");
    EasyMock.expectLastCall();
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy1").times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy1", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * Expiry is per key: evicting one key's stale resources leaves another key's warm ones untouched.
   */
  @Test
  public void testExpiryOfOneKeyLeavesOtherKeysUntouched() throws Exception
  {
    resourceFactory = (ResourceFactory<String, String>) EasyMock.createMock(ResourceFactory.class);

    pool = createPool(2, TimeUnit.SECONDS.toMillis(1), true);

    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(2);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy0", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    Thread.sleep(1500);

    // sally is primed after the sleep, so her resources are warm while billy's are stale.
    EasyMock.expect(resourceFactory.generate("sally")).andAnswer(new StringIncrementingAnswer("sally")).times(2);
    EasyMock.expect(resourceFactory.isGood("sally0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> sally = pool.take("sally");
    Assert.assertEquals("sally0", sally.get());
    sally.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    // Any close() of a sally resource here would be an unexpected call on the mock.
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();
    if (isAdaptivePool()) {
      resourceFactory.close("billy0");
      EasyMock.expectLastCall();
      EasyMock.expect(resourceFactory.isGood("billy2")).andReturn(true).times(1);
    }
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    EasyMock.replay(resourceFactory);

    billy = pool.take("billy");
    Assert.assertEquals("billy2", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    EasyMock.expect(resourceFactory.isGood("sally1")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    sally = pool.take("sally");
    Assert.assertEquals("sally1", sally.get());
    sally.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  /**
   * Under contention with expiry constantly evicting resources, the pool never lends one resource to two callers, never
   * hands out a closed resource, and never loses one.
   */
  @Test
  public void testConcurrentTakeAndReturn() throws Exception
  {
    final int maxPerKey = 4;
    final int threads = 8;
    final ExclusiveResourceFactory factory = new ExclusiveResourceFactory();
    final ResourcePool<String, String> stressPool = new ResourcePool<>(
        factory,
        new ResourcePoolConfig(maxPerKey, 5, poolImplementation),
        false
    );

    final ExecutorService exec = Execs.multiThreaded(threads, "resource-pool-stress-%d");
    final List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      futures.add(exec.submit(() -> {
        for (int j = 0; j < 200; j++) {
          ResourceContainer<String> container = stressPool.take("billy");
          factory.markLent(container.get());
          factory.markReturned(container.get());
          container.returnResource();
        }
      }));
    }
    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }
    exec.shutdown();
    stressPool.close();

    Assert.assertNull("resource misuse", factory.misuse());
    Assert.assertTrue(
        StringUtils.format("lent %s at once, max is %s", factory.peakLent(), maxPerKey),
        factory.peakLent() <= maxPerKey
    );
    Assert.assertEquals("every generated resource is closed", factory.generated(), factory.closed());
  }

  /**
   * Keeps lent containers reachable until here. {@link ResourceContainer#finalize()} gives a resource back if its
   * container is collected, which would hand a free slot to a thread that the test needs to stay blocked.
   */
  private static void keepLent(ResourceContainer<String> first, ResourceContainer<String> second)
  {
    Reference.reachabilityFence(first);
    Reference.reachabilityFence(second);
  }

  /**
   * Blocks until {@code thread} is parked waiting for a free slot.
   */
  private static void waitUntilParked(Thread thread) throws InterruptedException
  {
    for (int i = 0; i < 500 && thread.getState() != Thread.State.WAITING; i++) {
      Thread.sleep(10);
    }
    Assert.assertEquals("thread waiting for a resource", Thread.State.WAITING, thread.getState());
  }

  /**
   * Returns the resource obtained by taking {@code key} on another thread, or null if the take did not complete within
   * a few seconds - which is what a leaked lent slot looks like, since the pool blocks once every slot is handed out.
   * The resource stays lent until {@code heldUntil} is counted down, so successive takes cannot reuse each other's.
   */
  private String takeOnAnotherThread(CountDownLatch heldUntil, String key) throws InterruptedException
  {
    return takeOnAnotherThread(heldUntil, key, 5);
  }

  private String takeOnAnotherThread(CountDownLatch heldUntil, String key, long waitSeconds)
      throws InterruptedException
  {
    MyThread thread = new MyThread(heldUntil, key);
    thread.start();
    thread.waitForValueToBeGotten(waitSeconds, TimeUnit.SECONDS);
    return thread.getValue();
  }

  private static class StringIncrementingAnswer implements IAnswer<String>
  {
    int count = 0;
    private String string;

    public StringIncrementingAnswer(String string)
    {
      this.string = string;
    }

    @Override
    public String answer()
    {
      return string + count++;
    }
  }

  private class MyThread extends Thread
  {
    private final CountDownLatch gotValueLatch = new CountDownLatch(1);

    private final CountDownLatch latch1;
    private String resourceName;

    volatile String value = null;

    public MyThread(CountDownLatch latch1, String resourceName)
    {
      this.latch1 = latch1;
      this.resourceName = resourceName;
    }

    @Override
    public void run()
    {
      ResourceContainer<String> resourceContainer = pool.take(resourceName);
      value = resourceContainer.get();
      gotValueLatch.countDown();
      try {
        latch1.await();
      }
      catch (InterruptedException e) {

      }
      resourceContainer.returnResource();
    }

    public String getValue()
    {
      return value;
    }

    public void waitForValueToBeGotten(long length, TimeUnit timeUnit) throws InterruptedException
    {
      gotValueLatch.await(length, timeUnit);
    }
  }

  /**
   * Takes a resource and immediately gives it back, capturing whatever either step throws.
   */
  private class TakeAndReturnThread extends Thread
  {
    private final CountDownLatch done = new CountDownLatch(1);
    private final String resourceName;

    private volatile Throwable failure;

    private TakeAndReturnThread(String resourceName)
    {
      this.resourceName = resourceName;
    }

    @Override
    public void run()
    {
      try {
        ResourceContainer<String> container = pool.take(resourceName);
        if (container != null) {
          container.returnResource();
        }
      }
      catch (Throwable t) {
        failure = t;
      }
      done.countDown();
    }

    /**
     * Returns what taking and returning threw, or null if both completed cleanly.
     */
    private Throwable failureFromTakeAndReturn() throws InterruptedException
    {
      Assert.assertTrue("take completed", done.await(5, TimeUnit.SECONDS));
      return failure;
    }
  }

  /**
   * Hands out uniquely numbered resources and records any misuse of them by the pool.
   */
  private static class ExclusiveResourceFactory implements ResourceFactory<String, String>
  {
    private final AtomicInteger generated = new AtomicInteger();
    private final AtomicInteger closed = new AtomicInteger();
    private final AtomicInteger lent = new AtomicInteger();
    private final AtomicInteger peakLent = new AtomicInteger();
    private final Set<String> currentlyLent = ConcurrentHashMap.newKeySet();
    private final Set<String> destroyed = ConcurrentHashMap.newKeySet();
    private final AtomicReference<String> misuse = new AtomicReference<>();

    @Override
    public String generate(String key)
    {
      return key + generated.incrementAndGet();
    }

    @Override
    public boolean isGood(String resource)
    {
      return !destroyed.contains(resource);
    }

    @Override
    public void close(String resource)
    {
      closed.incrementAndGet();
      if (!destroyed.add(resource)) {
        misuse.compareAndSet(null, "closed twice: " + resource);
      }
    }

    void markLent(String resource)
    {
      if (destroyed.contains(resource)) {
        misuse.compareAndSet(null, "lent after close: " + resource);
      }
      if (!currentlyLent.add(resource)) {
        misuse.compareAndSet(null, "lent to two callers at once: " + resource);
      }
      peakLent.accumulateAndGet(lent.incrementAndGet(), Math::max);
    }

    void markReturned(String resource)
    {
      lent.decrementAndGet();
      currentlyLent.remove(resource);
    }

    String misuse()
    {
      return misuse.get();
    }

    int peakLent()
    {
      return peakLent.get();
    }

    int generated()
    {
      return generated.get();
    }

    int closed()
    {
      return closed.get();
    }
  }
}
