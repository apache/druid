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

package org.apache.druid.collections;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.Cleaners;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;

import java.lang.ref.WeakReference;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class StupidPool<T> implements NonBlockingPool<T>
{
  private static final Logger log = new Logger(StupidPool.class);


  /**
   * We add the ability to poison all StupidPools in order to catch resource leaks and fail them during tests.
   * <p>
   * StupidPool already has a mechanism by which it will log resource leaks (ResourceHolder objects that are not
   * closed), over time, we've built up a test suite that contains lots of those logs and generally they get swept
   * away to a perrenial Priority #2.  This is not a good state as the justification is usually that the logs are
   * coming from test harness, the production code is obviously good.  Anyway, we need tests to actually fail if there
   * are leaks like this so that the tests and the code can be improved.  Catching leaks is hard, though, because it
   * either requires reference counting and all tests sites to check the counts, or it requires catching objects being
   * GC'd, which is asynchronous.  We opt for this latter approach.
   * <p>
   * Specifically, when poisoned, the StupidPool will
   * 1) Maintain an exception (i.e. stack trace) object from each time that a resource holder is checked out
   * 2) If the ResourceHolder is GCd without being closed, the exception object will be registered back with the
   * stupid pool
   * 3) If an exception is registered with the StupidPool, then any attempt to take an object from that Pool will have
   * the exception thrown instead.
   * <p>
   * This means that we have a delayed reaction to the leak, in that the object must first be GCd before we can
   * identify the leak.  *Also* it means that the test that failed is not actually the test that leaked the object,
   * instead, developers must look at the stacktrace thrown to see which test actually checked out the object and did
   * not return it.  Additionally, it means that one test run can only discover a single leak (as once the pool is
   * poisoned, it will return the same exception constantly).  So, if there is some leaky code, it will likely require
   * multiple test runs to actually whack-a-mole all of the sources of the leaks.
   */
  private static final AtomicBoolean POISONED = new AtomicBoolean(false);

  static {
    if (Boolean.parseBoolean(System.getProperty("druid.test.stupidPool.poison"))) {
      POISONED.set(true);
    }
  }

  public static boolean isPoisoned()
  {
    return POISONED.get();
  }

  /**
   * StupidPool Implementation Note
   * It is assumed that StupidPools are never reclaimed by the GC, either stored in static fields or global singleton
   * injector like Guice. Otherwise false positive "Not closed! Object leaked from..." could be reported. To avoid
   * this, StupidPool should be made closeable (or implement {@link org.apache.druid.java.util.common.lifecycle.LifecycleStop}
   * and registered in the global lifecycle), in this close() method all {@link ObjectResourceHolder}s should be drained
   * from the {@code objects} queue, and notifier.disable() called for them.
   */
  @VisibleForTesting
  final Queue<ObjectResourceHolder> objects = new ConcurrentLinkedQueue<>();

  /**
   * {@link ConcurrentLinkedQueue}'s size() is O(n) queue traversal apparently for the sake of being 100%
   * wait-free, that is not required by {@code StupidPool}. In {@code poolSize} we account the queue size
   * ourselves, to avoid traversal of {@link #objects} in {@link #tryReturnToPool}.
   */
  @VisibleForTesting
  final AtomicLong poolSize = new AtomicLong(0);

  private final String name;
  private final Supplier<T> generator;

  private final AtomicLong createdObjectsCounter = new AtomicLong(0);
  private final AtomicLong leakedObjectsCounter = new AtomicLong(0);

  private final AtomicReference<RuntimeException> capturedException = new AtomicReference<>(null);

  //note that this is just the max entries in the cache, pool can still create as many buffers as needed.
  private final int objectsCacheMaxCount;

  public StupidPool(String name, Supplier<T> generator)
  {
    this(name, generator, 0, Integer.MAX_VALUE);
  }

  public StupidPool(String name, Supplier<T> generator, int initCount, int objectsCacheMaxCount)
  {
    Preconditions.checkArgument(
        initCount <= objectsCacheMaxCount,
        "initCount[%s] must be less/equal to objectsCacheMaxCount[%s]",
        initCount,
        objectsCacheMaxCount
    );
    this.name = name;
    this.generator = generator;
    this.objectsCacheMaxCount = objectsCacheMaxCount;

    for (int i = 0; i < initCount; i++) {
      objects.add(makeObjectWithHandler());
      poolSize.incrementAndGet();
    }
  }

  @Override
  public String toString()
  {
    return "StupidPool{" +
           "name=" + name +
           ", objectsCacheMaxCount=" + objectsCacheMaxCount +
           ", poolSize=" + poolSize() +
           "}";
  }

  @Override
  public ResourceHolder<T> take()
  {
    ObjectResourceHolder resourceHolder = objects.poll();
    if (resourceHolder == null) {
      if (POISONED.get() && capturedException.get() != null) {
        throw capturedException.get();
      }
      return makeObjectWithHandler();
    } else {
      poolSize.decrementAndGet();
      if (POISONED.get()) {
        final RuntimeException exception = capturedException.get();
        if (exception == null) {
          resourceHolder.notifier.except = new RE("Thread[%s]: leaky leak!", Thread.currentThread().getName());
        } else {
          throw exception;
        }
      }
      return resourceHolder;
    }
  }

  private ObjectResourceHolder makeObjectWithHandler()
  {
    T object = generator.get();
    createdObjectsCounter.incrementAndGet();
    ObjectId objectId = new ObjectId();
    ObjectLeakNotifier notifier = new ObjectLeakNotifier(this, POISONED.get());
    // Using objectId as referent for Cleaner, because if the object itself (e. g. ByteBuffer) is leaked after taken
    // from the pool, and the ResourceHolder is not closed, Cleaner won't notify about the leak.
    return new ObjectResourceHolder(object, objectId, Cleaners.register(objectId, notifier), notifier);
  }

  @VisibleForTesting
  public long poolSize()
  {
    return poolSize.get();
  }

  @VisibleForTesting
  long leakedObjectsCount()
  {
    return leakedObjectsCounter.get();
  }

  @VisibleForTesting
  public long objectsCreatedCount()
  {
    return createdObjectsCounter.get();
  }

  private void tryReturnToPool(T object, ObjectId objectId, Cleaners.Cleanable cleanable, ObjectLeakNotifier notifier)
  {
    long currentPoolSize;
    do {
      currentPoolSize = poolSize.get();
      if (currentPoolSize >= objectsCacheMaxCount) {
        notifier.disable();
        // Effectively does nothing, because notifier is disabled above. The purpose of this call is to deregister the
        // cleaner from the internal global linked list of all cleaners in the JVM, and let it be reclaimed itself.
        cleanable.clean();

        // Important to use the objectId after notifier.disable() (in the logging statement below), otherwise VM may
        // already decide that the objectId is unreachable and run Cleaner before notifier.disable(), that would be
        // reported as a false-positive "leak". Ideally reachabilityFence(objectId) should be inserted here.
        log.debug("cache num entries is exceeding in [%s], objectId [%s]", this, objectId);
        return;
      }
    } while (!poolSize.compareAndSet(currentPoolSize, currentPoolSize + 1));
    if (!objects.offer(new ObjectResourceHolder(object, objectId, cleanable, notifier))) {
      impossibleOffsetFailed(object, objectId, cleanable, notifier);
    }
  }

  /**
   * This should be impossible, because {@link ConcurrentLinkedQueue#offer(Object)} event don't have `return false;` in
   * it's body in OpenJDK 8.
   */
  private void impossibleOffsetFailed(
      T object,
      ObjectId objectId,
      Cleaners.Cleanable cleanable,
      ObjectLeakNotifier notifier
  )
  {
    poolSize.decrementAndGet();
    notifier.disable();
    // Effectively does nothing, because notifier is disabled above. The purpose of this call is to deregister the
    // cleaner from the internal global linked list of all cleaners in the JVM, and let it be reclaimed itself.
    cleanable.clean();
    log.error(
        new ISE("Queue offer failed"),
        "Could not offer object [%s] back into the queue, objectId [%s]",
        object,
        objectId
    );
  }

  class ObjectResourceHolder implements ResourceHolder<T>
  {
    private final AtomicReference<T> objectRef;
    private ObjectId objectId;
    private Cleaners.Cleanable cleanable;
    private ObjectLeakNotifier notifier;

    ObjectResourceHolder(
        final T object,
        final ObjectId objectId,
        final Cleaners.Cleanable cleanable,
        final ObjectLeakNotifier notifier
    )
    {
      this.objectRef = new AtomicReference<>(object);
      this.objectId = objectId;
      this.cleanable = cleanable;
      this.notifier = notifier;
    }

    // WARNING: it is entirely possible for a caller to hold onto the object and call ObjectResourceHolder.close,
    // Then still use that object even though it will be offered to someone else in StupidPool.take
    @Override
    public T get()
    {
      final T object = objectRef.get();
      if (object == null) {
        throw new ISE("Already Closed!");
      }

      return object;
    }

    @Override
    public void close()
    {
      final T object = objectRef.get();
      if (object != null && objectRef.compareAndSet(object, null)) {
        try {
          tryReturnToPool(object, objectId, cleanable, notifier);
        }
        finally {
          // Need to null reference to objectId because if ObjectResourceHolder is closed, but leaked, this reference
          // will prevent reporting leaks of ResourceHandlers when this object and objectId are taken from the pool
          // again.
          objectId = null;
          // Nulling cleaner and notifier is not strictly needed, but harmless for sure.
          cleanable = null;
          notifier = null;
        }
      }
    }

    void forceClean()
    {
      cleanable.clean();
    }
  }

  private static class ObjectLeakNotifier implements Runnable
  {
    /**
     * Don't reference {@link StupidPool} directly to prevent it's leak through the internal global chain of Cleaners.
     */
    final WeakReference<StupidPool<?>> poolReference;
    final AtomicLong leakedObjectsCounter;
    final AtomicBoolean disabled = new AtomicBoolean(false);

    private RuntimeException except;

    ObjectLeakNotifier(StupidPool<?> pool, boolean poisoned)
    {
      poolReference = new WeakReference<>(pool);
      leakedObjectsCounter = pool.leakedObjectsCounter;

      except = poisoned ? new RE("Thread[%s]: drip drip", Thread.currentThread().getName()) : null;
    }

    @Override
    public void run()
    {
      try {
        if (!disabled.getAndSet(true)) {
          leakedObjectsCounter.incrementAndGet();
          final StupidPool<?> pool = poolReference.get();
          log.warn("Not closed! Object leaked from %s. Allowing gc to prevent leak.", pool);
          if (except != null && pool != null) {
            pool.capturedException.set(except);
            log.error(except, "notifier[%s], dumping stack trace from object checkout and poisoning pool", this);
          }
        }
      }
      // Exceptions must not be thrown in Cleaner.clean(), which calls this ObjectReclaimer.run() method
      catch (Exception e) {
        try {
          log.error(e, "Exception in ObjectLeakNotifier.run()");
        }
        catch (Exception ignore) {
          // ignore
        }
      }
    }

    public void disable()
    {
      disabled.set(true);
    }
  }

  /**
   * Plays the role of the reference for Cleaner, see comment in {@link #makeObjectWithHandler}
   */
  private static class ObjectId
  {
  }
}
