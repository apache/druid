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

package io.druid.collections;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import sun.misc.Cleaner;

import java.lang.ref.WeakReference;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class StupidPool<T> implements NonBlockingPool<T>
{
  private static final Logger log = new Logger(StupidPool.class);

  private final String name;
  private final Supplier<T> generator;

  /**
   * StupidPool Implementation Note
   * It is assumed that StupidPools are never reclaimed by the GC, either stored in static fields or global singleton
   * injector like Guice. Otherwise false positive "Not closed! Object leaked from..." could be reported. To avoid
   * this, StupidPool should be made closeable (or implement {@link io.druid.java.util.common.lifecycle.LifecycleStop}
   * and registered in the global lifecycle), in this close() method all {@link ObjectResourceHolder}s should be drained
   * from the {@code objects} queue, and notifier.disable() called for them.
   */
  private final Queue<ObjectResourceHolder> objects = new ConcurrentLinkedQueue<>();
  /**
   * {@link ConcurrentLinkedQueue}'s size() is O(n) queue traversal apparently for the sake of being 100%
   * wait-free, that is not required by {@code StupidPool}. In {@code poolSize} we account the queue size
   * ourselves, to avoid traversal of {@link #objects} in {@link #tryReturnToPool}.
   */
  private final AtomicLong poolSize = new AtomicLong(0);
  private final AtomicLong leakedObjectsCounter = new AtomicLong(0);

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
      return makeObjectWithHandler();
    } else {
      poolSize.decrementAndGet();
      return resourceHolder;
    }
  }

  private ObjectResourceHolder makeObjectWithHandler()
  {
    T object = generator.get();
    ObjectId objectId = new ObjectId();
    ObjectLeakNotifier notifier = new ObjectLeakNotifier(this);
    // Using objectId as referent for Cleaner, because if the object itself (e. g. ByteBuffer) is leaked after taken
    // from the pool, and the ResourceHolder is not closed, Cleaner won't notify about the leak.
    return new ObjectResourceHolder(object, objectId, Cleaner.create(objectId, notifier), notifier);
  }

  @VisibleForTesting
  long poolSize()
  {
    return poolSize.get();
  }

  @VisibleForTesting
  long leakedObjectsCount()
  {
    return leakedObjectsCounter.get();
  }

  private void tryReturnToPool(T object, ObjectId objectId, Cleaner cleaner, ObjectLeakNotifier notifier)
  {
    long currentPoolSize;
    do {
      currentPoolSize = poolSize.get();
      if (currentPoolSize >= objectsCacheMaxCount) {
        notifier.disable();
        // Effectively does nothing, because notifier is disabled above. The purpose of this call is to deregister the
        // cleaner from the internal global linked list of all cleaners in the JVM, and let it be reclaimed itself.
        cleaner.clean();
        // Important to use the objectId after notifier.disable() (in the logging statement below), otherwise VM may
        // already decide that the objectId is unreachable and run Cleaner before notifier.disable(), that would be
        // reported as a false-positive "leak". Ideally reachabilityFence(objectId) should be inserted here.
        log.debug("cache num entries is exceeding in [%s], objectId [%s]", this, objectId);
        return;
      }
    } while (!poolSize.compareAndSet(currentPoolSize, currentPoolSize + 1));
    if (!objects.offer(new ObjectResourceHolder(object, objectId, cleaner, notifier))) {
      impossibleOffsetFailed(object, objectId, cleaner, notifier);
    }
  }

  /**
   * This should be impossible, because {@link ConcurrentLinkedQueue#offer(Object)} event don't have `return false;` in
   * it's body in OpenJDK 8.
   */
  private void impossibleOffsetFailed(T object, ObjectId objectId, Cleaner cleaner, ObjectLeakNotifier notifier)
  {
    poolSize.decrementAndGet();
    notifier.disable();
    // Effectively does nothing, because notifier is disabled above. The purpose of this call is to deregister the
    // cleaner from the internal global linked list of all cleaners in the JVM, and let it be reclaimed itself.
    cleaner.clean();
    log.error(
        new ISE("Queue offer failed"),
        "Could not offer object [%s] back into the queue, objectId [%s]",
        object,
        objectId
    );
  }

  private class ObjectResourceHolder implements ResourceHolder<T>
  {
    private final AtomicReference<T> objectRef;
    private ObjectId objectId;
    private Cleaner cleaner;
    private ObjectLeakNotifier notifier;

    ObjectResourceHolder(
        final T object,
        final ObjectId objectId,
        final Cleaner cleaner,
        final ObjectLeakNotifier notifier
    )
    {
      this.objectRef = new AtomicReference<>(object);
      this.objectId = objectId;
      this.cleaner = cleaner;
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
          tryReturnToPool(object, objectId, cleaner, notifier);
        }
        finally {
          // Need to null reference to objectId because if ObjectResourceHolder is closed, but leaked, this reference
          // will prevent reporting leaks of ResourceHandlers when this object and objectId are taken from the pool
          // again.
          objectId = null;
          // Nulling cleaner and notifier is not strictly needed, but harmless for sure.
          cleaner = null;
          notifier = null;
        }
      }
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

    ObjectLeakNotifier(StupidPool<?> pool)
    {
      poolReference = new WeakReference<>(pool);
      leakedObjectsCounter = pool.leakedObjectsCounter;
    }

    @Override
    public void run()
    {
      try {
        if (!disabled.getAndSet(true)) {
          leakedObjectsCounter.incrementAndGet();
          log.warn("Not closed! Object leaked from %s. Allowing gc to prevent leak.", poolReference.get());
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
