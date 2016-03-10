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

package io.druid.server.namespace.cache;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ExecutorServices;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = OnHeapNamespaceExtractionCacheManager.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "offHeap", value = OffHeapNamespaceExtractionCacheManager.class),
    @JsonSubTypes.Type(name = "onHeap", value = OnHeapNamespaceExtractionCacheManager.class)
})
public abstract class NamespaceExtractionCacheManager
{
  protected static class NamespaceImplData
  {
    public NamespaceImplData(
        final ListenableFuture<?> future,
        final ExtractionNamespace namespace,
        final String name
    )
    {
      this.future = future;
      this.namespace = namespace;
      this.name = name;
    }

    final ListenableFuture<?> future;
    final ExtractionNamespace namespace;
    final String name;
    final AtomicBoolean enabled = new AtomicBoolean(false);
    final AtomicReference<Function<String, String>> fn = new AtomicReference<>(null);
    final AtomicReference<Function<String, List<String>>> reverseFn = new AtomicReference<>(null);
  }

  private static final Logger log = new Logger(NamespaceExtractionCacheManager.class);
  private final ListeningScheduledExecutorService listeningScheduledExecutorService;
  protected final ConcurrentMap<String, Function<String, String>> fnCache;
  protected final ConcurrentMap<String, Function<String, List<String>>> reverseFnCache;
  protected final ConcurrentMap<String, NamespaceImplData> implData = new ConcurrentHashMap<>();
  protected final AtomicLong tasksStarted = new AtomicLong(0);
  protected final AtomicLong dataSize = new AtomicLong(0);
  protected final ServiceEmitter serviceEmitter;
  private final ConcurrentHashMap<String, String> lastVersion = new ConcurrentHashMap<>();
  private final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>> namespaceFunctionFactoryMap;

  public NamespaceExtractionCacheManager(
      Lifecycle lifecycle,
      final ConcurrentMap<String, Function<String, String>> fnCache,
      final ConcurrentMap<String, Function<String, List<String>>> reverseFnCache,
      final ServiceEmitter serviceEmitter,
      final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>> namespaceFunctionFactoryMap
  )
  {
    this.listeningScheduledExecutorService = MoreExecutors.listeningDecorator(
        Executors.newScheduledThreadPool(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("NamespaceExtractionCacheManager-%d")
                .setPriority(Thread.MIN_PRIORITY)
                .build()
        )
    );
    ExecutorServices.manageLifecycle(lifecycle, listeningScheduledExecutorService);
    this.serviceEmitter = serviceEmitter;
    this.fnCache = fnCache;
    this.reverseFnCache = reverseFnCache;
    this.namespaceFunctionFactoryMap = namespaceFunctionFactoryMap;
    listeningScheduledExecutorService.scheduleAtFixedRate(
        new Runnable()
        {
          long priorTasksStarted = 0L;

          @Override
          public void run()
          {
            try {
              final long tasks = tasksStarted.get();
              serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/size", dataSize.get()));
              serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/count", fnCache.size()));
              serviceEmitter.emit(
                  ServiceMetricEvent.builder()
                                    .build("namespace/deltaTasksStarted", tasks - priorTasksStarted)
              );
              priorTasksStarted = tasks;
            }catch(Exception e){
              log.error(e, "Error emitting namespace stats");
              if(Thread.currentThread().isInterrupted()){
                throw Throwables.propagate(e);
              }
            }
          }
        },
        1,
        10, TimeUnit.MINUTES
    );
  }

  protected boolean waitForServiceToEnd(long time, TimeUnit unit) throws InterruptedException
  {
    return listeningScheduledExecutorService.awaitTermination(time, unit);
  }


  protected <T extends ExtractionNamespace> Runnable getPostRunnable(
      final T namespace,
      final ExtractionNamespaceFunctionFactory<T> factory,
      final String cacheId
  )
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        final String nsName = namespace.getNamespace();
        final NamespaceImplData namespaceDatum = implData.get(nsName);
        if (namespaceDatum == null) {
          // was removed
          return;
        }
        synchronized (namespaceDatum.enabled) {
          if (!namespaceDatum.enabled.get()) {
            // skip because it was disabled
            return;
          }
          swapAndClearCache(nsName, cacheId);
          final Function<String, String> fn = factory.buildFn(namespace, getCacheMap(nsName));
          final Function<String, List<String>> reverseFn = factory.buildReverseFn(namespace, getCacheMap(nsName));
          final Function<String, String> priorFn = fnCache.put(nsName, fn);
          final Function<String, List<String>> priorReverseFn = reverseFnCache.put(nsName, reverseFn);
          if (priorFn != null && priorFn != namespaceDatum.fn.get()) {
            log.warn("Replaced prior function for namespace [%s]", nsName);
          }
          if (priorReverseFn != null && priorReverseFn != namespaceDatum.reverseFn.get()) {
            log.warn("Replaced prior reverse function for namespace [%s]", nsName);
          }
          namespaceDatum.fn.set(fn);
          namespaceDatum.reverseFn.set(reverseFn);
        }
      }
    };
  }

  public void scheduleOrUpdate(
      final Collection<ExtractionNamespace> namespaces
  )
  {
    Set<String> differentNamespaces = Sets.difference(
        implData.keySet(), Sets.newHashSet(
            Iterables.transform(
                namespaces,
                new Function<ExtractionNamespace, String>()
                {
                  @Nullable
                  @Override
                  public String apply(
                      ExtractionNamespace input
                  )
                  {
                    return input.getNamespace();
                  }
                }
            )
        )
    );
    if (log.isDebugEnabled()) {
      log.debug("Deleting %d namespaces: %s", differentNamespaces.size(), differentNamespaces);
    }
    for (String namespaceName : differentNamespaces) {
      final NamespaceImplData implDatum = implData.get(namespaceName);
      if (implDatum == null) {
        // Delete but we don't have it?
        log.wtf("Asked to delete something I just lost [%s]", namespaceName);
        continue;
      }
      delete(namespaceName);
    }
    for (final ExtractionNamespace namespace : namespaces) {
      final NamespaceImplData implDatum = implData.get(namespace.getNamespace());
      if (implDatum == null) {
        // New, probably
        schedule(namespace);
        continue;
      }
      if (!implDatum.enabled.get()) {
        // Race condition. Someone else disabled it first, go ahead and reschedule
        schedule(namespace);
        continue;
      }

      // Live one. Check if it needs updated
      if (implDatum.namespace.equals(namespace)) {
        // skip if no update
        continue;
      }
      if (log.isDebugEnabled()) {
        log.debug("Namespace [%s] needs updated to [%s]", implDatum.namespace, namespace);
      }
      removeNamespaceLocalMetadata(implDatum);
      schedule(namespace);
    }
  }

  private void cancelFuture(final NamespaceImplData implDatum)
  {
    synchronized (implDatum.enabled) {
      final CountDownLatch latch = new CountDownLatch(1);
      final ListenableFuture<?> future = implDatum.future;
      Futures.addCallback(
          future, new FutureCallback<Object>()
          {
            @Override
            public void onSuccess(Object result)
            {
              latch.countDown();
            }

            @Override
            public void onFailure(Throwable t)
            {
              // Expect CancellationException
              latch.countDown();
              if (!(t instanceof CancellationException)) {
                log.error(t, "Error in namespace [%s]", implDatum.name);
              }
            }
          }
      );
      if (!future.isDone()
          && !future.cancel(true)) { // Interrupt to make sure we don't pollute stuff after we've already cleaned up
        throw new ISE("Future for namespace [%s] was not able to be canceled", implDatum.name);
      }
      try {
        latch.await();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      }
    }
  }

  private boolean removeNamespaceLocalMetadata(final NamespaceImplData implDatum)
  {
    if (implDatum == null) {
      return false;
    }
    synchronized (implDatum.enabled) {
      if (!implDatum.enabled.compareAndSet(true, false)) {
        return false;
      }
      if (!implDatum.future.isDone()) {
        cancelFuture(implDatum);
      }
      return implData.remove(implDatum.name, implDatum);
    }
  }

  // Optimistic scheduling of updates to a namespace.
  public <T extends ExtractionNamespace> ListenableFuture<?> schedule(final T namespace)
  {
    final ExtractionNamespaceFunctionFactory<T> factory = (ExtractionNamespaceFunctionFactory<T>)
        namespaceFunctionFactoryMap.get(namespace.getClass());
    if (factory == null) {
      throw new ISE("Cannot find factory for namespace [%s]", namespace);
    }
    final String cacheId = UUID.randomUUID().toString();
    return schedule(namespace, factory, getPostRunnable(namespace, factory, cacheId), cacheId);
  }

  // For testing purposes this is protected
  protected <T extends ExtractionNamespace> ListenableFuture<?> schedule(
      final T namespace,
      final ExtractionNamespaceFunctionFactory<T> factory,
      final Runnable postRunnable,
      final String cacheId
  )
  {
    final String namespaceName = namespace.getNamespace();
    log.debug("Trying to update namespace [%s]", namespaceName);
    final NamespaceImplData implDatum = implData.get(namespaceName);
    if (implDatum != null) {
      synchronized (implDatum.enabled) {
        if (implDatum.enabled.get()) {
          // We also check at the end of the function, but fail fast here
          throw new IAE("Namespace [%s] already exists! Leaving prior running", namespace.toString());
        }
      }
    }
    final long updateMs = namespace.getPollMs();
    final CountDownLatch startLatch = new CountDownLatch(1);

    final Runnable command = new Runnable()
    {
      @Override
      public void run()
      {
        try {
          startLatch.await(); // wait for "election" to leadership or cancellation
          if (!Thread.currentThread().isInterrupted()) {
            final Map<String, String> cache = getCacheMap(cacheId);
            final String preVersion = lastVersion.get(namespaceName);
            final Callable<String> runnable = factory.getCachePopulator(namespace, preVersion, cache);

            tasksStarted.incrementAndGet();
            final String newVersion = runnable.call();
            if (preVersion != null && preVersion.equals(newVersion)) {
              throw new CancellationException(String.format("Version `%s` already exists", preVersion));
            }
            if (newVersion != null) {
              lastVersion.put(namespaceName, newVersion);
            }
            postRunnable.run();
            log.debug("Namespace [%s] successfully updated", namespaceName);
          }
        }
        catch (Throwable t) {
          delete(cacheId);
          if (t instanceof CancellationException) {
            log.debug(t, "Namespace [%s] cancelled", namespaceName);
          } else {
            log.error(t, "Failed update namespace [%s]", namespace);
          }
          if(Thread.currentThread().isInterrupted()) {
            throw Throwables.propagate(t);
          }
        }
      }
    };

    ListenableFuture<?> future;
    try {
      if (updateMs > 0) {
        future = listeningScheduledExecutorService.scheduleAtFixedRate(command, 0, updateMs, TimeUnit.MILLISECONDS);
      } else {
        future = listeningScheduledExecutorService.schedule(command, 0, TimeUnit.MILLISECONDS);
      }

      final NamespaceImplData me = new NamespaceImplData(future, namespace, namespaceName);
      final NamespaceImplData other = implData.putIfAbsent(namespaceName, me);
      if (other != null) {
        if (!future.isDone() && !future.cancel(true)) {
          log.warn("Unable to cancel future for namespace[%s] on race loss", namespaceName);
        }
        throw new IAE("Namespace [%s] already exists! Leaving prior running", namespace);
      } else {
        if (!me.enabled.compareAndSet(false, true)) {
          log.wtf("How did someone enable this before ME?");
        }
        log.debug("I own namespace [%s]", namespaceName);
        return future;
      }
    }
    finally {
      startLatch.countDown();
    }
  }

  /**
   * This method is expected to swap the cacheKey into the active namespace, and leave future requests for new cacheKey available. getCacheMap(cacheKey) should return empty data after this call.
   *
   * @param namespaceKey The namespace to swap the cache into
   * @param cacheKey     The cacheKey that contains the data of interest
   *
   * @return true if old data was cleared. False if no old data was found
   */
  protected abstract boolean swapAndClearCache(String namespaceKey, String cacheKey);

  /**
   * Return a ConcurrentMap with the specified ID (either namespace's name or a cache key ID)
   *
   * @param namespaceOrCacheKey Either a namespace or cache key should be acceptable here.
   *
   * @return A ConcurrentMap<String, String> that is backed by the impl which implements this method.
   */
  public abstract ConcurrentMap<String, String> getCacheMap(String namespaceOrCacheKey);

  /**
   * Clears out resources used by the namespace such as threads. Implementations may override this and call super.delete(...) if they have resources of their own which need cleared.
   * <p/>
   * This particular method is NOT thread safe, and any impl which is intended to be thread safe should safe-guard calls to this method.
   *
   * @param ns The namespace to be deleted
   *
   * @return True if a deletion occurred, false if no deletion occurred.
   *
   * @throws ISE if there is an error cancelling the namespace's future task
   */
  public boolean delete(final String ns)
  {
    final NamespaceImplData implDatum = implData.get(ns);
    final boolean deleted = removeNamespaceLocalMetadata(implDatum);
    // At this point we have won leader election on canceling this implDatum
    if (deleted) {
      log.info("Deleting namespace [%s]", ns);
      lastVersion.remove(implDatum.name);
      fnCache.remove(implDatum.name);
      return true;
    } else {
      log.debug("Did not delete namespace [%s]", ns);
      return false;
    }
  }

  public String getVersion(String namespace)
  {
    if (namespace == null) {
      return null;
    } else {
      return lastVersion.get(namespace);
    }
  }

  public Collection<String> getKnownNamespaces()
  {
    return fnCache.keySet();
  }
}
