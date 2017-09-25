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

package io.druid.server.lookup.namespace.cache;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.lookup.namespace.NamespaceExtractionConfig;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import sun.misc.Cleaner;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class OffHeapNamespaceExtractionCacheManager extends NamespaceExtractionCacheManager
{
  private static final Logger log = new Logger(OffHeapNamespaceExtractionCacheManager.class);

  private class MapDbCacheDisposer implements Runnable
  {
    final String mapDbKey;
    /**
     * Manages the race between dispose via {@link #disposeManually()} and automatic dispose by the JVM via
     * {@link #run()}.
     *
     * <p>In case of actual race, we don't wait in those methods until the other one, which manages to switch this flag
     * first, completes. This could result into the situation that neither one completes, if the JVM is shutting down
     * and the thread from which {@link Cleaner#clean()} (delegating to {@link #run()}) is called started the disposal
     * operation, then more deterministic shutdown hook / lifecycle.stop(), which may call {@link #disposeManually()}
     * completed early, and then the whole process shuts down before {@link Cleaner#clean()} completes, because shutdown
     * is not blocked by it. However this should be harmless because anyway we remove the whole MapDB's file in
     * lifecycle.stop() (see {@link OffHeapNamespaceExtractionCacheManager#OffHeapNamespaceExtractionCacheManager}).
     * However if we persist off-heap DB between JVM runs, this decision should be revised.
     */
    final AtomicBoolean disposed = new AtomicBoolean(false);

    private MapDbCacheDisposer(String mapDbKey)
    {
      this.mapDbKey = mapDbKey;
    }

    /**
     * To be called by the JVM via {@link Cleaner#clean()}. The only difference from {@link #disposeManually()} is
     * exception treatment.
     */
    @Override
    public void run()
    {
      if (disposed.compareAndSet(false, true)) {
        try {
          doDispose();
          // Log statement goes after doDispose(), because logging may fail (e. g. if we are in shutdownHooks).
          log.error("OffHeapNamespaceExtractionCacheManager.disposeCache() was not called, disposed resources by the JVM");
        }
        catch (Throwable t) {
          try {
            log.error(t, "Error while deleting key %s from MapDb", mapDbKey);
          }
          catch (Exception e) {
            t.addSuppressed(e);
          }
          Throwables.propagateIfInstanceOf(t, Error.class);
          // Must not throw exceptions in the cleaner thread, run by the JVM.
        }
      }
    }

    /**
     * To be called from {@link #disposeCache(CacheHandler)}. The only difference from {@link #run()} is exception
     * treatment, disposeManually() lefts all exceptions thrown in DB.delete() to the caller.
     */
    void disposeManually()
    {
      if (disposed.compareAndSet(false, true)) {
        // TODO: resolve what happens here if query is actively going on
        doDispose();
      }
    }

    private void doDispose()
    {
      if (!mmapDB.isClosed()) {
        mmapDB.delete(mapDbKey);
      }
      cacheCount.decrementAndGet();
    }
  }

  private static class MapDbCacheDisposerAndCleaner
  {
    final MapDbCacheDisposer cacheDisposer;
    final Cleaner cleaner;

    private MapDbCacheDisposerAndCleaner(MapDbCacheDisposer cacheDisposer, Cleaner cleaner)
    {
      this.cacheDisposer = cacheDisposer;
      this.cleaner = cleaner;
    }
  }

  private final DB mmapDB;
  private final File tmpFile;
  private AtomicLong mapDbKeyCounter = new AtomicLong(0);
  private AtomicInteger cacheCount = new AtomicInteger(0);

  @Inject
  public OffHeapNamespaceExtractionCacheManager(
      Lifecycle lifecycle,
      ServiceEmitter serviceEmitter,
      NamespaceExtractionConfig config
  )
  {
    super(lifecycle, serviceEmitter, config);
    try {
      tmpFile = File.createTempFile("druidMapDB", getClass().getCanonicalName());
      log.info("Using file [%s] for mapDB off heap namespace cache", tmpFile.getAbsolutePath());
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    mmapDB = DBMaker
        .newFileDB(tmpFile)
        .closeOnJvmShutdown()
        .transactionDisable()
        .deleteFilesAfterClose()
        .strictDBGet()
        .asyncWriteEnable()
        .mmapFileEnable()
        .commitFileSyncDisable()
        .cacheSize(10_000_000)
        .make();
    try {
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              // NOOP
            }

            @Override
            public synchronized void stop()
            {
              if (!mmapDB.isClosed()) {
                mmapDB.close();
                if (!tmpFile.delete()) {
                  log.warn("Unable to delete file at [%s]", tmpFile.getAbsolutePath());
                }
              }
            }
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public CacheHandler createCache()
  {
    ConcurrentMap<String, String> cache;
    String mapDbKey;
    // This loop will succeed because 2^64 cache maps couldn't exist in memory simultaneously
    while (true) {
      mapDbKey = Long.toString(mapDbKeyCounter.getAndIncrement());
      try {
        HTreeMap<String, String> hTreeMap = mmapDB.createHashMap(mapDbKey).make();
        // Access MapDB's HTreeMap and create a cleaner via proxy, because there is no 100% confidence that there are
        // no memory leaks in MapDB and in OffHeapCacheManager. Otherwise JVM will never be able to clean the cleaner
        // and dispose leaked cache.
        cache = new CacheProxy(hTreeMap);
        cacheCount.incrementAndGet();
        break;
      }
      catch (IllegalArgumentException e) {
        // failed to create a map, the key exists, go to the next iteration
      }
    }
    MapDbCacheDisposer cacheDisposer = new MapDbCacheDisposer(mapDbKey);
    // Cleaner is "the second level of defence". Normally all users of createCache() must call disposeCache() with
    // the returned CacheHandler instance manually. But if they don't do this for whatever reason, JVM will cleanup
    // the cache itself.
    Cleaner cleaner = Cleaner.create(cache, cacheDisposer);
    MapDbCacheDisposerAndCleaner disposerAndCleaner = new MapDbCacheDisposerAndCleaner(
        cacheDisposer,
        cleaner
    );
    return new CacheHandler(this, cache, disposerAndCleaner);
  }

  @Override
  void disposeCache(CacheHandler cacheHandler)
  {
    MapDbCacheDisposerAndCleaner disposerAndCleaner = (MapDbCacheDisposerAndCleaner) cacheHandler.id;
    disposerAndCleaner.cacheDisposer.disposeManually();
    // This clean() call effectively just removes the Cleaner from the internal linked list of all cleaners.
    // The thunk.run() will be a no-op because cacheDisposer.disposed is already set to true.
    disposerAndCleaner.cleaner.clean();
  }

  @Override
  int cacheCount()
  {
    return cacheCount.get();
  }

  @Override
  void monitor(ServiceEmitter serviceEmitter)
  {
    serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/count", cacheCount()));
    serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/diskSize", tmpFile.length()));
  }
}
