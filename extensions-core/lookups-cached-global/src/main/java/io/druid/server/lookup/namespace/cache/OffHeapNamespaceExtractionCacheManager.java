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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Striped;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

/**
 *
 */
public class OffHeapNamespaceExtractionCacheManager extends NamespaceExtractionCacheManager
{
  private static final Logger log = new Logger(OffHeapNamespaceExtractionCacheManager.class);
  private final DB mmapDB;
  private ConcurrentMap<String, String> currentNamespaceCache = new ConcurrentHashMap<>();
  private Striped<Lock> nsLocks = Striped.lazyWeakLock(1024); // Needed to make sure delete() doesn't do weird things
  private final File tmpFile;

  @Inject
  public OffHeapNamespaceExtractionCacheManager(
      Lifecycle lifecycle,
      ServiceEmitter emitter,
      final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>> namespaceFunctionFactoryMap
  )
  {
    super(lifecycle, emitter, namespaceFunctionFactoryMap);
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
  protected boolean swapAndClearCache(String namespaceKey, String cacheKey)
  {
    final Lock lock = nsLocks.get(namespaceKey);
    lock.lock();
    try {
      Preconditions.checkArgument(mmapDB.exists(cacheKey), "Namespace [%s] does not exist", cacheKey);

      final String swapCacheKey = UUID.randomUUID().toString();
      mmapDB.rename(cacheKey, swapCacheKey);

      final String priorCache = currentNamespaceCache.put(namespaceKey, swapCacheKey);
      if (priorCache != null) {
        // TODO: resolve what happens here if query is actively going on
        mmapDB.delete(priorCache);
        return true;
      } else {
        return false;
      }
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public boolean delete(final String namespaceKey)
  {
    final Lock lock = nsLocks.get(namespaceKey);
    lock.lock();
    try {
      if (super.delete(namespaceKey)) {
        final String mmapDBkey = currentNamespaceCache.remove(namespaceKey);
        if (mmapDBkey != null) {
          final long pre = tmpFile.length();
          mmapDB.delete(mmapDBkey);
          log.debug("MapDB file size: pre %d  post %d", pre, tmpFile.length());
          return true;
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public ConcurrentMap<String, String> getCacheMap(String namespaceKey)
  {
    final Lock lock = nsLocks.get(namespaceKey);
    lock.lock();
    try {
      String mapDBKey = currentNamespaceCache.get(namespaceKey);
      if (mapDBKey == null) {
        // Not something created by swapAndClearCache
        mapDBKey = namespaceKey;
      }
      return mmapDB.createHashMap(mapDBKey).makeOrGet();
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  protected void monitor(ServiceEmitter serviceEmitter)
  {
    serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/diskSize", tmpFile.length()));
  }
}
