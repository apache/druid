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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Striped;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.io.IOException;
import java.util.List;
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
  private Striped<Lock> nsLocks = Striped.lock(32); // Needed to make sure delete() doesn't do weird things
  private final File tmpFile;

  @Inject
  public OffHeapNamespaceExtractionCacheManager(
      Lifecycle lifecycle,
      @Named("namespaceExtractionFunctionCache")
      ConcurrentMap<String, Function<String, String>> fnCache,
      @Named("namespaceReverseExtractionFunctionCache")
      ConcurrentMap<String, Function<String, List<String>>> reverseFnCache,
      ServiceEmitter emitter,
      final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>> namespaceFunctionFactoryMap
  )
  {
    super(lifecycle, fnCache, reverseFnCache, emitter, namespaceFunctionFactoryMap);
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
    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            // NOOP
          }

          @Override
          public void stop()
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
        mmapDB.delete(priorCache);
        dataSize.set(tmpFile.length());
        return true;
      } else {
        dataSize.set(tmpFile.length());
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
      super.delete(namespaceKey);
      final String mmapDBkey = currentNamespaceCache.get(namespaceKey);
      if (mmapDBkey != null) {
        final long pre = tmpFile.length();
        mmapDB.delete(mmapDBkey);
        dataSize.set(tmpFile.length());
        log.debug("MapDB file size: pre %d  post %d", pre, dataSize.get());
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
  public ConcurrentMap<String, String> getCacheMap(String namespaceOrCacheKey)
  {
    final Lock lock = nsLocks.get(namespaceOrCacheKey);
    lock.lock();
    try {
      String realKey = currentNamespaceCache.get(namespaceOrCacheKey);
      if (realKey == null) {
        realKey = namespaceOrCacheKey;
      }
      final Lock nsLock = nsLocks.get(realKey);
      if (lock != nsLock) {
        nsLock.lock();
      }
      try {
        return mmapDB.createHashMap(realKey).makeOrGet();
      }
      finally {
        if (lock != nsLock) {
          nsLock.unlock();
        }
      }
    }
    finally {
      lock.unlock();
    }
  }
}
