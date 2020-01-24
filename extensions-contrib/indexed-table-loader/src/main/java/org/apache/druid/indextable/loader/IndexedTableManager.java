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

package org.apache.druid.indextable.loader;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.indextable.loader.config.IndexedTableConfig;
import org.apache.druid.indextable.loader.config.IndexedTableLoaderConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.join.table.IndexedTable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

/**
 * This class manages all indexed tables.
 *
 * Take inspiration from {@link org.apache.druid.query.lookup.LookupReferencesManager}
 */
@ManageLifecycle
public class IndexedTableManager
{
  private static final String INDEXED_TABLE_LOADER_THREAD_FORMAT = "IndexedTableManager-loader[%d]";
  private static final EmittingLogger LOG = new EmittingLogger(IndexedTableManager.class);

  private final Map<String, IndexedTableConfig> indexedTableLoaders;
  private final ExecutorService loaderExecutorService;
  private final LifecycleLock lifecycleLock;
  private final ConcurrentMap<String, IndexedTable> indexedTables;

  @Inject
  IndexedTableManager(
      IndexedTableLoaderConfig config,
      Map<String, IndexedTableConfig> indexedTableLoaders
  )
  {
    this (
        indexedTableLoaders,
        Execs.multiThreaded(
            config.getNumThreads(),
            INDEXED_TABLE_LOADER_THREAD_FORMAT),
        new LifecycleLock(),
        new ConcurrentHashMap<>()
    );
  }

  @VisibleForTesting
  IndexedTableManager(
      Map<String, IndexedTableConfig> indexedTableLoaders,
      ExecutorService loaderExecutorService,
      LifecycleLock lifecycleLock,
      ConcurrentMap<String, IndexedTable> indexedTables
  )
  {
    this.indexedTableLoaders = indexedTableLoaders;
    this.loaderExecutorService = loaderExecutorService;
    this.lifecycleLock = lifecycleLock;
    this.indexedTables = indexedTables;
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("IndexedTableManager can not start.");
    }
    try {
      indexedTableLoaders.forEach(
          (name, loader) -> loaderExecutorService.submit(
              () -> {
                // TODO: guicify this
                LOG.info("loading index table - %s", name);
                IndexedTableSupplier supplier = new IndexedTableSupplier(
                    loader.getKeys(), loader.getIngestionSpec(), FileUtils::createTempDir
                );
                indexedTables.put(name, supplier.get());
              }
          ));
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  /**
   * Interrupts the loader thread if it is still running
   */
  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }
    // TODO: better clean up of tasks
    try {
      loaderExecutorService.shutdownNow();
    }
    finally {
      lifecycleLock.exitStop();
    }
  }
}
