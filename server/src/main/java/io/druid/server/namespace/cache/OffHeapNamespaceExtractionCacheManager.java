/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.namespace.cache;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
@JsonTypeName("offHeap")
public class OffHeapNamespaceExtractionCacheManager extends NamespaceExtractionCacheManager
{
  private static final Logger log = new Logger(OffHeapNamespaceExtractionCacheManager.class);
  private final DB mmapDB;

  @JsonCreator
  public OffHeapNamespaceExtractionCacheManager(
      @JacksonInject Lifecycle lifecycle
  )
  {
    super(lifecycle);
    final File tmpFile;
    try {
      tmpFile = File.createTempFile("druidMapDB", getClass().getCanonicalName());
      tmpFile.deleteOnExit();
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
        .cacheSize(100 << 20) // 100 MB
        .make();
  }

  @Override
  public void delete(final String ns)
  {
    super.delete(ns);
    mmapDB.delete(ns);
  }

  @Override
  public ConcurrentMap<String, String> getCacheMap(String namespace)
  {
    return mmapDB.createHashMap(namespace).makeOrGet();
  }

  @Override
  public Collection<String> getKnownNamespaces()
  {
    return Collections2.transform(
        Collections2.filter(
            mmapDB.getAll().entrySet(),
            new Predicate<Map.Entry<String, Object>>()
            {
              @Override
              public boolean apply(Map.Entry<String, Object> input)
              {
                return input.getValue() instanceof HTreeMap;
              }
            }
        ),
        new Function<Map.Entry<String, Object>, String>()
        {
          @Override
          public String apply(Map.Entry<String, Object> input)
          {
            return input.getKey();
          }
        }
    );
  }
}
