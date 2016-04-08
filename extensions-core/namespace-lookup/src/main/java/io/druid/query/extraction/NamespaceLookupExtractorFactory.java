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
package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.lookup.LookupExtractorFactory;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@JsonTypeName("namespace")
public class NamespaceLookupExtractorFactory implements LookupExtractorFactory
{
  private static final Logger LOG = new Logger(NamespaceLookupExtractorFactory.class);

  private static long SCHEDULE_TIMEOUT = 60_000;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final ReadWriteLock startStopSync = new ReentrantReadWriteLock();
  private final ExtractionNamespace extractionNamespace;
  private final NamespaceExtractionCacheManager manager;

  private final String extractorID;

  @JsonCreator
  public NamespaceLookupExtractorFactory(
      @JsonProperty("extractionNamespace") ExtractionNamespace extractionNamespace,
      @JacksonInject NamespaceExtractionCacheManager manager
  )
  {
    this.extractionNamespace = Preconditions.checkNotNull(
        extractionNamespace,
        "extractionNamespace should be specified"
    );
    this.manager = manager;
    this.extractorID = buildID();
  }

  @Override
  public boolean start()
  {
    final Lock writeLock = startStopSync.writeLock();
    writeLock.lock();
    try {
      if (!started.compareAndSet(false, true)) {
        LOG.warn("Already started!");
        return false;
      }
      if (!manager.scheduleAndWait(extractorID, extractionNamespace, SCHEDULE_TIMEOUT)) {
        LOG.warn("Failed to schedule lookup [%s]", extractorID);
        return false;
      }
      LOG.debug("NamespaceLookupExtractorFactory[%s] started", extractorID);
      return true;
    }
    finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean close()
  {
    final Lock writeLock = startStopSync.writeLock();
    writeLock.lock();
    try {
      if (!started.compareAndSet(true, false)) {
        LOG.warn("Not started!");
        return false;
      }
      return manager.checkedDelete(extractorID);
    }
    finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean replaces(@Nullable LookupExtractorFactory other)
  {
    if (other != null && other instanceof NamespaceLookupExtractorFactory) {
      NamespaceLookupExtractorFactory that = (NamespaceLookupExtractorFactory) other;
      return !extractionNamespace.equals(that.extractionNamespace);
    }
    return true;
  }

  @JsonProperty
  public ExtractionNamespace getExtractionNamespace()
  {
    return extractionNamespace;
  }

  private String buildID()
  {
    return UUID.randomUUID().toString();
  }

  // Grab the latest snapshot from the cache manager
  @Override
  public LookupExtractor get()
  {
    final Lock readLock = startStopSync.readLock();
    readLock.lock();
    try {
      if (!started.get()) {
        throw new ISE("Factory [%s] not started", extractorID);
      }
      String preVersion = null, postVersion = null;
      Map<String, String> map = null;
      // Make sure we absolutely know what version of map we grabbed (for caching purposes)
      do {
        preVersion = manager.getVersion(extractorID);
        if (preVersion == null) {
          throw new ISE("Namespace vanished for [%s]", extractorID);
        }
        map = manager.getCacheMap(extractorID);
        postVersion = manager.getVersion(extractorID);
        if (postVersion == null) {
          // We lost some horrible race... make sure we clean up
          manager.delete(extractorID);
          throw new ISE("Lookup [%s] is deleting", extractorID);
        }
      } while (!preVersion.equals(postVersion));
      final byte[] v = StringUtils.toUtf8(postVersion);
      final byte[] id = StringUtils.toUtf8(extractorID);
      return new MapLookupExtractor(map, false)
      {
        @Override
        public byte[] getCacheKey()
        {
          return ByteBuffer
              .allocate(id.length + 1 + v.length + 1)
              .put(id)
              .put((byte) 0xFF)
              .put(v)
              .put((byte) 0xFF)
              .array();
        }
      };
    }
    finally {
      readLock.unlock();
    }
  }
}
