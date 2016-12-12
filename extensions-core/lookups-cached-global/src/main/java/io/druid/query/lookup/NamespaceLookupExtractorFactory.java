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

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.server.lookup.namespace.cache.CacheScheduler;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@JsonTypeName("cachedNamespace")
public class NamespaceLookupExtractorFactory implements LookupExtractorFactory
{
  private static final Logger LOG = new Logger(NamespaceLookupExtractorFactory.class);

  private static final byte[] CLASS_CACHE_KEY;

  static {
    final byte[] keyUtf8 = StringUtils.toUtf8(NamespaceLookupExtractorFactory.class.getCanonicalName());
    CLASS_CACHE_KEY = ByteBuffer.allocate(keyUtf8.length + 1).put(keyUtf8).put((byte) 0xFF).array();
  }

  CacheScheduler.Entry entry = null;
  private final ReadWriteLock startStopSync = new ReentrantReadWriteLock();
  private final CacheScheduler cacheScheduler;
  private final LookupIntrospectHandler lookupIntrospectHandler;
  private final ExtractionNamespace extractionNamespace;
  private final long firstCacheTimeout;
  private final boolean injective;

  private final String extractorID;

  @JsonCreator
  public NamespaceLookupExtractorFactory(
      @JsonProperty("extractionNamespace") ExtractionNamespace extractionNamespace,
      @JsonProperty("firstCacheTimeout") long firstCacheTimeout,
      @JsonProperty("injective") boolean injective,
      @JacksonInject final CacheScheduler cacheScheduler
  )
  {
    this.extractionNamespace = Preconditions.checkNotNull(
        extractionNamespace,
        "extractionNamespace should be specified"
    );
    this.firstCacheTimeout = firstCacheTimeout;
    Preconditions.checkArgument(this.firstCacheTimeout >= 0);
    this.injective = injective;
    this.cacheScheduler = cacheScheduler;
    this.extractorID = String.format("namespace-factory-%s-%s", extractionNamespace, UUID.randomUUID().toString());
    this.lookupIntrospectHandler = new NamespaceLookupIntrospectHandler(this);
  }

  @VisibleForTesting
  public NamespaceLookupExtractorFactory(
      ExtractionNamespace extractionNamespace,
      CacheScheduler cacheScheduler
  )
  {
    this(extractionNamespace, 60000, false, cacheScheduler);
  }

  @Override
  public boolean start()
  {
    final Lock writeLock = startStopSync.writeLock();
    try {
      writeLock.lockInterruptibly();
      try {
        if (entry != null) {
          LOG.warn("Already started! [%s]", extractorID);
          return true;
        }
        if (firstCacheTimeout > 0) {
          entry = cacheScheduler.scheduleAndWait(extractionNamespace, firstCacheTimeout);
          if (entry == null) {
            LOG.error("Failed to schedule and wait for lookup [%s]", extractorID);
            return false;
          }
        } else {
          entry = cacheScheduler.schedule(extractionNamespace);
        }
        LOG.debug("NamespaceLookupExtractorFactory[%s] started", extractorID);
        return true;
      }
      finally {
        writeLock.unlock();
      }
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean close()
  {
    final Lock writeLock = startStopSync.writeLock();
    try {
      writeLock.lockInterruptibly();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    try {
      if (entry == null) {
        LOG.warn("Not started! [%s]", extractorID);
        return true;
      }
      entry.close();
      entry = null;
      return true;
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
      if (isInjective() != ((NamespaceLookupExtractorFactory) other).isInjective()) {
        return true;
      }
      if (getFirstCacheTimeout() != ((NamespaceLookupExtractorFactory) other).getFirstCacheTimeout()) {
        return true;
      }
      return !extractionNamespace.equals(that.extractionNamespace);
    }
    return true;
  }

  @Override
  public LookupIntrospectHandler getIntrospectHandler()
  {
    return lookupIntrospectHandler;
  }

  @JsonProperty
  public ExtractionNamespace getExtractionNamespace()
  {
    return extractionNamespace;
  }

  @JsonProperty
  public long getFirstCacheTimeout()
  {
    return firstCacheTimeout;
  }

  @JsonProperty
  public boolean isInjective()
  {
    return injective;
  }

  // Grab the latest snapshot from the CacheScheduler's entry
  @Override
  public LookupExtractor get()
  {
    final Lock readLock = startStopSync.readLock();
    try {
      readLock.lockInterruptibly();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    try {
      if (entry == null) {
        throw new ISE("Factory [%s] not started", extractorID);
      }
      final CacheScheduler.CacheState cacheState = entry.getCacheState();
      if (cacheState instanceof CacheScheduler.NoCache) {
        final String noCacheReason = ((CacheScheduler.NoCache) cacheState).name();
        throw new ISE("%s: %s, extractorID = %s", entry, noCacheReason, extractorID);
      }
      CacheScheduler.VersionedCache versionedCache = (CacheScheduler.VersionedCache) cacheState;
      Map<String, String> map = versionedCache.getCache();
      final byte[] v = StringUtils.toUtf8(versionedCache.getVersion());
      final byte[] id = StringUtils.toUtf8(extractorID);
      return new MapLookupExtractor(map, isInjective())
      {
        @Override
        public byte[] getCacheKey()
        {
          return ByteBuffer
              .allocate(CLASS_CACHE_KEY.length + id.length + 1 + v.length + 1 + 1)
              .put(CLASS_CACHE_KEY)
              .put(id).put((byte) 0xFF)
              .put(v).put((byte) 0xFF)
              .put(isOneToOne() ? (byte) 1 : (byte) 0)
              .array();
        }
      };
    }
    finally {
      readLock.unlock();
    }
  }
}
