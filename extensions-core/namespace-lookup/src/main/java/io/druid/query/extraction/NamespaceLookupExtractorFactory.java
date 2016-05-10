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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.lookup.LookupExtractorFactory;
import io.druid.query.lookup.LookupIntrospectHandler;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@JsonTypeName("cachedNamespace")
public class NamespaceLookupExtractorFactory implements LookupExtractorFactory
{
  private static final Logger LOG = new Logger(NamespaceLookupExtractorFactory.class);

  private static final long DEFAULT_SCHEDULE_TIMEOUT = 60_000;
  private static final byte[] CLASS_CACHE_KEY;

  static {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      baos.write(StringUtils.toUtf8(NamespaceLookupExtractorFactory.class.getCanonicalName()));
      baos.write(0xFF);
      CLASS_CACHE_KEY = baos.toByteArray();
    }
    catch (IOException e) {
      // Should never happen
      throw Throwables.propagate(e);
    }
  }

  @JsonIgnore
  private final AtomicBoolean started = new AtomicBoolean(false);
  @JsonIgnore
  private final ReadWriteLock startStopSync = new ReentrantReadWriteLock();
  @JsonIgnore
  private final NamespaceExtractionCacheManager manager;
  @JsonIgnore
  private final LookupIntrospectHandler lookupIntrospectHandler;
  private final ExtractionNamespace extractionNamespace;
  private final long firstCacheTimeout;
  private final boolean oneToOne;

  private final String extractorID;

  @JsonCreator
  public NamespaceLookupExtractorFactory(
      @JsonProperty("extractionNamespace") ExtractionNamespace extractionNamespace,
      @JsonProperty("firstCacheTimeout") Long firstCacheTimeout,
      @JsonProperty("oneToOne") boolean oneToOne,
      @JacksonInject NamespaceExtractionCacheManager manager
  )
  {
    this.extractionNamespace = Preconditions.checkNotNull(
        extractionNamespace,
        "extractionNamespace should be specified"
    );
    this.firstCacheTimeout = firstCacheTimeout == null ? DEFAULT_SCHEDULE_TIMEOUT : firstCacheTimeout;
    Preconditions.checkArgument(this.firstCacheTimeout >= 0);
    this.oneToOne = oneToOne;
    this.manager = manager;
    this.extractorID = buildID();
    this.lookupIntrospectHandler = new LookupIntrospectHandler()
    {
      @GET
      @Path("/keys")
      @Produces(MediaType.APPLICATION_JSON)
      public Response getKeys()
      {
        return Response.ok(getLatest().keySet().toString()).build();
      }

      @GET
      @Path("/values")
      @Produces(MediaType.APPLICATION_JSON)
      public Response getValues()
      {
        return Response.ok(getLatest().values().toString()).build();
      }

      @GET
      @Produces(MediaType.APPLICATION_JSON)
      public Response getMap()
      {
        return Response.ok(getLatest()).build();
      }

      private Map<String, String> getLatest()
      {
        return ((MapLookupExtractor) get()).getMap();
      }
    };
  }

  public NamespaceLookupExtractorFactory(
      ExtractionNamespace extractionNamespace,
      NamespaceExtractionCacheManager manager
  )
  {
    this(extractionNamespace, null, false, manager);
  }

  @Override
  public boolean start()
  {
    final Lock writeLock = startStopSync.writeLock();
    writeLock.lock();
    try {
      if (!started.compareAndSet(false, true)) {
        LOG.warn("Already started! [%s]", extractorID);
        return true;
      }
      if (!manager.scheduleAndWait(extractorID, extractionNamespace, firstCacheTimeout)) {
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
        LOG.warn("Not started! [%s]", extractorID);
        return true;
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
      if (isOneToOne() != ((NamespaceLookupExtractorFactory) other).isOneToOne()) {
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
  public boolean isOneToOne()
  {
    return oneToOne;
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
      return new MapLookupExtractor(map, isOneToOne())
      {
        @Override
        public byte[] getCacheKey()
        {
          return ByteBuffer
              .allocate(CLASS_CACHE_KEY.length + id.length + 1 + v.length + 1 + 1)
              .put(id)
              .put((byte) 0xFF)
              .put(v)
              .put((byte) 0xFF)
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
