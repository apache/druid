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
import com.google.common.collect.ImmutableMap;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.ServletResourceUtils;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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

  private static final long DEFAULT_SCHEDULE_TIMEOUT = 60_000;
  private static final byte[] CLASS_CACHE_KEY;

  static {
    final byte[] keyUtf8 = StringUtils.toUtf8(NamespaceLookupExtractorFactory.class.getCanonicalName());
    CLASS_CACHE_KEY = ByteBuffer.allocate(keyUtf8.length + 1).put(keyUtf8).put((byte) 0xFF).array();
  }

  private volatile boolean started = false;
  private final ReadWriteLock startStopSync = new ReentrantReadWriteLock();
  private final NamespaceExtractionCacheManager manager;
  private final LookupIntrospectHandler lookupIntrospectHandler;
  private final ExtractionNamespace extractionNamespace;
  private final long firstCacheTimeout;
  private final boolean injective;

  private final String extractorID;

  @JsonCreator
  public NamespaceLookupExtractorFactory(
      @JsonProperty("extractionNamespace") ExtractionNamespace extractionNamespace,
      @JsonProperty("firstCacheTimeout") Long firstCacheTimeout,
      @JsonProperty("injective") boolean injective,
      @JacksonInject final NamespaceExtractionCacheManager manager
  )
  {
    this.extractionNamespace = Preconditions.checkNotNull(
        extractionNamespace,
        "extractionNamespace should be specified"
    );
    this.firstCacheTimeout = firstCacheTimeout == null ? DEFAULT_SCHEDULE_TIMEOUT : firstCacheTimeout;
    Preconditions.checkArgument(this.firstCacheTimeout >= 0);
    this.injective = injective;
    this.manager = manager;
    this.extractorID = buildID();
    this.lookupIntrospectHandler = new LookupIntrospectHandler()
    {
      @GET
      @Path("/keys")
      @Produces(MediaType.APPLICATION_JSON)
      public Response getKeys()
      {
        try {
          return Response.ok(getLatest().keySet()).build();
        }
        catch (ISE e) {
          return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
        }
      }

      @GET
      @Path("/values")
      @Produces(MediaType.APPLICATION_JSON)
      public Response getValues()
      {
        try {
          return Response.ok(getLatest().values()).build();
        }
        catch (ISE e) {
          return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
        }
      }

      @GET
      @Path("/version")
      @Produces(MediaType.APPLICATION_JSON)
      public Response getVersion()
      {
        final String version = manager.getVersion(extractorID);
        if (null == version) {
          // Handle race between delete and this method being called
          return Response.status(Response.Status.NOT_FOUND).build();
        } else {
          return Response.ok(ImmutableMap.of("version", version)).build();
        }
      }

      @GET
      @Produces(MediaType.APPLICATION_JSON)
      public Response getMap()
      {
        try {
          return Response.ok(getLatest()).build();
        }
        catch (ISE e) {
          return Response.status(Response.Status.NOT_FOUND).entity(ServletResourceUtils.sanitizeException(e)).build();
        }
      }

      private Map<String, String> getLatest()
      {
        return ((MapLookupExtractor) get()).getMap();
      }
    };
  }

  @VisibleForTesting
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
      if (started) {
        LOG.warn("Already started! [%s]", extractorID);
        return true;
      }
      if (!manager.scheduleAndWait(extractorID, extractionNamespace, firstCacheTimeout)) {
        LOG.error("Failed to schedule lookup [%s]", extractorID);
        return false;
      }
      LOG.debug("NamespaceLookupExtractorFactory[%s] started", extractorID);
      started = true;
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
      if (!started) {
        LOG.warn("Not started! [%s]", extractorID);
        return true;
      }
      started = false;
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
      if (!started) {
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
