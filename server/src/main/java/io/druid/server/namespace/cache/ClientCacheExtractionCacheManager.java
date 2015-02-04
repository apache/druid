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
import com.google.api.client.util.Preconditions;
import com.google.common.util.concurrent.Striped;
import com.metamx.common.StringUtils;
import com.metamx.common.UOE;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.client.cache.Cache;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Simple wrapper to Cache. Unfortunately Cache has a lot of missing functionality at the time being
 * TODO: rewrite Cache
 */
@JsonTypeName("cache")
public class ClientCacheExtractionCacheManager extends NamespaceExtractionCacheManager
{
  private static final Logger log = new Logger(ClientCacheExtractionCacheManager.class);
  private final Cache cache;

  @JsonCreator
  public ClientCacheExtractionCacheManager(
      @JacksonInject Cache cache,
      @JacksonInject Lifecycle lifecycle
  )
  {
    super(lifecycle);
    this.cache = cache;
  }

  @Override
  public void delete(String ns)
  {
    super.delete(ns);
    cache.close(ns);
  }

  @Override
  public ConcurrentMap<String, String> getCacheMap(String namespace)
  {
    return makeMap(namespace);
  }

  @Override
  public Collection<String> getKnownNamespaces()
  {
    return cache.getNamespaces();
  }

  private static final Cache.NamedKey getKey(final String namespace, final String key)
  {
    return new Cache.NamedKey(namespace, StringUtils.toUtf8(key));
  }

  private static final boolean keyCheck(Object key)
  {
    Preconditions.checkNotNull(key);
    if (!(key instanceof String)) {
      log.warn("Must be String, cannot use [%s]", key.getClass().getCanonicalName());
      return false;
    }
    return true;
  }

  private static final String sanitizeString(byte[] prior)
  {
    if (prior == null) {
      return null;
    }
    final String sPrior = StringUtils.fromUtf8(prior);
    return sPrior.isEmpty() ? null : sPrior;
  }

  private ConcurrentMap<String, String> makeMap(final String namespace)
  {
    // Shitty concurrentMap.
    // Isn't actually concurrent across the cluster
    return new ConcurrentMap<String, String>()
    {
      Striped<ReadWriteLock> striped = Striped.readWriteLock(64);

      @Override
      public String putIfAbsent(String key, String value)
      {
        final ReadWriteLock lock = striped.get(key);
        lock.writeLock().lock();
        try {
          final Cache.NamedKey namedKey = getKey(namespace, key);
          final byte[] prior = cache.get(namedKey);
          if (prior != null) {
            return sanitizeString(prior);
          }
          cache.put(getKey(namespace, key), StringUtils.toUtf8(value));
          return null;
        }
        finally {
          lock.writeLock().unlock();
        }
      }

      @Override
      public boolean remove(Object key, Object value)
      {
        throw new UOE("Cannot remove");
      }

      @Override
      public boolean replace(String key, String oldValue, String newValue)
      {
        throw new UOE("Cannot atomically replace");
      }

      @Override
      public String replace(String key, String value)
      {
        final Cache.NamedKey namedKey = getKey(namespace, key);
        final ReadWriteLock lock = striped.get(key);
        lock.writeLock().lock();
        try {
          final byte[] prior = cache.get(namedKey);
          cache.put(namedKey, StringUtils.toUtf8(value));
          return prior == null ? null : StringUtils.fromUtf8(prior);
        }
        finally {
          lock.writeLock().unlock();
        }
      }

      @Override
      public int size()
      {
        throw new UOE("Cannot determine cache size");
      }

      @Override
      public boolean isEmpty()
      {
        throw new UOE("Cannot determine cache size");
      }

      @Override
      public boolean containsKey(Object key)
      {
        if (!keyCheck(key)) {
          return false;
        }
        final String sKey = (String) key;
        final ReadWriteLock lock = striped.get(sKey);
        lock.readLock().lock();
        try {
          return cache.get(getKey(namespace, sKey)) != null;
        }
        finally {
          lock.readLock().unlock();
        }
      }

      @Override
      public boolean containsValue(Object value)
      {
        throw new UOE("Cannot check for value");
      }

      @Override
      public String get(Object key)
      {
        if (!keyCheck(key)) {
          return null;
        }
        final String sKey = (String) key;
        final ReadWriteLock lock = striped.get(sKey);
        lock.readLock().lock();
        try {
          final byte[] bytes = cache.get(getKey(namespace, sKey));
          if (bytes == null) {
            return null;
          }
          return StringUtils.fromUtf8(bytes);
        }
        finally {
          lock.readLock().unlock();
        }
      }

      @Override
      public String put(String key, String value)
      {
        final ReadWriteLock lock = striped.get(key);
        lock.writeLock().lock();
        try {
          final Cache.NamedKey namedKey = getKey(namespace, key);
          final byte[] prior = cache.get(namedKey);
          cache.put(namedKey, StringUtils.toUtf8(value));
          return prior == null ? null : StringUtils.fromUtf8(prior);
        }
        finally {
          lock.writeLock().unlock();
        }
      }

      @Override
      public String remove(Object key)
      {
        throw new UOE("Cannot remove");
      }

      @Override
      public void putAll(Map<? extends String, ? extends String> m)
      {
        for (Entry<? extends String, ? extends String> entry : m.entrySet()) {
          put(entry.getKey(), entry.getValue());
        }
      }

      @Override
      public void clear()
      {
        cache.close(namespace);
      }

      @Override
      public Set<String> keySet()
      {
        throw new UOE("Cannot get key set");
      }

      @Override
      public Collection<String> values()
      {
        throw new UOE("Cannot get value set");
      }

      @Override
      public Set<Entry<String, String>> entrySet()
      {
        throw new UOE("Cannot get entry set");
      }
    };
  }
}
