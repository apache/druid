/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.client.cache;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.UOE;
import com.metamx.common.logger.Logger;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.ops.LinkedOperationQueueFactory;
import net.spy.memcached.ops.OperationQueueFactory;
import org.apache.commons.codec.digest.DigestUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class MemcachedCache implements Cache
{
  private static final Logger log = new Logger(MemcachedCache.class);

  public static MemcachedCache create(final MemcachedCacheConfig config)
  {
    try {
      LZ4Transcoder transcoder = new LZ4Transcoder(config.getMaxObjectSize());

      // always use compression
      transcoder.setCompressionThreshold(0);

      OperationQueueFactory opQueueFactory;
      long maxQueueBytes = config.getMaxOperationQueueSize();
      if(maxQueueBytes > 0) {
        opQueueFactory = new MemcachedOperationQueueFactory(maxQueueBytes);
      } else {
        opQueueFactory = new LinkedOperationQueueFactory();
      }

      return new MemcachedCache(
        new MemcachedClient(
          new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                                        .setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH)
                                        .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
                                        .setDaemon(true)
                                        .setFailureMode(FailureMode.Cancel)
                                        .setTranscoder(transcoder)
                                        .setShouldOptimize(true)
                                        .setOpQueueMaxBlockTime(config.getTimeout())
                                        .setOpTimeout(config.getTimeout())
                                        .setReadBufferSize(config.getReadBufferSize())
                                        .setOpQueueFactory(opQueueFactory)
                                        .build(),
          AddrUtil.getAddresses(config.getHosts())
        ),
        config
      );
    } catch(IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private final int timeout;
  private final int expiration;
  private final String memcachedPrefix;

  private final MemcachedClientIF client;

  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);
  private final AtomicLong timeoutCount = new AtomicLong(0);
  private final AtomicLong errorCount = new AtomicLong(0);


  MemcachedCache(MemcachedClientIF client,  MemcachedCacheConfig config) {
    Preconditions.checkArgument(config.getMemcachedPrefix().length() <= MAX_PREFIX_LENGTH,
                                "memcachedPrefix length [%d] exceeds maximum length [%d]",
                                config.getMemcachedPrefix().length(),
                                MAX_PREFIX_LENGTH);
    this.timeout = config.getTimeout();
    this.expiration = config.getExpiration();
    this.client = client;
    this.memcachedPrefix = config.getMemcachedPrefix();
  }

  @Override
  public CacheStats getStats()
  {
    return new CacheStats(
        hitCount.get(),
        missCount.get(),
        0,
        0,
        0,
        timeoutCount.get(),
        errorCount.get()
    );
  }

  @Override
  public byte[] get(NamedKey key)
  {
    Future<Object> future;
    try {
      future = client.asyncGet(computeKeyHash(memcachedPrefix, key));
    } catch(IllegalStateException e) {
      // operation did not get queued in time (queue is full)
      errorCount.incrementAndGet();
      log.warn(e, "Unable to queue cache operation");
      return null;
    }
    try {
      byte[] bytes = (byte[]) future.get(timeout, TimeUnit.MILLISECONDS);
      if(bytes != null) {
        hitCount.incrementAndGet();
      }
      else {
        missCount.incrementAndGet();
      }
      return bytes == null ? null : deserializeValue(key, bytes);
    }
    catch(TimeoutException e) {
      timeoutCount.incrementAndGet();
      future.cancel(false);
      return null;
    }
    catch(InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    catch(ExecutionException e) {
      errorCount.incrementAndGet();
      log.warn(e, "Exception pulling item from cache");
      return null;
    }
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    try {
      client.set(computeKeyHash(memcachedPrefix, key), expiration, serializeValue(key, value));
    } catch(IllegalStateException e) {
      // operation did not get queued in time (queue is full)
      errorCount.incrementAndGet();
      log.warn(e, "Unable to queue cache operation");
    }
  }

  private static byte[] serializeValue(NamedKey key, byte[] value) {
    byte[] keyBytes = key.toByteArray();
    return ByteBuffer.allocate(Ints.BYTES + keyBytes.length + value.length)
                     .putInt(keyBytes.length)
                     .put(keyBytes)
                     .put(value)
                     .array();
  }

  private static byte[] deserializeValue(NamedKey key, byte[] bytes) {
    ByteBuffer buf = ByteBuffer.wrap(bytes);

    final int keyLength = buf.getInt();
    byte[] keyBytes = new byte[keyLength];
    buf.get(keyBytes);
    byte[] value = new byte[buf.remaining()];
    buf.get(value);

    Preconditions.checkState(Arrays.equals(keyBytes, key.toByteArray()),
                             "Keys do not match, possible hash collision?");
    return value;
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    Map<String, NamedKey> keyLookup = Maps.uniqueIndex(
        keys,
        new Function<NamedKey, String>()
        {
          @Override
          public String apply(
              @Nullable NamedKey input
          )
          {
            return computeKeyHash(memcachedPrefix, input);
          }
        }
    );

    Map<NamedKey, byte[]> results = Maps.newHashMap();

    BulkFuture<Map<String, Object>> future;
    try {
      future = client.asyncGetBulk(keyLookup.keySet());
    } catch(IllegalStateException e) {
      // operation did not get queued in time (queue is full)
      errorCount.incrementAndGet();
      log.warn(e, "Unable to queue cache operation");
      return results;
    }

    try {
      Map<String, Object> some = future.getSome(timeout, TimeUnit.MILLISECONDS);

      if(future.isTimeout()) {
        future.cancel(false);
        timeoutCount.incrementAndGet();
      }
      missCount.addAndGet(keyLookup.size() - some.size());
      hitCount.addAndGet(some.size());

      for(Map.Entry<String, Object> entry : some.entrySet()) {
        final NamedKey key = keyLookup.get(entry.getKey());
        final byte[] value = (byte[]) entry.getValue();
        results.put(
            key,
            value == null ? null : deserializeValue(key, value)
        );
      }

      return results;
    }
    catch(InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    catch(ExecutionException e) {
      errorCount.incrementAndGet();
      log.warn(e, "Exception pulling item from cache");
      return results;
    }
  }

  @Override
  public void close(String namespace)
  {
    // no resources to cleanup
  }

  public static final int MAX_PREFIX_LENGTH =
        MemcachedClientIF.MAX_KEY_LENGTH
        - 40 // length of namespace hash
        - 40 // length of key hash
        - 2  // length of separators
        ;

  private static String computeKeyHash(String memcachedPrefix, NamedKey key) {
    // hash keys to keep things under 250 characters for memcached
    return memcachedPrefix + ":" + DigestUtils.sha1Hex(key.namespace) + ":" + DigestUtils.sha1Hex(key.key);
  }

  public boolean isLocal() {
    return false;
  }

  @Override
  public Collection<String> getNamespaces()
  {
    throw new UOE("Cannot get namespaces on memcache");
  }
}
