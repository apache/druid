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

package io.druid.client.cache;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.collections.LoadBalancingPool;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import net.jpountz.lz4.LZ4Exception;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Hashing;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class RedisCache implements Cache
{
  private static final Logger log = new Logger(RedisCache.class);

  private final Supplier<ResourceHolder<ShardedJedis>> supplier;
  private final String prefix;
  private final long expiration;
  private final RedisTranscoder transcoder;
  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);
  private final AtomicLong timeoutCount = new AtomicLong(0);
  private final AtomicLong errorCount = new AtomicLong(0);

  public static Cache create(final RedisCacheConfig config)
  {
    final Supplier<ResourceHolder<ShardedJedis>> clientSupplier;

    if (config.getPoolSize() > 1) {
      clientSupplier = new LoadBalancingPool<>(
          config.getPoolSize(),
          new Supplier<ShardedJedis>()
          {
            @Override
            public ShardedJedis get()
            {
              return new ShardedJedis(config.getShardsInfo(), Hashing.MURMUR_HASH);
            }
          }
      );
    } else {
      clientSupplier = Suppliers.<ResourceHolder<ShardedJedis>>ofInstance(
          StupidResourceHolder.create(new ShardedJedis(config.getShardsInfo(), Hashing.MURMUR_HASH))
      );
    }

    return new RedisCache(clientSupplier, config);
  }

  public RedisCache(Supplier<ResourceHolder<ShardedJedis>> supplier, RedisCacheConfig config)
  {
    this.supplier = supplier;
    prefix = config.getPrefix();
    expiration = config.getExpiration();
    transcoder = new RedisTranscoder();
  }

  @Override
  public byte[] get(NamedKey key)
  {
    byte[] result = null;

    try (ResourceHolder<ShardedJedis> clientHolder = supplier.get()) {
      final ShardedJedis jedis = clientHolder.get();
      final byte[] itemKey = CacheImplUtils.computeKeyHash(prefix, key).getBytes();
      final byte[] serialized = jedis.get(itemKey);

      if (serialized != null) {
        result = deserialize(key, serialized);
        hitCount.incrementAndGet();
      }
      else {
        missCount.incrementAndGet();
      }
    }
    catch (IOException | JedisException | LZ4Exception ex) {
      this.countException(ex);
      log.warn(ex, "Error getting cache item");
    }

    return result;
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    final byte[] serialized = serialize(key, value);

    try (ResourceHolder<ShardedJedis> clientHolder = supplier.get()) {
      final ShardedJedis jedis = clientHolder.get();

      final byte[] itemKey = CacheImplUtils.computeKeyHash(prefix, key).getBytes();
      Jedis shard = jedis.getShard(itemKey);

      if (expiration > 0) {
        shard.psetex(itemKey, expiration, serialized);
      }
      else {
        shard.set(itemKey, serialized);
      }

      final byte[] setKey = CacheImplUtils.computeNamespaceHash(prefix, key.namespace).getBytes();
      shard = jedis.getShard(setKey);
      shard.sadd(setKey, key.key);
    }
    catch (IOException | JedisException ex) {
      this.countException(ex);
      log.warn(ex, "Error putting cache item");
    }
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    final Map<byte[], NamedKey> keyLookup = Maps.uniqueIndex(
      keys,
      new Function<NamedKey, byte[]>()
      {
        @Override
        public byte[] apply(
          @Nullable NamedKey input
        )
        {
          return CacheImplUtils.computeKeyHash(prefix, input).getBytes();
        }
      }
    );

    // Sometimes broker passes empty keys list to getBulk()
    if (keyLookup.size() == 0) {
      return ImmutableMap.of();
    }

    final Map<NamedKey, byte[]> results = Maps.newHashMap();
    final byte[][] keysArray = Iterables.toArray(keyLookup.keySet(), byte[].class);
    final List<Object> valuesList;

    try (ResourceHolder<ShardedJedis> clientHolder = supplier.get()) {
      final ShardedJedis jedis = clientHolder.get();
      final ShardedJedisPipeline pipeline = jedis.pipelined();

      for (byte[] itemKey : keysArray) {
        pipeline.get(itemKey);
      }

      pipeline.sync();
      valuesList = pipeline.getResults();
    }
    catch (IOException | JedisException ex) {
      this.countException(ex);
      log.warn(ex, "Unable to get cache items");
      return results;
    }

    final int length = keysArray.length;
    int i = 0;

    while (i < length) {
      final Object value = valuesList.get(i);
      if (value != null) {
        final NamedKey key = keyLookup.get(keysArray[i]);
        try {
          final byte[] deserialized = deserialize(key, (byte []) value);
          hitCount.incrementAndGet();
          results.put(key, deserialized);
        }
        catch (LZ4Exception ex) {
          errorCount.incrementAndGet();
          log.warn(ex, "Error decompressing cache item");
        }
      }
      else {
        missCount.incrementAndGet();
      }

      i++;
    }

    return results;
  }

  @Override
  public void close(String namespace)
  {
    try (ResourceHolder<ShardedJedis> clientHolder = supplier.get()) {
      final ShardedJedis jedis = clientHolder.get();

      final byte[] setKey = CacheImplUtils.computeNamespaceHash(prefix, namespace).getBytes();
      final Set<byte[]> keys = jedis.smembers(setKey);
      if (keys.size() == 0) {
        return;
      }

      keys.add(setKey);
      for (byte[] key : keys) {
        final String itemKey = CacheImplUtils.computeKeyHash(prefix, namespace, key);
        jedis.del(itemKey);
      }
    }
    catch (JedisConnectionException | IOException ex) {
      Throwables.propagate(ex);
    }
    catch (JedisException ex) {
      log.warn(ex, "Error cleaning cache items");
    }
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
  public boolean isLocal()
  {
    return false;
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    //TODO
  }

  private void countException(Exception ex)
  {
    if (isTimeout(ex)) {
      timeoutCount.incrementAndGet();
    }
    else {
      errorCount.incrementAndGet();
    }
  }

  private static boolean isTimeout(Exception ex)
  {
    //FIXME NoSuchElementException is thrown not only in case of timeout
    Throwable thr = ex.getCause();
    return thr instanceof NoSuchElementException || thr instanceof SocketTimeoutException;
  }

  private byte[] deserialize(NamedKey key, byte[] bytes)
  {
    final byte[] data = transcoder.decompress(bytes);
    return CacheImplUtils.deserializeValue(key, data);
  }

  private byte[] serialize(NamedKey key, byte[] value)
  {
    final byte[] data = CacheImplUtils.serializeValue(key, value);
    return transcoder.compress(data);
  }
}
