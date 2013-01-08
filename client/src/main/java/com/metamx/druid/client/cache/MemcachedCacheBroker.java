/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.client.cache;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import net.iharder.base64.Base64;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.transcoders.SerializingTranscoder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class MemcachedCacheBroker implements CacheBroker
{
  public static MemcachedCacheBroker create(final MemcachedCacheBrokerConfig config)
  {
    try {
      SerializingTranscoder transcoder = new SerializingTranscoder(config.getMaxObjectSize());
      // disable compression
      transcoder.setCompressionThreshold(Integer.MAX_VALUE);

      return new MemcachedCacheBroker(
        new MemcachedClient(
          new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                                        .setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH)
                                        .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
                                        .setDaemon(true)
                                        .setFailureMode(FailureMode.Retry)
                                        .setTranscoder(transcoder)
                                        .setShouldOptimize(true)
                                        .build(),
          AddrUtil.getAddresses(config.getHosts())
        ),
        config.getTimeout(),
        config.getExpiration()
      );
    } catch(IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private final int timeout;
  private final int expiration;

  private final MemcachedClientIF client;

  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);
  private final AtomicLong timeoutCount = new AtomicLong(0);

  MemcachedCacheBroker(MemcachedClientIF client, int timeout, int expiration) {
    this.timeout = timeout;
    this.expiration = expiration;
    this.client = client;
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
        timeoutCount.get()
    );
  }

  @Override
  public byte[] get(String identifier, byte[] key)
  {
    Future<Object> future = client.asyncGet(computeKey(identifier, key));
    try {
      byte[] bytes = (byte[]) future.get(timeout, TimeUnit.MILLISECONDS);
      if(bytes != null) {
        hitCount.incrementAndGet();
      }
      else {
        missCount.incrementAndGet();
      }
      return bytes;
    }
    catch(TimeoutException e) {
      timeoutCount.incrementAndGet();
      future.cancel(false);
      return null;
    }
    catch(InterruptedException e) {
      throw Throwables.propagate(e);
    }
    catch(ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void put(String identifier, byte[] key, byte[] value)
  {
    client.set(computeKey(identifier, key), expiration, value);
  }

  @Override
  public Map<Pair<String, ByteBuffer>, byte[]> getBulk(Iterable<Pair<String, ByteBuffer>> identifierKeyPairs)
  {
    Map<String, Pair<String, ByteBuffer>> keyLookup = Maps.uniqueIndex(
        identifierKeyPairs,
        new Function<Pair<String, ByteBuffer>, String>()
        {
          @Override
          public String apply(
              @Nullable Pair<String, ByteBuffer> input
          )
          {
            return computeKey(input.lhs, input.rhs.array());
          }
        }
    );

    BulkFuture<Map<String, Object>> future = client.asyncGetBulk(keyLookup.keySet());

    try {
      Map<String, Object> some = future.getSome(timeout, TimeUnit.MILLISECONDS);

      if(future.isTimeout()) {
        future.cancel(false);
        timeoutCount.incrementAndGet();
      }
      missCount.addAndGet(keyLookup.size() - some.size());
      hitCount.addAndGet(some.size());

      Map<Pair<String, ByteBuffer>, byte[]> results = Maps.newHashMap();
      for(Map.Entry<String, Object> entry : some.entrySet()) {
        results.put(
            keyLookup.get(entry.getKey()),
            (byte[])entry.getValue()
        );
      }

      return results;
    }
    catch(InterruptedException e) {
      throw Throwables.propagate(e);
    }
    catch(ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close(String identifier)
  {
    // no resources to cleanup
  }

  private String computeKey(String identifier, byte[] key) {
    return identifier + ":" + Base64.encodeBytes(key, Base64.DONT_BREAK_LINES);
  }
}
