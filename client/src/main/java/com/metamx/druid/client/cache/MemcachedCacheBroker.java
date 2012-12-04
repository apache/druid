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

import net.iharder.base64.Base64;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class MemcachedCacheBroker implements CacheBroker
{
  public static CacheBroker create(final MemcachedCacheBrokerConfig config)
  {
    try {
      return new MemcachedCacheBroker(
        new MemcachedClient(
          new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                                        .setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH)
                                        .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
                                        .setDaemon(true)
                                        .setShouldOptimize(true)
                                        .build(),
          AddrUtil.getAddresses(config.getHosts())
        ),
        config.getTimeout(),
        config.getExpiration()
      );
    } catch(IOException e) {
      throw new RuntimeException(e);
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
  public Cache provideCache(final String identifier)
  {
    return new Cache()
    {
      @Override
      public byte[] get(byte[] key)
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
          return null;
        }
        catch(ExecutionException e) {
          return null;
        }
      }

      @Override
      public void put(byte[] key, byte[] value)
      {
        client.set(computeKey(identifier, key), expiration, value);
      }

      @Override
      public void close()
      {
        // no resources to cleanup
      }
    };
  }

  private String computeKey(String identifier, byte[] key) {
    return identifier + Base64.encodeBytes(key);
  }
}
