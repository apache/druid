/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.client.cache;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.collect.Lists;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.transcoders.SerializingTranscoder;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MemcachedCacheBenchmark extends SimpleBenchmark
{
  public static final String NAMESPACE = "default";
  private static final String BASE_KEY = "test_2012-11-26T00:00:00.000Z_2012-11-27T00:00:00.000Z_2012-11-27T04:11:25.979Z_";
  private static byte[] randBytes;
  @Param({"localhost:11211"})
  String hosts;
  // object size in kB
  @Param({"1", "5", "10", "40"})
  int objectSize;
  @Param({"100", "1000"})
  int objectCount;
  private MemcachedCache cache;
  private MemcachedClientIF client;

  public static void main(String[] args) throws Exception
  {
    Runner.main(MemcachedCacheBenchmark.class, args);
  }

  @Override
  protected void setUp() throws Exception
  {
    SerializingTranscoder transcoder = new SerializingTranscoder(
        50 * 1024 * 1024 // 50 MB
    );
    // disable compression
    transcoder.setCompressionThreshold(Integer.MAX_VALUE);

    client = new MemcachedClient(
        new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                                      .setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH)
                                      .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
                                      .setDaemon(true)
                                      .setFailureMode(FailureMode.Retry)
                                      .setTranscoder(transcoder)
                                      .setShouldOptimize(true)
                                      .build(),
        AddrUtil.getAddresses(hosts)
    );


    cache = new MemcachedCache(
        client,
        new MemcachedCacheConfig()
        {
          @Override
          public String getMemcachedPrefix()
          {
            return "druid-memcached-benchmark";
          }

          @Override
          public int getTimeout()
          {
            return 30000;
          }

          @Override
          public int getExpiration()
          {
            return 3600;
          }
        }
    );

    randBytes = new byte[objectSize * 1024];
    new Random(0).nextBytes(randBytes);
  }

  @Override
  protected void tearDown() throws Exception
  {
    client.shutdown(1, TimeUnit.MINUTES);
  }

  public void timePutObjects(int reps)
  {
    for (int i = 0; i < reps; ++i) {
      for (int k = 0; k < objectCount; ++k) {
        String key = BASE_KEY + k;
        cache.put(new Cache.NamedKey(NAMESPACE, key.getBytes()), randBytes);
      }
      // make sure the write queue is empty
      client.waitForQueues(1, TimeUnit.HOURS);
    }
  }

  public long timeGetObject(int reps)
  {
    byte[] bytes = null;
    long count = 0;
    for (int i = 0; i < reps; i++) {
      for (int k = 0; k < objectCount; ++k) {
        String key = BASE_KEY + k;
        bytes = cache.get(new Cache.NamedKey(NAMESPACE, key.getBytes()));
        count += bytes.length;
      }
    }
    return count;
  }

  public long timeBulkGetObjects(int reps)
  {
    long count = 0;
    for (int i = 0; i < reps; i++) {
      List<Cache.NamedKey> keys = Lists.newArrayList();
      for (int k = 0; k < objectCount; ++k) {
        String key = BASE_KEY + k;
        keys.add(new Cache.NamedKey(NAMESPACE, key.getBytes()));
      }
      Map<Cache.NamedKey, byte[]> results = cache.getBulk(keys);
      for (Cache.NamedKey key : keys) {
        byte[] bytes = results.get(key);
        count += bytes.length;
      }
    }
    return count;
  }
}
