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

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.java.util.common.StringUtils;
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
        Suppliers.<ResourceHolder<MemcachedClientIF>>ofInstance(
            StupidResourceHolder.create(client)
        ),
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
        }, MemcachedCacheTest.NOOP_MONITOR
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
        cache.put(new Cache.NamedKey(NAMESPACE, StringUtils.toUtf8(key)), randBytes);
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
        bytes = cache.get(new Cache.NamedKey(NAMESPACE, StringUtils.toUtf8(key)));
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
        keys.add(new Cache.NamedKey(NAMESPACE, StringUtils.toUtf8(key)));
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
