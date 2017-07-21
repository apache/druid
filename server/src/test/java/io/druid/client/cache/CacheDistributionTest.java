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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.KetamaNodeLocator;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.util.DefaultKetamaNodeLocatorConfiguration;
import org.apache.commons.codec.digest.DigestUtils;
import org.easymock.EasyMock;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(Parameterized.class)
public class CacheDistributionTest
{
  public static final int KEY_COUNT = 1_000_000;

  @Parameterized.Parameters(name = "repetitions={0}, hash={1}")
  public static Iterable<Object[]> data()
  {
    List<HashAlgorithm> hash = ImmutableList.of(
        DefaultHashAlgorithm.FNV1A_64_HASH, DefaultHashAlgorithm.KETAMA_HASH, MemcachedCache.MURMUR3_128
    );
    List<Integer> repetitions = Arrays.asList(160, 500, 1000, 2500, 5000);

    Set<List<Object>> values = Sets.cartesianProduct(
        Sets.newLinkedHashSet(hash),
        Sets.newLinkedHashSet(repetitions)
    );
    return Iterables.transform(
        values, new Function<List<Object>, Object[]>()
        {
          @Nullable
          @Override
          public Object[] apply(List<Object> input)
          {
            return input.toArray();
          }
        }
    );
  }

  final HashAlgorithm hash;
  final int reps;

  @BeforeClass
  public static void header()
  {
    System.out.printf(
        Locale.ENGLISH,
        "%25s\t%5s\t%10s\t%10s\t%10s\t%10s\t%10s\t%7s\t%5s%n",
        "hash", "reps", "node 1", "node 2", "node 3", "node 4", "node 5", "min/max", "ns"
    );
  }

  public CacheDistributionTest(final HashAlgorithm hash, final int reps)
  {
    this.hash = hash;
    this.reps = reps;
  }

  // Run to get a sense of cache key distribution for different ketama reps / hash functions
  // This test is disabled by default because it's a qualitative test not an unit test and thus it have a meaning only
  // when being run and checked by humans.
  @Ignore
  @Test
  public void testDistribution() throws Exception
  {
    KetamaNodeLocator locator = new KetamaNodeLocator(
        ImmutableList.of(
            dummyNode("druid-cache.0001", 11211),
            dummyNode("druid-cache.0002", 11211),
            dummyNode("druid-cache.0003", 11211),
            dummyNode("druid-cache.0004", 11211),
            dummyNode("druid-cache.0005", 11211)
        ),
        hash,
        new DefaultKetamaNodeLocatorConfiguration()
        {
          @Override
          public int getNodeRepetitions()
          {
            return reps;
          }
        }
    );

    Map<MemcachedNode, AtomicLong> counter = Maps.newHashMap();
    long t = 0;
    for(int i = 0; i < KEY_COUNT; ++i) {
      final String k = DigestUtils.sha1Hex("abc" + i) + ":" + DigestUtils.sha1Hex("xyz" + i);
      long t0 = System.nanoTime();
      MemcachedNode node = locator.getPrimary(k);
      t += System.nanoTime() - t0;
      if(counter.containsKey(node)) {
        counter.get(node).incrementAndGet();
      } else {
        counter.put(node, new AtomicLong(1));
      }
    }

    long min = Long.MAX_VALUE;
    long max = 0;
    System.out.printf(Locale.ENGLISH, "%25s\t%5d\t", hash, reps);
    for(AtomicLong count : counter.values()) {
      System.out.printf(Locale.ENGLISH, "%10d\t", count.get());
      min = Math.min(min, count.get());
      max = Math.max(max, count.get());
    }
    System.out.printf(Locale.ENGLISH, "%7.2f\t%5.0f%n", (double) min / (double) max, (double)t / KEY_COUNT);
  }

  private static MemcachedNode dummyNode(String host, int port)
  {
    SocketAddress address = InetSocketAddress.createUnresolved(host, port);
    MemcachedNode node = EasyMock.createNiceMock(MemcachedNode.class);
    EasyMock.expect(node.getSocketAddress()).andReturn(address).anyTimes();
    EasyMock.replay(node);
    return node;
  }
}
