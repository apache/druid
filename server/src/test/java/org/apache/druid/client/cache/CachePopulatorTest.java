/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.druid.client.cache;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class CachePopulatorTest
{
  private final ExecutorService exec = Execs.multiThreaded(2, "cache-populator-test-%d");
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Cache cache = new MapCache(new ByteCountingLRUMap(Long.MAX_VALUE));
  private final CachePopulatorStats stats = new CachePopulatorStats();

  @After
  public void tearDown()
  {
    exec.shutdownNow();
  }

  @Test
  public void testForegroundPopulator()
  {
    final CachePopulator populator = new ForegroundCachePopulator(objectMapper, stats, -1);
    final List<String> strings = ImmutableList.of("foo", "bar");

    Assert.assertEquals(strings, wrapAndReturn(populator, makeKey(1), strings));
    Assert.assertEquals(strings, readFromCache(makeKey(1)));
    Assert.assertEquals(1, stats.snapshot().getNumOk());
    Assert.assertEquals(0, stats.snapshot().getNumError());
    Assert.assertEquals(0, stats.snapshot().getNumOversized());
  }

  @Test
  public void testForegroundPopulatorMaxEntrySize()
  {
    final CachePopulator populator = new ForegroundCachePopulator(objectMapper, stats, 30);
    final List<String> strings = ImmutableList.of("foo", "bar");
    final List<String> strings2 = ImmutableList.of("foo", "baralararararararaarararararaa");

    Assert.assertEquals(strings, wrapAndReturn(populator, makeKey(1), strings));
    Assert.assertEquals(strings, readFromCache(makeKey(1)));
    Assert.assertEquals(strings2, wrapAndReturn(populator, makeKey(2), strings2));
    Assert.assertNull(readFromCache(makeKey(2)));

    Assert.assertEquals(1, stats.snapshot().getNumOk());
    Assert.assertEquals(0, stats.snapshot().getNumError());
    Assert.assertEquals(1, stats.snapshot().getNumOversized());
  }

  @Test(timeout = 60000L)
  public void testBackgroundPopulator() throws InterruptedException
  {
    final CachePopulator populator = new BackgroundCachePopulator(exec, objectMapper, stats, -1);
    final List<String> strings = ImmutableList.of("foo", "bar");

    Assert.assertEquals(strings, wrapAndReturn(populator, makeKey(1), strings));

    // Wait for background updates to happen.
    while (cache.getStats().getNumEntries() < 1) {
      Thread.sleep(100);
    }

    Assert.assertEquals(strings, readFromCache(makeKey(1)));
    Assert.assertEquals(1, stats.snapshot().getNumOk());
    Assert.assertEquals(0, stats.snapshot().getNumError());
    Assert.assertEquals(0, stats.snapshot().getNumOversized());
  }

  @Test(timeout = 60000L)
  public void testBackgroundPopulatorMaxEntrySize() throws InterruptedException
  {
    final CachePopulator populator = new BackgroundCachePopulator(exec, objectMapper, stats, 30);
    final List<String> strings = ImmutableList.of("foo", "bar");
    final List<String> strings2 = ImmutableList.of("foo", "baralararararararaarararararaa");

    Assert.assertEquals(strings, wrapAndReturn(populator, makeKey(1), strings));
    Assert.assertEquals(strings2, wrapAndReturn(populator, makeKey(2), strings2));

    // Wait for background updates to happen.
    while (cache.getStats().getNumEntries() < 1 || stats.snapshot().getNumOversized() < 1) {
      Thread.sleep(100);
    }

    Assert.assertEquals(strings, readFromCache(makeKey(1)));
    Assert.assertNull(readFromCache(makeKey(2)));
    Assert.assertEquals(1, stats.snapshot().getNumOk());
    Assert.assertEquals(0, stats.snapshot().getNumError());
    Assert.assertEquals(1, stats.snapshot().getNumOversized());
  }

  private static Cache.NamedKey makeKey(final int n)
  {
    return new Cache.NamedKey("test", Ints.toByteArray(n));
  }

  private List<String> wrapAndReturn(
      final CachePopulator populator,
      final Cache.NamedKey key,
      final List<String> strings
  )
  {
    return populator.wrap(Sequences.simple(strings), s -> ImmutableMap.of("s", s), cache, key).toList();
  }

  private List<String> readFromCache(final Cache.NamedKey key)
  {
    final byte[] bytes = cache.get(key);
    if (bytes == null) {
      return null;
    }

    try (
        final MappingIterator<Map<String, String>> iterator = objectMapper.readValues(
            objectMapper.getFactory().createParser(bytes),
            JacksonUtils.TYPE_REFERENCE_MAP_STRING_STRING
        )
    ) {
      final List<Map<String, String>> retVal = new ArrayList<>();
      Iterators.addAll(retVal, iterator);

      // Undo map-wrapping that was done in wrapAndReturn.
      return retVal.stream().map(m -> m.get("s")).collect(Collectors.toList());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
