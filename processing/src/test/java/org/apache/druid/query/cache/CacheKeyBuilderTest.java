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

package org.apache.druid.query.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CacheKeyBuilderTest
{
  @Test
  public void testCacheKeyBuilder()
  {
    final Cacheable cacheable = () -> new byte[]{10, 20};

    final byte[] actual = new CacheKeyBuilder((byte) 10)
        .appendBoolean(false)
        .appendString("test")
        .appendInt(10)
        .appendFloat(0.1f)
        .appendDouble(2.3)
        .appendByteArray(CacheKeyBuilder.STRING_SEPARATOR) // test when an item is same with the separator
        .appendFloatArray(new float[]{10.0f, 11.0f})
        .appendStrings(Lists.newArrayList("test1", "test2"))
        .appendCacheable(cacheable)
        .appendCacheable(null)
        .appendCacheables(Lists.newArrayList(cacheable, null, cacheable))
        .build();

    final int expectedSize = 1                                           // id
                             + 1                                         // bool
                             + 4                                         // 'test'
                             + Integer.BYTES                             // 10
                             + Float.BYTES                               // 0.1f
                             + Double.BYTES                              // 2.3
                             + CacheKeyBuilder.STRING_SEPARATOR.length   // byte array
                             + Float.BYTES * 2                           // 10.0f, 11.0f
                             + Integer.BYTES + 5 * 2 + 1                 // 'test1' 'test2'
                             + cacheable.getCacheKey().length            // cacheable
                             + Integer.BYTES + 4                         // cacheable list
                             + 11;                                       // type keys
    Assert.assertEquals(expectedSize, actual.length);

    final byte[] expected = ByteBuffer.allocate(expectedSize)
                                      .put((byte) 10)
                                      .put(CacheKeyBuilder.BOOLEAN_KEY)
                                      .put((byte) 0)
                                      .put(CacheKeyBuilder.STRING_KEY)
                                      .put(StringUtils.toUtf8("test"))
                                      .put(CacheKeyBuilder.INT_KEY)
                                      .putInt(10)
                                      .put(CacheKeyBuilder.FLOAT_KEY)
                                      .putFloat(0.1f)
                                      .put(CacheKeyBuilder.DOUBLE_KEY)
                                      .putDouble(2.3)
                                      .put(CacheKeyBuilder.BYTE_ARRAY_KEY)
                                      .put(CacheKeyBuilder.STRING_SEPARATOR)
                                      .put(CacheKeyBuilder.FLOAT_ARRAY_KEY)
                                      .putFloat(10.0f)
                                      .putFloat(11.0f)
                                      .put(CacheKeyBuilder.STRING_LIST_KEY)
                                      .putInt(2)
                                      .put(StringUtils.toUtf8("test1"))
                                      .put(CacheKeyBuilder.STRING_SEPARATOR)
                                      .put(StringUtils.toUtf8("test2"))
                                      .put(CacheKeyBuilder.CACHEABLE_KEY)
                                      .put(cacheable.getCacheKey())
                                      .put(CacheKeyBuilder.CACHEABLE_KEY)
                                      .put(CacheKeyBuilder.CACHEABLE_LIST_KEY)
                                      .putInt(3)
                                      .put(cacheable.getCacheKey())
                                      .put(cacheable.getCacheKey())
                                      .array();

    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testDifferentOrderList()
  {
    byte[] key1 = new CacheKeyBuilder((byte) 10)
        .appendStringsIgnoringOrder(Lists.newArrayList("AB", "BA"))
        .build();

    byte[] key2 = new CacheKeyBuilder((byte) 10)
        .appendStringsIgnoringOrder(Lists.newArrayList("BA", "AB"))
        .build();

    Assert.assertArrayEquals(key1, key2);

    final Cacheable cacheable1 = () -> new byte[]{1};

    final Cacheable cacheable2 = () -> new byte[]{2};

    key1 = new CacheKeyBuilder((byte) 10)
        .appendCacheablesIgnoringOrder(Lists.newArrayList(cacheable1, cacheable2))
        .build();

    key2 = new CacheKeyBuilder((byte) 10)
        .appendCacheablesIgnoringOrder(Lists.newArrayList(cacheable2, cacheable1))
        .build();

    Assert.assertArrayEquals(key1, key2);
  }

  @Test
  public void testNotEqualStrings()
  {
    final List<byte[]> keys = new ArrayList<>();
    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendString("test")
            .appendString("test")
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendString("testtest")
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendString("testtest")
            .appendString("")
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendString("")
            .appendString("testtest")
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendStrings(ImmutableList.of("test", "test"))
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendStrings(ImmutableList.of("testtest"))
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendStrings(ImmutableList.of("testtest", ""))
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendStrings(ImmutableList.of("testtest"))
            .appendStrings(ImmutableList.of())
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendStrings(ImmutableList.of())
            .appendStrings(ImmutableList.of("testtest"))
            .build()
    );

    assertNotEqualsEachOther(keys);
  }

  @Test
  public void testNotEqualCacheables()
  {
    final Cacheable test = () -> StringUtils.toUtf8("test");

    final Cacheable testtest = () -> StringUtils.toUtf8("testtest");

    final List<byte[]> keys = new ArrayList<>();
    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendCacheable(test)
            .appendCacheable(test)
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendCacheable(testtest)
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendCacheables(Lists.newArrayList(test, test))
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendCacheables(Collections.singletonList(testtest))
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendCacheables(Collections.singletonList(testtest))
            .appendCacheables(new ArrayList<>())
            .build()
    );

    keys.add(
        new CacheKeyBuilder((byte) 10)
            .appendCacheables(new ArrayList<>())
            .appendCacheables(Collections.singletonList(testtest))
            .build()
    );

    assertNotEqualsEachOther(keys);
  }

  private static void assertNotEqualsEachOther(List<byte[]> keys)
  {
    for (int i = 0; i < keys.size(); i++) {
      for (int j = i + 1; j < keys.size(); j++) {
        Assert.assertFalse(Arrays.equals(keys.get(i), keys.get(j)));
      }
    }
  }

  @Test
  public void testEmptyOrNullStringLists()
  {
    byte[] key1 = new CacheKeyBuilder((byte) 10)
        .appendStrings(Lists.newArrayList("", ""))
        .build();

    byte[] key2 = new CacheKeyBuilder((byte) 10)
        .appendStrings(Collections.singletonList(""))
        .build();

    Assert.assertFalse(Arrays.equals(key1, key2));

    key1 = new CacheKeyBuilder((byte) 10)
        .appendStrings(Collections.singletonList(""))
        .build();

    key2 = new CacheKeyBuilder((byte) 10)
        .appendStrings(Collections.singletonList(null))
        .build();

    Assert.assertArrayEquals(key1, key2);
  }

  @Test
  public void testEmptyOrNullCacheables()
  {
    final byte[] key1 = new CacheKeyBuilder((byte) 10)
        .appendCacheables(new ArrayList<>())
        .build();

    final byte[] key2 = new CacheKeyBuilder((byte) 10)
        .appendCacheables(Collections.singletonList(null))
        .build();

    Assert.assertFalse(Arrays.equals(key1, key2));
  }

  @Test
  public void testIgnoringOrder()
  {
    byte[] actual = new CacheKeyBuilder((byte) 10)
        .appendStringsIgnoringOrder(Lists.newArrayList("test2", "test1", "te"))
        .build();

    byte[] expected = ByteBuffer.allocate(20)
                                .put((byte) 10)
                                .put(CacheKeyBuilder.STRING_LIST_KEY)
                                .putInt(3)
                                .put(StringUtils.toUtf8("te"))
                                .put(CacheKeyBuilder.STRING_SEPARATOR)
                                .put(StringUtils.toUtf8("test1"))
                                .put(CacheKeyBuilder.STRING_SEPARATOR)
                                .put(StringUtils.toUtf8("test2"))
                                .array();

    Assert.assertArrayEquals(expected, actual);

    final Cacheable c1 = () -> StringUtils.toUtf8("te");

    final Cacheable c2 = () -> StringUtils.toUtf8("test1");

    final Cacheable c3 = () -> StringUtils.toUtf8("test2");

    actual = new CacheKeyBuilder((byte) 10)
        .appendCacheablesIgnoringOrder(Lists.newArrayList(c3, c2, c1))
        .build();

    expected = ByteBuffer.allocate(18)
                         .put((byte) 10)
                         .put(CacheKeyBuilder.CACHEABLE_LIST_KEY)
                         .putInt(3)
                         .put(c1.getCacheKey())
                         .put(c2.getCacheKey())
                         .put(c3.getCacheKey())
                         .array();

    Assert.assertArrayEquals(expected, actual);
  }
}
