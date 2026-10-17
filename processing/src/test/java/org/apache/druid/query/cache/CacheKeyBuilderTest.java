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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    final byte[] separatorLikeBytes = new byte[]{(byte) 0xFF};

    final byte[] actual = new CacheKeyBuilder((byte) 10)
        .appendBoolean(false)
        .appendString("test")
        .appendInt(10)
        .appendLong(Long.MAX_VALUE)
        .appendFloat(0.1f)
        .appendDouble(2.3)
        .appendByteArray(separatorLikeBytes)
        .appendFloatArray(new float[]{10.0f, 11.0f})
        .appendStrings(Lists.newArrayList("test1", "test2"))
        .appendCacheable(cacheable)
        .appendCacheable(null)
        .appendCacheables(Lists.newArrayList(cacheable, null, cacheable))
        .build();

    // Every item is a type key, then the length of its value, then the value; every element of a collection is
    // likewise preceded by its own length.
    final int itemHeader = 1 + Integer.BYTES;
    final int expectedSize =
        1                                                            // id
        + itemHeader + 1                                             // bool
        + itemHeader + 4                                             // 'test'
        + itemHeader + Integer.BYTES                                 // 10
        + itemHeader + Long.BYTES                                    // Long.MAX_VALUE
        + itemHeader + Float.BYTES                                   // 0.1f
        + itemHeader + Double.BYTES                                  // 2.3
        + itemHeader + 1                                             // byte array
        + itemHeader + Float.BYTES * 2                               // 10.0f, 11.0f
        + itemHeader + (Integer.BYTES + 5 * 2 + 1)                   // 'test1' 0xFF 'test2'
        + itemHeader + 2                                             // cacheable
        + itemHeader                                                 // null cacheable
        + itemHeader + (Integer.BYTES + (Integer.BYTES + 2) + Integer.BYTES + (Integer.BYTES + 2)); // cacheable list
    Assertions.assertEquals(expectedSize, actual.length);

    final byte[] expected = ByteBuffer.allocate(expectedSize)
                                      .put((byte) 10)
                                      .put(CacheKeyBuilder.BOOLEAN_KEY)
                                      .putInt(1)
                                      .put((byte) 0)
                                      .put(CacheKeyBuilder.STRING_KEY)
                                      .putInt(4)
                                      .put(StringUtils.toUtf8("test"))
                                      .put(CacheKeyBuilder.INT_KEY)
                                      .putInt(Integer.BYTES)
                                      .putInt(10)
                                      .put(CacheKeyBuilder.LONG_KEY)
                                      .putInt(Long.BYTES)
                                      .putLong(Long.MAX_VALUE)
                                      .put(CacheKeyBuilder.FLOAT_KEY)
                                      .putInt(Float.BYTES)
                                      .putFloat(0.1f)
                                      .put(CacheKeyBuilder.DOUBLE_KEY)
                                      .putInt(Double.BYTES)
                                      .putDouble(2.3)
                                      .put(CacheKeyBuilder.BYTE_ARRAY_KEY)
                                      .putInt(separatorLikeBytes.length)
                                      .put(separatorLikeBytes)
                                      .put(CacheKeyBuilder.FLOAT_ARRAY_KEY)
                                      .putInt(Float.BYTES * 2)
                                      .putFloat(10.0f)
                                      .putFloat(11.0f)
                                      .put(CacheKeyBuilder.STRING_LIST_KEY)
                                      .putInt(Integer.BYTES + 5 * 2 + 1)
                                      .putInt(2)
                                      .put(StringUtils.toUtf8("test1"))
                                      .put(CacheKeyBuilder.STRING_SEPARATOR)
                                      .put(StringUtils.toUtf8("test2"))
                                      .put(CacheKeyBuilder.CACHEABLE_KEY)
                                      .putInt(2)
                                      .put(cacheable.getCacheKey())
                                      .put(CacheKeyBuilder.CACHEABLE_KEY)
                                      .putInt(0)
                                      .put(CacheKeyBuilder.CACHEABLE_LIST_KEY)
                                      .putInt(Integer.BYTES + (Integer.BYTES + 2) + Integer.BYTES + (Integer.BYTES + 2))
                                      .putInt(3)
                                      .putInt(2)
                                      .put(cacheable.getCacheKey())
                                      .putInt(0)
                                      .putInt(2)
                                      .put(cacheable.getCacheKey())
                                      .array();

    Assertions.assertArrayEquals(expected, actual);
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

    Assertions.assertArrayEquals(key1, key2);

    final Cacheable cacheable1 = () -> new byte[]{1};

    final Cacheable cacheable2 = () -> new byte[]{2};

    key1 = new CacheKeyBuilder((byte) 10)
        .appendCacheablesIgnoringOrder(Lists.newArrayList(cacheable1, cacheable2))
        .build();

    key2 = new CacheKeyBuilder((byte) 10)
        .appendCacheablesIgnoringOrder(Lists.newArrayList(cacheable2, cacheable1))
        .build();

    Assertions.assertArrayEquals(key1, key2);
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
        Assertions.assertFalse(Arrays.equals(keys.get(i), keys.get(j)));
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

    Assertions.assertFalse(Arrays.equals(key1, key2));

    key1 = new CacheKeyBuilder((byte) 10)
        .appendStrings(Collections.singletonList(""))
        .build();

    key2 = new CacheKeyBuilder((byte) 10)
        .appendStrings(Collections.singletonList(null))
        .build();

    Assertions.assertArrayEquals(key1, key2);
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

    Assertions.assertFalse(Arrays.equals(key1, key2));
  }

  @Test
  public void testIgnoringOrder()
  {
    final int stringListSize = Integer.BYTES + (2 + 5 + 5) + CacheKeyBuilder.STRING_SEPARATOR.length * 2;
    final int cacheableListSize = Integer.BYTES + (Integer.BYTES + 2) + (Integer.BYTES + 5) * 2;

    byte[] actual = new CacheKeyBuilder((byte) 10)
        .appendStringsIgnoringOrder(Lists.newArrayList("test2", "test1", "te"))
        .build();

    byte[] expected = ByteBuffer.allocate(1 + 1 + Integer.BYTES + stringListSize)
                                .put((byte) 10)
                                .put(CacheKeyBuilder.STRING_LIST_KEY)
                                .putInt(stringListSize)
                                .putInt(3)
                                .put(StringUtils.toUtf8("te"))
                                .put(CacheKeyBuilder.STRING_SEPARATOR)
                                .put(StringUtils.toUtf8("test1"))
                                .put(CacheKeyBuilder.STRING_SEPARATOR)
                                .put(StringUtils.toUtf8("test2"))
                                .array();

    Assertions.assertArrayEquals(expected, actual);

    final Cacheable c1 = () -> StringUtils.toUtf8("te");

    final Cacheable c2 = () -> StringUtils.toUtf8("test1");

    final Cacheable c3 = () -> StringUtils.toUtf8("test2");

    actual = new CacheKeyBuilder((byte) 10)
        .appendCacheablesIgnoringOrder(Lists.newArrayList(c3, c2, c1))
        .build();

    expected = ByteBuffer.allocate(1 + 1 + Integer.BYTES + cacheableListSize)
                         .put((byte) 10)
                         .put(CacheKeyBuilder.CACHEABLE_LIST_KEY)
                         .putInt(cacheableListSize)
                         .putInt(3)
                         .putInt(2)
                         .put(c1.getCacheKey())
                         .putInt(5)
                         .put(c2.getCacheKey())
                         .putInt(5)
                         .put(c3.getCacheKey())
                         .array();

    Assertions.assertArrayEquals(expected, actual);
  }
}
