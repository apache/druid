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

package io.druid.query.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.druid.common.utils.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CacheKeyBuilderTest
{
  @Parameters
  public static List<Object[]> params()
  {
    return ImmutableList.of(
        new Object[]{CacheKeyBuilder.DEFAULT_SEPARATOR},
        new Object[]{CacheKeyBuilder.EMPTY_BYTES},
        new Object[]{new byte[] {'\001'}},
        new Object[]{StringUtils.toUtf8("string")}
    );
  }

  private final byte[] separator;

  public CacheKeyBuilderTest(byte[] separator)
  {
    this.separator = separator;
  }

  @Test
  public void testCacheKeyBuilder()
  {
    final Cacheable cacheable = new Cacheable()
    {
      @Override
      public byte[] getCacheKey()
      {
        return new byte[]{10, 20};
      }
    };

    final byte[] actual = new CacheKeyBuilder((byte) 10, separator)
        .appendBoolean(false)
        .appendString("test")
        .appendInt(10)
        .appendFloat(0.1f)
        .appendDouble(2.3)
        .appendFloatArray(new float[]{10.0f, 11.0f})
        .appendStringList(Lists.newArrayList("test1", "test2"))
        .appendCacheable(cacheable)
        .appendCacheable(null)
        .build();

    final int expectedSize = 1                                  // id
                             + 1                                // bool
                             + 4                                // 'test'
                             + Ints.BYTES                       // 10
                             + Floats.BYTES                     // 0.1f
                             + Doubles.BYTES                    // 2.3
                             + Floats.BYTES * 2                 // 10.0f, 11.0f
                             + 5 * 2                            // 'test1' 'test2'
                             + cacheable.getCacheKey().length   // cacheable
                             + 9 * separator.length;                               // separators
    assertEquals(expectedSize, actual.length);

    final byte[] expected = ByteBuffer.allocate(expectedSize)
                                      .put((byte) 10)
                                      .put((byte) 0)
                                      .put(separator)
                                      .put(StringUtils.toUtf8("test"))
                                      .put(separator)
                                      .putInt(10)
                                      .put(separator)
                                      .putFloat(0.1f)
                                      .put(separator)
                                      .putDouble(2.3)
                                      .put(separator)
                                      .putFloat(10.0f)
                                      .putFloat(11.0f)
                                      .put(separator)
                                      .put(StringUtils.toUtf8("test1"))
                                      .put(separator)
                                      .put(StringUtils.toUtf8("test2"))
                                      .put(separator)
                                      .put(cacheable.getCacheKey())
                                      .put(separator)
                                      .array();

    assertTrue(Arrays.equals(expected, actual));
  }

  @Test
  public void testStrings()
  {
    final byte[] actual = new CacheKeyBuilder((byte) 10, separator)
        .appendString("test")
        .appendString("test")
        .appendStringList(Lists.newArrayList("test", "test"))
        .build();

    final byte[] expected = ByteBuffer.allocate(actual.length)
                                      .put((byte) 10)
                                      .put(StringUtils.toUtf8("test"))
                                      .put(separator)
                                      .put(StringUtils.toUtf8("test"))
                                      .put(separator)
                                      .put(StringUtils.toUtf8("test"))
                                      .put(separator)
                                      .put(StringUtils.toUtf8("test"))
                                      .array();

    assertTrue(Arrays.equals(expected, actual));
  }

  @Test
  public void testNotEqualStrings()
  {
    final byte[] key1 = new CacheKeyBuilder((byte) 10, separator)
        .appendString("test")
        .appendString("test")
        .build();

    final byte[] key2 = new CacheKeyBuilder((byte) 10, separator)
        .appendString("testtest")
        .build();

    if (Arrays.equals(separator, CacheKeyBuilder.EMPTY_BYTES)) {
      assertTrue(Arrays.equals(key1, key2));
    } else {
      assertFalse(Arrays.equals(key1, key2));
    }
  }
}
