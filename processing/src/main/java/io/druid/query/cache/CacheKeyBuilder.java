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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.Cacheable;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * CacheKeyBuilder is a tool for easily generating cache keys of {@link Cacheable} objects.
 *
 * The layout of the serialized cache key is like below.
 *
 * +--------------------------------------------------------+
 * | ID (1 byte)                                            |
 * | type key (1 byte) | serialized value (variable length) |
 * | type key (1 byte) | serialized value (variable length) |
 * | ...                                                    |
 * +--------------------------------------------------------+
 *
 */
public class CacheKeyBuilder
{
  static final byte BYTE_KEY = 0;
  static final byte BYTE_ARRAY_KEY = 1;
  static final byte BOOLEAN_KEY = 2;
  static final byte INT_KEY = 3;
  static final byte FLOAT_KEY = 4;
  static final byte FLOAT_ARRAY_KEY = 5;
  static final byte DOUBLE_KEY = 6;
  static final byte STRING_KEY = 7;
  static final byte STRING_LIST_KEY = 8;
  static final byte CACHEABLE_KEY = 9;
  static final byte CACHEABLE_LIST_KEY = 10;

  static final byte[] STRING_SEPARATOR = new byte[]{(byte) 0xFF};
  static final byte[] EMPTY_BYTES = StringUtils.EMPTY_BYTES;

  private static class Item
  {
    private final byte typeKey;
    private final byte[] item;

    Item(byte typeKey, byte[] item)
    {
      this.typeKey = typeKey;
      this.item = item;
    }

    int byteSize()
    {
      return 1 + item.length;
    }
  }

  private static byte[] floatArrayToByteArray(float[] input)
  {
    final ByteBuffer buffer = ByteBuffer.allocate(Floats.BYTES * input.length);
    buffer.asFloatBuffer().put(input);
    return buffer.array();
  }

  private static byte[] cacheableToByteArray(@Nullable Cacheable cacheable)
  {
    if (cacheable == null) {
      return EMPTY_BYTES;
    } else {
      final byte[] key = cacheable.getCacheKey();
      Preconditions.checkArgument(!Arrays.equals(key, EMPTY_BYTES), "cache key is equal to the empty key");
      return key;
    }
  }

  private static byte[] stringCollectionToByteArray(Collection<String> input, boolean preserveOrder)
  {
    return collectionToByteArray(
        input,
        new Function<String, byte[]>()
        {
          @Override
          public byte[] apply(@Nullable String input)
          {
            return StringUtils.toUtf8WithNullToEmpty(input);
          }
        },
        STRING_SEPARATOR,
        preserveOrder
    );
  }

  private static byte[] cacheableCollectionToByteArray(Collection<? extends Cacheable> input, boolean preserveOrder)
  {
    return collectionToByteArray(
        input,
        new Function<Cacheable, byte[]>()
        {
          @Override
          public byte[] apply(@Nullable Cacheable input)
          {
            return input == null ? EMPTY_BYTES : input.getCacheKey();
          }
        },
        EMPTY_BYTES,
        preserveOrder
    );
  }

  private static <T> byte[] collectionToByteArray(
      Collection<? extends T> collection,
      Function<T, byte[]> serializeFunction,
      byte[] separator,
      boolean preserveOrder
  )
  {
    if (collection.size() > 0) {
      List<byte[]> byteArrayList = Lists.newArrayListWithCapacity(collection.size());
      int totalByteLength = 0;
      for (T eachItem : collection) {
        final byte[] byteArray = serializeFunction.apply(eachItem);
        totalByteLength += byteArray.length;
        byteArrayList.add(byteArray);
      }

      if (!preserveOrder) {
        // Sort the byte array list to guarantee that collections of same items but in different orders make the same result
        Collections.sort(byteArrayList, UnsignedBytes.lexicographicalComparator());
      }

      final Iterator<byte[]> iterator = byteArrayList.iterator();
      final int bufSize = Ints.BYTES + separator.length * (byteArrayList.size() - 1) + totalByteLength;
      final ByteBuffer buffer = ByteBuffer.allocate(bufSize)
                                          .putInt(byteArrayList.size())
                                          .put(iterator.next());

      while (iterator.hasNext()) {
        buffer.put(separator).put(iterator.next());
      }

      return buffer.array();
    } else {
      return EMPTY_BYTES;
    }
  }

  private final List<Item> items = Lists.newArrayList();
  private final byte id;
  private int size;

  public CacheKeyBuilder(byte id)
  {
    this.id = id;
    this.size = 1;
  }

  public CacheKeyBuilder appendByte(byte input)
  {
    appendItem(BYTE_KEY, new byte[]{input});
    return this;
  }

  public CacheKeyBuilder appendByteArray(byte[] input)
  {
    appendItem(BYTE_ARRAY_KEY, input);
    return this;
  }

  public CacheKeyBuilder appendString(@Nullable String input)
  {
    appendItem(STRING_KEY, StringUtils.toUtf8WithNullToEmpty(input));
    return this;
  }

  /**
   * Add a collection of strings to the cache key.
   * Strings in the collection are concatenated with a separator of '0xFF',
   * and they appear in the cache key in their input order.
   *
   * @param input a collection of strings to be included in the cache key
   * @return this instance
   */
  public CacheKeyBuilder appendStrings(Collection<String> input)
  {
    appendItem(STRING_LIST_KEY, stringCollectionToByteArray(input, true));
    return this;
  }

  /**
   * Add a collection of strings to the cache key.
   * Strings in the collection are sorted by their byte representation and
   * concatenated with a separator of '0xFF'.
   *
   * @param input a collection of strings to be included in the cache key
   * @return this instance
   */
  public CacheKeyBuilder appendStringsIgnoringOrder(Collection<String> input)
  {
    appendItem(STRING_LIST_KEY, stringCollectionToByteArray(input, false));
    return this;
  }

  public CacheKeyBuilder appendBoolean(boolean input)
  {
    appendItem(BOOLEAN_KEY, new byte[]{(byte) (input ? 1 : 0)});
    return this;
  }

  public CacheKeyBuilder appendInt(int input)
  {
    appendItem(INT_KEY, Ints.toByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendFloat(float input)
  {
    appendItem(FLOAT_KEY, ByteBuffer.allocate(Floats.BYTES).putFloat(input).array());
    return this;
  }

  public CacheKeyBuilder appendDouble(double input)
  {
    appendItem(DOUBLE_KEY, ByteBuffer.allocate(Doubles.BYTES).putDouble(input).array());
    return this;
  }

  public CacheKeyBuilder appendFloatArray(float[] input)
  {
    appendItem(FLOAT_ARRAY_KEY, floatArrayToByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendCacheable(@Nullable Cacheable input)
  {
    appendItem(CACHEABLE_KEY, cacheableToByteArray(input));
    return this;
  }

  /**
   * Add a collection of Cacheables to the cache key.
   * Cacheables in the collection are concatenated without any separator,
   * and they appear in the cache key in their input order.
   *
   * @param input a collection of Cacheables to be included in the cache key
   * @return this instance
   */
  public CacheKeyBuilder appendCacheables(Collection<? extends Cacheable> input)
  {
    appendItem(CACHEABLE_LIST_KEY, cacheableCollectionToByteArray(input, true));
    return this;
  }

  /**
   * Add a collection of Cacheables to the cache key.
   * Cacheables in the collection are sorted by their byte representation and
   * concatenated without any separator.
   *
   * @param input a collection of Cacheables to be included in the cache key
   * @return this instance
   */
  public CacheKeyBuilder appendCacheablesIgnoringOrder(Collection<? extends Cacheable> input)
  {
    appendItem(CACHEABLE_LIST_KEY, cacheableCollectionToByteArray(input, false));
    return this;
  }

  private void appendItem(byte typeKey, byte[] input)
  {
    final Item item = new Item(typeKey, input);
    items.add(item);
    size += item.byteSize();
  }

  public byte[] build()
  {
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.put(id);

    for (Item item : items) {
      buffer.put(item.typeKey).put(item.item);
    }

    return buffer.array();
  }
}
