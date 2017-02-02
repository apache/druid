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

import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.druid.common.utils.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class CacheKeyBuilder
{
  static final byte[] DEFAULT_SEPARATOR = new byte[]{(byte) 0xFF};
  public static final byte[] EMPTY_BYTES = new byte[0];

  private static byte[] byteToByteArray(byte input)
  {
    return new byte[]{input};
  }

  private static byte[] stringToByteArray(String input)
  {
    return StringUtils.toUtf8WithNullToEmpty(input);
  }

  private static byte[] booleanToByteArray(boolean input)
  {
    return new byte[]{(byte) (input ? 1 : 0)};
  }

  private static byte[] intToByteArray(int input)
  {
    return Ints.toByteArray(input);
  }

  private static byte[] floatToByteArray(float input)
  {
    return ByteBuffer.allocate(Floats.BYTES).putFloat(input).array();
  }

  private static byte[] floatArrayToByteArray(float[] input)
  {
    final ByteBuffer buffer = ByteBuffer.allocate(Floats.BYTES * input.length);
    buffer.asFloatBuffer().put(input);
    return buffer.array();
  }

  private static byte[] doubleToByteArray(double input)
  {
    return ByteBuffer.allocate(Doubles.BYTES).putDouble(input).array();
  }

  private final List<byte[]> items = Lists.newArrayList();
  private final byte id;
  private final byte[] separator;
  private int size;

  public CacheKeyBuilder(byte id)
  {
    this(id, DEFAULT_SEPARATOR);
  }

  public CacheKeyBuilder(byte id, byte[] separator)
  {
    this.id = id;
    this.size = 1;
    this.separator = separator;
  }

  public CacheKeyBuilder appendByte(byte input)
  {
    appendItem(byteToByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendString(@Nullable String input)
  {
    appendItem(stringToByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendStringList(List<String> input)
  {
    for (String eachStr : input) {
      appendItem(stringToByteArray(eachStr));
    }
    return this;
  }

  public CacheKeyBuilder appendBoolean(boolean input)
  {
    appendItem(booleanToByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendInt(int input)
  {
    appendItem(intToByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendFloat(float input)
  {
    appendItem(floatToByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendDouble(double input)
  {
    appendItem(doubleToByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendFloatArray(float[] input)
  {
    appendItem(floatArrayToByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendCacheable(@Nullable Cacheable input)
  {
    appendItem(input == null ? EMPTY_BYTES : input.getCacheKey());
    return this;
  }

  public CacheKeyBuilder appendCacheableList(List<? extends Cacheable> inputs)
  {
    for (Cacheable input : inputs) {
      appendItem(input.getCacheKey());
    }
    return this;
  }

  private void appendItem(byte[] item)
  {
    items.add(item);
    size += item.length;
  }

  public byte[] build()
  {
    final ByteBuffer buffer = ByteBuffer.allocate(size + separator.length * (items.size() - 1));
    buffer.put(id);

    for (int i = 0; i < items.size(); i++) {
      if (i == items.size() - 1) {
        buffer.put(items.get(i));
      } else {
        buffer.put(items.get(i)).put(separator);
      }
    }
    return buffer.array();
  }
}
