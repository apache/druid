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

package org.apache.druid.query.aggregation.cardinality.accurate.collector;


import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.cardinality.accurate.bitmap64.LongMutableBitmap;
import org.apache.druid.query.aggregation.cardinality.accurate.bitmap64.LongRoaringBitmap;

import java.nio.ByteBuffer;

public class LongRoaringBitmapCollector implements LongBitmapCollector
{
  public final LongMutableBitmap bitmap;

  public LongRoaringBitmapCollector(LongMutableBitmap mutableBitmap)
  {
    this.bitmap = mutableBitmap;
  }

  @Override
  public void add(long value)
  {
    this.bitmap.add(value);
  }

  @Override
  public long getCardinality()
  {
    return this.bitmap.size();
  }

  @Override
  public LongBitmapCollector fold(LongBitmapCollector other)
  {
    if (other == null) {
      return this;
    }
    bitmap.or(((LongRoaringBitmapCollector) other).bitmap);
    return this;
  }

  @Override
  public int compareTo(LongBitmapCollector other)
  {
    return Long.compare(this.getCardinality(), other.getCardinality());
  }

  public static LongRoaringBitmapCollector of(Object obj)
  {
    return new LongRoaringBitmapCollector((LongMutableBitmap) obj);
  }

  public static LongRoaringBitmapCollector deserialize(Object serializedCollector)
  {
    if (serializedCollector instanceof String) {
      return LongRoaringBitmapCollector.of(deserializeFromBase64EncodedString((String) serializedCollector));
    } else if (serializedCollector instanceof byte[]) {
      return LongRoaringBitmapCollector.of(deserializeFromByteArray((byte[]) serializedCollector));
    } else if (serializedCollector instanceof LongRoaringBitmapCollector) {
      return (LongRoaringBitmapCollector) serializedCollector;
    } else {
      return null;
    }
  }

  private static LongMutableBitmap deserializeFromBase64EncodedString(String str)
  {
    return deserializeFromByteArray(StringUtils.decodeBase64(StringUtils.toUtf8(str)));
  }

  private static LongMutableBitmap deserializeFromByteArray(byte[] bytes)
  {
    return LongRoaringBitmap.deserializeFromByteArray(bytes);
  }

  @Override
  public ByteBuffer toByteBuffer()
  {
    return ByteBuffer.wrap(bitmap.toBytes());
  }
}
