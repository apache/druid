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

package org.apache.druid.query.aggregation.exact.count.bitmap64;

import org.apache.druid.java.util.common.logger.Logger;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class RoaringBitmap64Counter implements Bitmap64
{
  private static final Logger log = new Logger(RoaringBitmap64Counter.class);

  private final Roaring64NavigableMap bitmap;

  public RoaringBitmap64Counter()
  {
    this.bitmap = new Roaring64NavigableMap();
  }

  private RoaringBitmap64Counter(Roaring64NavigableMap bitmap)
  {
    this.bitmap = bitmap;
  }

  public static RoaringBitmap64Counter fromBytes(byte[] bytes)
  {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    try {
      DataInputStream in = new DataInputStream(inputStream);
      Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
      bitmap.deserialize(in);
      return new RoaringBitmap64Counter(bitmap);
    }
    catch (Exception e) {
      log.error(e, "Failed to deserialize RoaringBitmap64Counter from bytes");
      throw new RuntimeException(e);
    }
  }

  @Override
  public void add(long value)
  {
    bitmap.addLong(value);
  }

  @Override
  public long getCardinality()
  {
    return bitmap.getLongCardinality();
  }

  @Override
  public Bitmap64 fold(Bitmap64 rhs)
  {
    if (rhs != null) {
      bitmap.or(((RoaringBitmap64Counter) rhs).bitmap);
    }

    return this;
  }

  @Override
  public ByteBuffer toByteBuffer()
  {
    bitmap.runOptimize();
    try {
      final ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream();
      bitmap.serialize(new DataOutputStream(out));
      return out.getBuffer();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
