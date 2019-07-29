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

package org.apache.druid.query.aggregation.cardinality.accurate.bitmap64;

import com.google.common.base.Throwables;
import org.roaringbitmap.BitmapDataProviderSupplier;
import org.roaringbitmap.RoaringBitmapSupplier;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;


public class LongImmutableRoaringBitmap implements LongImmutableBitmap
{
  protected InnerRoaringBitmap64 underlyingBitmap;

  public LongImmutableRoaringBitmap()
  {
    this(new InnerRoaringBitmap64());
  }

  public LongImmutableRoaringBitmap(InnerRoaringBitmap64 underlyingBitmap)
  {
    this.underlyingBitmap = underlyingBitmap;
  }

  @Override
  public LongIterator iterator()
  {
    return underlyingBitmap.getLongIterator();
  }

  @Override
  public long size()
  {
    return underlyingBitmap.getLongCardinality();
  }

  @Override
  public byte[] toBytes()
  {
    try {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      underlyingBitmap.serialize(new DataOutputStream(out));
      return out.toByteArray();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isEmpty()
  {
    return underlyingBitmap.isEmpty();
  }

  @Override
  public boolean get(long value)
  {
    return underlyingBitmap.contains(value);
  }

  static class InnerRoaringBitmap64 extends Roaring64NavigableMap
  {
    private static BitmapDataProviderSupplier supplier = new RoaringBitmapSupplier();

    public InnerRoaringBitmap64()
    {
      super(supplier);
    }

    public InnerRoaringBitmap64 copy()
    {
      InnerRoaringBitmap64 copy = new InnerRoaringBitmap64();
      LongIterator iter = getLongIterator();
      while (iter.hasNext()) {
        copy.addLong(iter.next());
      }
      return copy;
    }
  }
}

