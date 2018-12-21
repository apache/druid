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
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class RoaringBitmap extends ImmutableRoaringBitmap implements MutableBitmap
{
  public RoaringBitmap()
  {
    super();
  }

  public RoaringBitmap(InnerRoaringBitmap64 underlyingBitmap)
  {
    super(underlyingBitmap);
  }

  @Override
  public void clear()
  {
    this.underlyingBitmap.clear();
  }

  @Override
  public void or(MutableBitmap mutableBitmap)
  {
    RoaringBitmap other = (RoaringBitmap) mutableBitmap;
    Roaring64NavigableMap unwrappedOtherBitmap = other.underlyingBitmap;
    this.underlyingBitmap.or(unwrappedOtherBitmap);
  }

  @Override
  public void and(MutableBitmap mutableBitmap)
  {
    RoaringBitmap other = (RoaringBitmap) mutableBitmap;
    Roaring64NavigableMap unwrappedOtherBitmap = other.underlyingBitmap;
    this.underlyingBitmap.and(unwrappedOtherBitmap);
  }

  @Override
  public void xor(MutableBitmap mutableBitmap)
  {
    RoaringBitmap other = (RoaringBitmap) mutableBitmap;
    Roaring64NavigableMap unwrappedOtherBitmap = other.underlyingBitmap;
    this.underlyingBitmap.xor(unwrappedOtherBitmap);
  }

  @Override
  public void andNot(MutableBitmap mutableBitmap)
  {
    RoaringBitmap other = (RoaringBitmap) mutableBitmap;
    Roaring64NavigableMap unwrappedOtherBitmap = other.underlyingBitmap;
    this.underlyingBitmap.andNot(unwrappedOtherBitmap);
  }

  @Override
  public long getSizeInBytes()
  {
    return this.underlyingBitmap.getLongSizeInBytes();
  }

  @Override
  public void add(long entry)
  {
    this.underlyingBitmap.add(entry);
  }

  @Override
  public void remove(long entry)
  {
    this.underlyingBitmap.removeLong(entry);
  }

  public ImmutableRoaringBitmap toImmutableBitmap()
  {
    InnerRoaringBitmap64 mrb = this.underlyingBitmap.copy();
    return new ImmutableRoaringBitmap(mrb);
  }

  public static RoaringBitmap deserializeFromByteArray(byte[] bytes)
  {
    InnerRoaringBitmap64 innerRoaringBitmap64 = new InnerRoaringBitmap64();
    try {
      innerRoaringBitmap64.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return new RoaringBitmap(innerRoaringBitmap64);
  }

}
