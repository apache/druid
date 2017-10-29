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

package io.druid.collections.bitmap;

import io.druid.java.util.common.IAE;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * WrappedBitSetBitmap implements MutableBitmap for java.util.BitSet
 */
public class WrappedBitSetBitmap extends WrappedImmutableBitSetBitmap implements MutableBitmap
{

  public WrappedBitSetBitmap()
  {
    super();
  }

  public WrappedBitSetBitmap(BitSet bitSet)
  {
    super(bitSet);
  }

  public WrappedBitSetBitmap(ByteBuffer byteBuffer)
  {
    super(byteBuffer);
  }

  protected BitSet cloneBitSet()
  {
    return (BitSet) bitmap.clone();
  }

  @Override
  public void clear()
  {
    bitmap.clear();
  }

  @Override
  public void or(MutableBitmap mutableBitmap)
  {
    if (mutableBitmap instanceof WrappedBitSetBitmap) {
      WrappedBitSetBitmap bitSet = (WrappedBitSetBitmap) mutableBitmap;
      this.bitmap.or(bitSet.bitmap);
    } else {
      throw new IAE(
          "Unknown class type: %s  expected %s",
          mutableBitmap.getClass().getCanonicalName(),
          WrappedBitSetBitmap.class.getCanonicalName()
      );
    }
  }

  @Override
  public void and(MutableBitmap mutableBitmap)
  {
    if (mutableBitmap instanceof WrappedBitSetBitmap) {
      WrappedBitSetBitmap bitSet = (WrappedBitSetBitmap) mutableBitmap;
      this.bitmap.and(bitSet.bitmap);
    } else {
      throw new IAE(
          "Unknown class type: %s  expected %s",
          mutableBitmap.getClass().getCanonicalName(),
          WrappedBitSetBitmap.class.getCanonicalName()
      );
    }
  }

  @Override
  public void xor(MutableBitmap mutableBitmap)
  {
    if (mutableBitmap instanceof WrappedBitSetBitmap) {
      WrappedBitSetBitmap bitSet = (WrappedBitSetBitmap) mutableBitmap;
      this.bitmap.xor(bitSet.bitmap);
    } else {
      throw new IAE(
          "Unknown class type: %s  expected %s",
          mutableBitmap.getClass().getCanonicalName(),
          WrappedBitSetBitmap.class.getCanonicalName()
      );
    }
  }

  @Override
  public void andNot(MutableBitmap mutableBitmap)
  {
    if (mutableBitmap instanceof WrappedBitSetBitmap) {
      WrappedBitSetBitmap bitSet = (WrappedBitSetBitmap) mutableBitmap;
      this.bitmap.andNot(bitSet.bitmap);
    } else {
      throw new IAE(
          "Unknown class type: %s  expected %s",
          mutableBitmap.getClass().getCanonicalName(),
          WrappedBitSetBitmap.class.getCanonicalName()
      );
    }
  }

  @Override
  public int getSizeInBytes()
  {
    // BitSet.size() returns the size in *bits*
    return this.bitmap.size() / Byte.SIZE;
  }

  @Override
  public void add(int entry)
  {
    this.bitmap.set(entry);
  }

  @Override
  public void remove(int entry)
  {
    this.bitmap.clear(entry);
  }

  @Override
  public void serialize(ByteBuffer buffer)
  {
    buffer.put(this.bitmap.toByteArray());
  }
}
