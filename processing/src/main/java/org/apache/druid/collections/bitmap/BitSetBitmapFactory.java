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

package org.apache.druid.collections.bitmap;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * BitSetBitmapFactory implements BitmapFactory as a wrapper for java.util.BitSet
 */
public class BitSetBitmapFactory implements BitmapFactory
{
  @Override
  public MutableBitmap makeEmptyMutableBitmap()
  {
    return new WrappedBitSetBitmap();
  }

  @Override
  public ImmutableBitmap makeEmptyImmutableBitmap()
  {
    return makeEmptyMutableBitmap();
  }

  @Override
  public ImmutableBitmap makeImmutableBitmap(MutableBitmap mutableBitmap)
  {
    return mutableBitmap;
  }

  @Override
  public ImmutableBitmap mapImmutableBitmap(ByteBuffer b)
  {
    return new WrappedBitSetBitmap(BitSet.valueOf(b.array()));
  }

  @Override
  public ImmutableBitmap union(Iterable<ImmutableBitmap> b)
  {
    WrappedBitSetBitmap newSet = null;
    for (ImmutableBitmap bm : b) {
      if (null == newSet) {
        newSet = new WrappedBitSetBitmap(((WrappedBitSetBitmap) bm).cloneBitSet());
      } else {
        newSet.union(bm);
      }
    }
    return newSet;
  }

  @Override
  public ImmutableBitmap intersection(Iterable<ImmutableBitmap> b)
  {

    WrappedBitSetBitmap newSet = null;
    for (ImmutableBitmap bm : b) {
      if (null == newSet) {
        newSet = new WrappedBitSetBitmap(((WrappedBitSetBitmap) bm).cloneBitSet());
      } else {
        newSet.intersection(bm);
      }
    }
    return newSet;
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b, int length)
  {
    return null;
  }
}
