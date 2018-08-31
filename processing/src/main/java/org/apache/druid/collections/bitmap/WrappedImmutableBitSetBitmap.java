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

import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.NoSuchElementException;

/**
 * WrappedImmutableBitSetBitmap implements ImmutableBitmap for java.util.BitSet
 */
public class WrappedImmutableBitSetBitmap implements ImmutableBitmap
{
  protected final BitSet bitmap;

  public WrappedImmutableBitSetBitmap(BitSet bitmap)
  {
    this.bitmap = bitmap;
  }

  public WrappedImmutableBitSetBitmap()
  {
    this(new BitSet());
  }

  // WARNING: the current implementation of BitSet (1.7) copies the contents of ByteBuffer to
  // on heap!
  // TODO: make a new BitSet implementation which can use ByteBuffers properly.
  public WrappedImmutableBitSetBitmap(ByteBuffer byteBuffer)
  {
    this(BitSet.valueOf(byteBuffer));
  }

  @Override
  public IntIterator iterator()
  {
    return new BitSetIterator();
  }

  @Override
  public boolean get(int value)
  {
    return bitmap.get(value);
  }

  @Override
  public int size()
  {
    return bitmap.cardinality();
  }

  @Override
  public byte[] toBytes()
  {
    return bitmap.toByteArray();
  }

  @Override
  public boolean isEmpty()
  {
    return bitmap.isEmpty();
  }

  public ImmutableBitmap union(ImmutableBitmap otherBitmap)
  {
    WrappedBitSetBitmap retval = new WrappedBitSetBitmap((BitSet) bitmap.clone());
    retval.or((WrappedBitSetBitmap) otherBitmap);
    return retval;
  }

  @Override
  public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
  {
    WrappedBitSetBitmap retval = new WrappedBitSetBitmap((BitSet) bitmap.clone());
    retval.and((WrappedBitSetBitmap) otherBitmap);
    return retval;
  }

  private class BitSetIterator implements IntIterator
  {
    private int nextPos;

    BitSetIterator()
    {
      nextPos = bitmap.nextSetBit(0);
    }

    @Override
    public boolean hasNext()
    {
      return nextPos >= 0;
    }

    @Override
    public int next()
    {
      int pos = nextPos;
      if (pos < 0) {
        throw new NoSuchElementException();
      }
      nextPos = bitmap.nextSetBit(pos + 1);
      return pos;
    }

    @Override
    public IntIterator clone()
    {
      BitSetIterator newIt = new BitSetIterator();
      newIt.nextPos = nextPos;
      return newIt;
    }
  }

}
