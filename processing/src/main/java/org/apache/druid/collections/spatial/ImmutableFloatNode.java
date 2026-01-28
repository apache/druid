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

package org.apache.druid.collections.spatial;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Byte layout:
 * Header
 * 0 to 1 : the MSB is a boolean flag for isLeaf, the next 15 bits represent the number of children of a node
 * Body
 * 2 to 2 + numDims * Float.BYTES : minCoordinates
 * 2 + numDims * Float.BYTES to 2 + 2 * numDims * Float.BYTES : maxCoordinates
 * concise set
 * rest (children) : Every 4 bytes is storing an offset representing the position of a child.
 *
 * The child offset is an offset from the initialOffset
 */
public class ImmutableFloatNode implements ImmutableNode<float[]>
{
  public static final int HEADER_NUM_BYTES = 2;

  private final int numDims;
  private final int initialOffset;
  private final int offsetFromInitial;

  private final short numChildren;
  private final boolean isLeaf;
  private final int childrenOffset;

  private final ByteBuffer data;

  private final BitmapFactory bitmapFactory;

  public ImmutableFloatNode(
      int numDims,
      int initialOffset,
      int offsetFromInitial,
      ByteBuffer data,
      BitmapFactory bitmapFactory
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.numDims = numDims;
    this.initialOffset = initialOffset;
    this.offsetFromInitial = offsetFromInitial;
    short header = data.getShort(initialOffset + offsetFromInitial);
    this.isLeaf = (header & 0x8000) != 0;
    this.numChildren = (short) (header & 0x7FFF);
    final int sizePosition = initialOffset + offsetFromInitial + HEADER_NUM_BYTES + 2 * numDims * Float.BYTES;
    int bitmapSize = data.getInt(sizePosition);
    this.childrenOffset = sizePosition
                          + Integer.BYTES
                          + bitmapSize;

    this.data = data;
  }

  public ImmutableFloatNode(
      int numDims,
      int initialOffset,
      int offsetFromInitial,
      short numChildren,
      boolean leaf,
      ByteBuffer data,
      BitmapFactory bitmapFactory
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.numDims = numDims;
    this.initialOffset = initialOffset;
    this.offsetFromInitial = offsetFromInitial;
    this.numChildren = numChildren;
    this.isLeaf = leaf;
    final int sizePosition = initialOffset + offsetFromInitial + HEADER_NUM_BYTES + 2 * numDims * Float.BYTES;
    int bitmapSize = data.getInt(sizePosition);
    this.childrenOffset = sizePosition
                          + Integer.BYTES
                          + bitmapSize;

    this.data = data;
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }

  @Override
  public int getInitialOffset()
  {
    return initialOffset;
  }

  @Override
  public int getOffsetFromInitial()
  {
    return offsetFromInitial;
  }

  @Override
  public int getNumDims()
  {
    return numDims;
  }

  @Override
  public boolean isLeaf()
  {
    return isLeaf;
  }

  @Override
  public float[] getMinCoordinates()
  {
    return getCoords(initialOffset + offsetFromInitial + HEADER_NUM_BYTES);
  }

  @Override
  public float[] getMaxCoordinates()
  {
    return getCoords(initialOffset + offsetFromInitial + HEADER_NUM_BYTES + numDims * Float.BYTES);
  }

  @Override
  public ImmutableBitmap getImmutableBitmap()
  {
    final int sizePosition = initialOffset + offsetFromInitial + HEADER_NUM_BYTES + 2 * numDims * Float.BYTES;
    int numBytes = data.getInt(sizePosition);
    final ByteBuffer readOnlyBuffer = data.asReadOnlyBuffer();
    readOnlyBuffer.position(sizePosition + Integer.BYTES);
    readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
    return bitmapFactory.mapImmutableBitmap(readOnlyBuffer);
  }

  @Override
  @SuppressWarnings("ArgumentParameterSwap")
  public Iterable<ImmutableNode<float[]>> getChildren()
  {
    return new Iterable<>()
    {
      @Override
      public Iterator<ImmutableNode<float[]>> iterator()
      {
        return new Iterator<>()
        {
          private int count = 0;

          @Override
          public boolean hasNext()
          {
            return (count < numChildren);
          }

          @Override
          public ImmutableNode<float[]> next()
          {
            if (isLeaf) {
              return new ImmutableFloatPoint(
                  numDims,
                  initialOffset,
                  data.getInt(childrenOffset + (count++) * Integer.BYTES),
                  data,
                  bitmapFactory
              );
            }
            return new ImmutableFloatNode(
                numDims,
                initialOffset,
                data.getInt(childrenOffset + (count++) * Integer.BYTES),
                data,
                bitmapFactory
            );
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public ByteBuffer getData()
  {
    return data;
  }

  private float[] getCoords(int offset)
  {
    final float[] retVal = new float[numDims];

    final ByteBuffer readOnlyBuffer = data.asReadOnlyBuffer();
    readOnlyBuffer.position(offset);
    readOnlyBuffer.asFloatBuffer().get(retVal);

    return retVal;
  }
}
