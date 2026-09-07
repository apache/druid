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

package org.apache.druid.segment.vector;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.BitmapOffset;
import org.roaringbitmap.PeekableIntIterator;

import javax.annotation.Nullable;

/**
 * Like {@link BitmapVectorOffset}, but in descending order.
 *
 * <p>As required by {@link ReadableVectorOffset#getOffsets()}, the offsets within each batch are in internally
 * ascending order.
 *
 * @see DescendingNoFilterVectorOffset the no-filter version
 */
public class DescendingBitmapVectorOffset implements VectorOffset
{
  private final ImmutableBitmap bitmap;
  private final int[] offsets;
  private final int startOffset;
  private final int endOffset;

  // Null when [startOffset, endOffset) is empty, in which case this offset is immediately done.
  @Nullable
  private PeekableIntIterator iterator;
  private int currentVectorSize;
  private boolean isContiguous;

  public DescendingBitmapVectorOffset(
      final int vectorSize,
      final ImmutableBitmap bitmap,
      final int startOffset,
      final int endOffset
  )
  {
    this.bitmap = bitmap;
    this.offsets = new int[vectorSize];
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    reset();
  }

  @Override
  public int getId()
  {
    Preconditions.checkState(currentVectorSize > 0, "currentVectorSize > 0");
    return offsets[0];
  }

  @Override
  public void advance()
  {
    // The iterator yields offsets from highest to lowest, so fill the array back-to-front (internally-ascending order).
    int i = offsets.length;
    while (i > 0 && iterator != null && iterator.hasNext() && iterator.peekNext() >= startOffset) {
      offsets[--i] = iterator.next();
    }

    currentVectorSize = offsets.length - i;

    if (i > 0 && currentVectorSize > 0) {
      // Partial batch: move it to the front of the array.
      System.arraycopy(offsets, i, offsets, 0, currentVectorSize);
    }

    isContiguous = false;
    if (currentVectorSize > 1) {
      final int hiPos = currentVectorSize - 1;
      isContiguous = offsets[hiPos] - offsets[0] == hiPos;
    }
  }

  @Override
  public boolean isDone()
  {
    return currentVectorSize == 0;
  }

  @Override
  public boolean isContiguous()
  {
    return isContiguous;
  }

  @Override
  public int getMaxVectorSize()
  {
    return offsets.length;
  }

  @Override
  public int getCurrentVectorSize()
  {
    return currentVectorSize;
  }

  @Override
  public int getStartOffset()
  {
    if (isContiguous) {
      return offsets[0];
    } else {
      throw DruidException.defensive("Cannot call getStartOffset() on a non-contiguous offset");
    }
  }

  @Override
  public int[] getOffsets()
  {
    if (!isContiguous) {
      return offsets;
    } else {
      throw DruidException.defensive("Cannot call getOffsets() on a contiguous offset");
    }
  }

  @Override
  public void reset()
  {
    currentVectorSize = 0;
    isContiguous = false;

    if (startOffset < endOffset) {
      iterator = BitmapOffset.getReverseBitmapOffsetIterator(bitmap);
      iterator.advanceIfNeeded(endOffset - 1);
      advance();
    } else {
      // Empty range.
      iterator = null;
    }
  }
}
