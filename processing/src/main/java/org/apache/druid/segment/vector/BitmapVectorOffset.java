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
import org.roaringbitmap.BatchIterator;

public class BitmapVectorOffset implements VectorOffset
{
  private final ImmutableBitmap bitmap;
  private final int[] offsets;
  private final int startOffset;
  private final int endOffset;

  private BatchIterator iterator;
  private boolean pastEnd;
  private int currentVectorSize;

  public BitmapVectorOffset(
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
    // Should not be called when the offset is empty.
    Preconditions.checkState(currentVectorSize > 0, "currentVectorSize > 0");
    return offsets[0];
  }

  @Override
  public void advance()
  {
    currentVectorSize = 0;

    if (pastEnd) {
      return;
    }

    while (currentVectorSize == 0 && iterator.hasNext()) {
      final int numRead = iterator.nextBatch(offsets);

      int from = 0;
      while (from < numRead && offsets[from] < startOffset) {
        from++;
      }

      if (from > 0) {
        System.arraycopy(offsets, from, offsets, 0, numRead - from);
      }

      int to = numRead - from;
      while (to > 0 && offsets[to - 1] >= endOffset) {
        pastEnd = true;
        to--;
      }

      currentVectorSize = to;
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
    return false;
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
    throw new UnsupportedOperationException("not contiguous");
  }

  @Override
  public int[] getOffsets()
  {
    return offsets;
  }

  @Override
  public void reset()
  {
    iterator = bitmap.batchIterator();
    currentVectorSize = 0;
    pastEnd = false;
    advance();
  }
}
