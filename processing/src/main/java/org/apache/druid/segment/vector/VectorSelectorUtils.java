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

import org.roaringbitmap.PeekableIntIterator;

import javax.annotation.Nullable;
import java.util.Arrays;

public class VectorSelectorUtils
{
  /**
   * Helper used by ColumnarLongs, ColumnarDoubles, etc. to populate null-flag vectors.
   */
  @Nullable
  public static boolean[] populateNullVector(
      @Nullable final boolean[] nullVector,
      final ReadableVectorOffset offset,
      final PeekableIntIterator nullIterator
  )
  {
    if (!nullIterator.hasNext()) {
      return null;
    }

    final boolean[] retVal;

    if (nullVector != null) {
      retVal = nullVector;
    } else {
      retVal = new boolean[offset.getMaxVectorSize()];
    }

    if (offset.isContiguous()) {
      final int startOffset = offset.getStartOffset();
      nullIterator.advanceIfNeeded(startOffset);
      if (!nullIterator.hasNext()) {
        return null;
      }
      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        final int row = i + startOffset;
        nullIterator.advanceIfNeeded(row);
        if (!nullIterator.hasNext()) {
          Arrays.fill(retVal, i, offset.getCurrentVectorSize(), false);
          break;
        }
        retVal[i] = row == nullIterator.peekNext();
      }
    } else {
      final int[] currentOffsets = offset.getOffsets();
      nullIterator.advanceIfNeeded(currentOffsets[0]);
      if (!nullIterator.hasNext()) {
        return null;
      }
      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        final int row = currentOffsets[i];
        nullIterator.advanceIfNeeded(row);
        if (!nullIterator.hasNext()) {
          Arrays.fill(retVal, i, offset.getCurrentVectorSize(), false);
          break;
        }
        retVal[i] = row == nullIterator.peekNext();
      }
    }

    return retVal;
  }
}
