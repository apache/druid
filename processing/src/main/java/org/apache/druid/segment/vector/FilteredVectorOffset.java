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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;

public class FilteredVectorOffset implements VectorOffset
{
  private final VectorOffset baseOffset;
  private final VectorValueMatcher filterMatcher;
  private final int[] offsets;
  private int currentVectorSize = 0;
  private boolean allTrue = false;

  private FilteredVectorOffset(final VectorOffset baseOffset, final VectorValueMatcher filterMatcher)
  {
    this.baseOffset = baseOffset;
    this.filterMatcher = filterMatcher;
    this.offsets = new int[baseOffset.getMaxVectorSize()];
    advanceWhileVectorIsEmptyAndPopulateOffsets();
  }

  public static FilteredVectorOffset create(
      final VectorOffset baseOffset,
      final VectorColumnSelectorFactory baseColumnSelectorFactory,
      final Filter filter
  )
  {
    // This is not the same logic as the row-by-row FilteredOffset, which uses bitmaps whenever possible.
    // I am not convinced that approach is best in all cases (it's potentially too eager) and also have not implemented
    // it for vector matchers yet. So let's keep this method simple for now, and try to harmonize them in the future.
    Preconditions.checkState(filter.canVectorizeMatcher(), "Cannot vectorize");
    final VectorValueMatcher filterMatcher = filter.makeVectorMatcher(baseColumnSelectorFactory);
    return new FilteredVectorOffset(baseOffset, filterMatcher);
  }

  @Override
  public int getId()
  {
    // Should not be called when the offset is empty.
    Preconditions.checkState(currentVectorSize > 0, "currentVectorSize > 0");
    return baseOffset.getId();
  }

  @Override
  public void advance()
  {
    baseOffset.advance();
    advanceWhileVectorIsEmptyAndPopulateOffsets();
  }

  @Override
  public boolean isDone()
  {
    return currentVectorSize == 0;
  }

  @Override
  public boolean isContiguous()
  {
    return allTrue && baseOffset.isContiguous();
  }

  @Override
  public int getMaxVectorSize()
  {
    return baseOffset.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    return currentVectorSize;
  }

  @Override
  public int getStartOffset()
  {
    if (isContiguous()) {
      return baseOffset.getStartOffset();
    } else {
      throw new ISE("Cannot call getStartOffset when not contiguous!");
    }
  }

  @Override
  public int[] getOffsets()
  {
    if (isContiguous()) {
      throw new ISE("Cannot call getOffsets when not contiguous!");
    } else if (allTrue) {
      return baseOffset.getOffsets();
    } else {
      return offsets;
    }
  }

  private void advanceWhileVectorIsEmptyAndPopulateOffsets()
  {
    allTrue = false;

    int j = 0;

    while (j == 0) {
      if (baseOffset.isDone()) {
        currentVectorSize = 0;
        return;
      }

      final ReadableVectorMatch match = filterMatcher.match(VectorMatch.allTrue(baseOffset.getCurrentVectorSize()));

      if (match.isAllTrue(baseOffset.getCurrentVectorSize())) {
        currentVectorSize = baseOffset.getCurrentVectorSize();
        allTrue = true;
        return;
      } else if (match.isAllFalse()) {
        baseOffset.advance();
      } else {
        final int[] selection = match.getSelection();
        final int selectionSize = match.getSelectionSize();

        if (baseOffset.isContiguous()) {
          final int startOffset = baseOffset.getStartOffset();

          for (int i = 0; i < selectionSize; i++) {
            offsets[j++] = startOffset + selection[i];
          }
        } else {
          final int[] baseOffsets = baseOffset.getOffsets();

          for (int i = 0; i < selectionSize; i++) {
            offsets[j++] = baseOffsets[selection[i]];
          }
        }

        if (j == 0) {
          baseOffset.advance();
        }
      }
    }

    currentVectorSize = j;
  }

  @Override
  public void reset()
  {
    currentVectorSize = 0;
    allTrue = false;
    baseOffset.reset();
    advanceWhileVectorIsEmptyAndPopulateOffsets();
  }
}
