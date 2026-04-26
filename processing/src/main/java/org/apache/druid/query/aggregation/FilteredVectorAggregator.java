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

package org.apache.druid.query.aggregation;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Aggregates an underlying {@link #delegate} into a buffer when {@link #matcher} matches.
 *
 * <p>If {@link #elseValue} is set, an extra byte is added to the beginning of the buffer to track whether we've
 * seen an unmatched row.
 */
public class FilteredVectorAggregator implements VectorAggregator
{
  private final VectorValueMatcher matcher;
  private final VectorAggregator delegate;
  private final int[] delegatePositions;
  @Nullable
  private final Number elseValue;
  private final int valueOffset;

  @Nullable
  private VectorMatch maskScratch = null;

  public FilteredVectorAggregator(
      final VectorValueMatcher matcher,
      final VectorAggregator delegate,
      @Nullable final Number elseValue
  )
  {
    this.matcher = matcher;
    this.delegate = delegate;
    this.delegatePositions = new int[matcher.getMaxVectorSize()];
    this.elseValue = elseValue;
    this.valueOffset = elseValue != null ? Byte.BYTES : 0;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    if (elseValue != null) {
      buf.put(position, (byte) 0);
    } else {
      delegate.init(buf, position + valueOffset);
    }
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final ReadableVectorMatch mask;
    final int maskSize = endRow - startRow;

    if (startRow == 0) {
      mask = VectorMatch.allTrue(endRow);
    } else {
      if (maskScratch == null) {
        maskScratch = VectorMatch.wrap(new int[matcher.getMaxVectorSize()]);
      }

      final int[] maskArray = maskScratch.getSelection();
      for (int i = 0; i < maskSize; i++) {
        maskArray[i] = startRow + i;
      }

      maskScratch.setSelectionSize(maskSize);
      mask = maskScratch;
    }

    final ReadableVectorMatch match = matcher.match(mask, false);
    final int matchedSize = match.getSelectionSize();

    if (match.isAllTrue(matcher.getCurrentVectorSize())) {
      delegate.aggregate(buf, position + valueOffset, startRow, endRow);
    } else if (!match.isAllFalse()) {
      Arrays.fill(delegatePositions, 0, matchedSize, position + valueOffset);
      delegate.aggregate(buf, matchedSize, delegatePositions, match.getSelection(), 0);
    }

    // Set the unmatched-row flag if any row in this slice did not match the filter.
    if (elseValue != null && matchedSize < maskSize) {
      markUnmatched(buf, position);
    }
  }

  @Override
  public void aggregate(
      final ByteBuffer buf,
      final int numRows,
      final int[] positions,
      @Nullable final int[] rows,
      final int positionOffset
  )
  {
    final ReadableVectorMatch match0;

    if (rows == null) {
      match0 = VectorMatch.allTrue(numRows);
    } else {
      match0 = VectorMatch.wrap(rows).setSelectionSize(numRows);
    }

    final ReadableVectorMatch match = matcher.match(match0, false);
    final int[] selection = match.getSelection();

    if (rows == null) {
      if (elseValue != null) {
        // Mark "saw an unmatched row" for any input row not present in the matcher's selection.
        int matchIdx = 0;
        for (int i = 0; i < numRows; i++) {
          if (matchIdx < match.getSelectionSize() && selection[matchIdx] == i) {
            matchIdx++;
          } else {
            markUnmatched(buf, positions[i] + positionOffset);
          }
        }
      }

      for (int i = 0; i < match.getSelectionSize(); i++) {
        delegatePositions[i] = valueOffset + positions[selection[i]];
      }
    } else {
      if (elseValue != null) {
        // Mark "saw an unmatched row" for any input row whose original-row id is not in the selection.
        int matchIdx = 0;
        for (int i = 0; i < numRows; i++) {
          if (matchIdx < match.getSelectionSize() && rows[i] == selection[matchIdx]) {
            matchIdx++;
          } else {
            markUnmatched(buf, positions[i] + positionOffset);
          }
        }
      }

      // i iterates over the match; j iterates over the "rows" array
      for (int i = 0, j = 0; i < match.getSelectionSize(); i++) {
        for (; rows[j] < selection[i]; j++) {
          // Do nothing; the for loop is doing the work of incrementing j.
        }

        if (rows[j] != selection[i]) {
          throw new ISE("Selection contained phantom row[%d]", selection[i]);
        }

        delegatePositions[i] = valueOffset + positions[j];
      }
    }

    delegate.aggregate(buf, match.getSelectionSize(), delegatePositions, selection, positionOffset);
  }

  @Override
  @Nullable
  public Object get(final ByteBuffer buf, final int position)
  {
    final Object delegateVal = delegate.get(buf, position + valueOffset);
    if (elseValue != null && delegateVal == null && hasUnmatched(buf, position)) {
      return elseValue;
    } else {
      return delegateVal;
    }
  }

  @Override
  public void close()
  {
    delegate.close();
    maskScratch = null;
  }

  @Override
  public void relocate(
      final int oldPosition,
      final int newPosition,
      final ByteBuffer oldBuffer,
      final ByteBuffer newBuffer
  )
  {
    delegate.relocate(oldPosition + valueOffset, newPosition + valueOffset, oldBuffer, newBuffer);
  }

  private static void markUnmatched(final ByteBuffer buf, final int position)
  {
    buf.put(position, (byte) 1);
  }

  private static boolean hasUnmatched(final ByteBuffer buf, final int position)
  {
    return buf.get(position) == 1;
  }
}
