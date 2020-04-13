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

public class FilteredVectorAggregator implements VectorAggregator
{
  private final VectorValueMatcher matcher;
  private final VectorAggregator delegate;
  private final int[] delegatePositions;

  @Nullable
  private VectorMatch maskScratch = null;

  public FilteredVectorAggregator(
      final VectorValueMatcher matcher,
      final VectorAggregator delegate
  )
  {
    this.matcher = matcher;
    this.delegate = delegate;
    this.delegatePositions = new int[matcher.getMaxVectorSize()];
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    delegate.init(buf, position);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final ReadableVectorMatch mask;

    if (startRow == 0) {
      mask = VectorMatch.allTrue(endRow);
    } else {
      if (maskScratch == null) {
        maskScratch = VectorMatch.wrap(new int[matcher.getMaxVectorSize()]);
      }

      final int maskSize = endRow - startRow;
      final int[] maskArray = maskScratch.getSelection();
      for (int i = 0; i < maskSize; i++) {
        maskArray[i] = startRow + i;
      }

      maskScratch.setSelectionSize(maskSize);
      mask = maskScratch;
    }

    final ReadableVectorMatch match = matcher.match(mask);

    if (match.isAllTrue(matcher.getCurrentVectorSize())) {
      delegate.aggregate(buf, position, startRow, endRow);
    } else if (!match.isAllFalse()) {
      Arrays.fill(delegatePositions, 0, match.getSelectionSize(), position);
      delegate.aggregate(buf, match.getSelectionSize(), delegatePositions, match.getSelection(), 0);
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

    final ReadableVectorMatch match = matcher.match(match0);
    final int[] selection = match.getSelection();

    if (rows == null) {
      for (int i = 0; i < match.getSelectionSize(); i++) {
        delegatePositions[i] = positions[selection[i]];
      }
    } else {
      // i iterates over the match; j iterates over the "rows" array
      for (int i = 0, j = 0; i < match.getSelectionSize(); i++) {
        for (; rows[j] < selection[i]; j++) {
          // Do nothing; the for loop is doing the work of incrementing j.
        }

        if (rows[j] != selection[i]) {
          throw new ISE("Selection contained phantom row[%d]", selection[i]);
        }

        delegatePositions[i] = positions[j];
      }
    }

    delegate.aggregate(buf, match.getSelectionSize(), delegatePositions, selection, positionOffset);
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return delegate.get(buf, position);
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
    delegate.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
  }
}
