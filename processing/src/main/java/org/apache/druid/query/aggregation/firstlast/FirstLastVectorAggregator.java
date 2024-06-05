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

package org.apache.druid.query.aggregation.firstlast;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Base type for vectorized version of on heap 'last' aggregator for primitive numeric column selectors..
 */
public abstract class FirstLastVectorAggregator<RhsType, PairType extends SerializablePair<Long, RhsType>>
    implements VectorAggregator
{
  public static final int NULLITY_OFFSET = Long.BYTES;
  public static final int VALUE_OFFSET = NULLITY_OFFSET + Byte.BYTES;

  @Nullable
  private final VectorValueSelector timeSelector;
  @Nullable
  private final VectorValueSelector valueSelector;
  @Nullable
  private final VectorObjectSelector objectSelector;
  private final SelectionPredicate selectionPredicate;
  private final boolean useDefault = NullHandling.replaceWithDefault();


  /**
   *  timeSelector can be null, however aggregate functions are no-op then.
   */
  public FirstLastVectorAggregator(
      @Nullable VectorValueSelector timeSelector,
      @Nullable VectorValueSelector valueSelector,
      @Nullable VectorObjectSelector objectSelector,
      SelectionPredicate selectionPredicate
  )
  {
    if (timeSelector != null) {
      Preconditions.checkArgument(
          (valueSelector != null && objectSelector == null) || (valueSelector == null && objectSelector != null),
          "exactly one of 'valueSelector' and 'objectSelector' must be provided"
      );
    }
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    this.objectSelector = objectSelector;
    this.selectionPredicate = selectionPredicate;
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    // Not a normal case, and this doesn't affect the folding. timeSelectors should be present (albeit irrelevent) when folding.
    // timeSelector == null means that the aggregating column's capabilities aren't known, and it only happens for a special case
    // while building string aggregator
    if (timeSelector == null) {
      return;
    }

    // If objectSelector isn't null, then the objects might be folded up. If that's the case, whatever's represented by
    // the timeSelector doesn't hold any relevance.
    if (objectSelector != null) {
      final Object[] maybeFoldedObjects = objectSelector.getObjectVector();
      final boolean[] timeNullityVector = timeSelector.getNullVector();
      final long[] timeVector = timeSelector.getLongVector();

      PairType selectedPair = null;

      for (int index = startRow; index < endRow; ++index) {

        PairType pair = readPairFromVectorSelectors(timeNullityVector, timeVector, maybeFoldedObjects, index);
        if (pair != null) {
          if (selectedPair == null) {
            selectedPair = pair;
          } else if (selectionPredicate.apply(pair.lhs, selectedPair.lhs)) {
            selectedPair = pair;
          }
        }
      }
      // Something's been selected of the row vector
      if (selectedPair != null) {
        // Compare the latest value of the folded up row vector to the latest value in the buffer
        if (selectionPredicate.apply(selectedPair.lhs, buf.getLong(position))) {
          if (selectedPair.rhs != null) {
            putValue(buf, position, selectedPair.lhs, selectedPair.rhs);
          } else if (useDefault) {
            putDefaultValue(buf, position, selectedPair.lhs);
          } else {
            putNull(buf, position, selectedPair.lhs);
          }
        }
      }

    } else {
      // No object selector, no folding present. Check the timeSelector before checking the valueSelector
      final boolean[] timeNullityVector = timeSelector.getNullVector();
      final long[] timeVector = timeSelector.getLongVector();
      final boolean[] valueNullityVector = valueSelector.getNullVector();
      Integer selectedIndex = null;

      for (int index = startRow; index < endRow; ++index) {
        if (timeNullityVector != null && timeNullityVector[index]) {
          // Don't aggregate values where time isn't present
          continue;
        }
        // Find the latest time inside the vector objects
        if (selectedIndex == null) {
          selectedIndex = index;
        } else {
          if (selectionPredicate.apply(timeVector[index], timeVector[selectedIndex])) {
            selectedIndex = index;
          }
        }
      }
      // Compare the selectedIndex's value to the value on the buffer. This way, we write to the buffer only once
      // Weeds out empty vectors, where endRow == startRow
      if (selectedIndex != null) {
        if (selectionPredicate.apply(timeVector[selectedIndex], buf.getLong(position))) {
          // Write the value here
          if (valueNullityVector == null || !valueNullityVector[selectedIndex]) {
            putValue(buf, position, timeVector[selectedIndex], valueSelector, selectedIndex);
          } else if (useDefault) {
            putDefaultValue(buf, position, timeVector[selectedIndex]);
          } else {
            putNull(buf, position, timeVector[selectedIndex]);
          }
        }
      }
    }
  }

  @Override
  public void aggregate(
      ByteBuffer buf,
      int numRows,
      int[] positions,
      @Nullable int[] rows,
      int positionOffset
  )
  {
    // Not a normal case, and this doesn't affect the folding. timeSelectors should be present (albeit irrelevent) when folding.
    // timeSelector == null means that the aggregating column's capabilities aren't known, and it only happens for a special case
    // while building string aggregator
    if (timeSelector == null) {
      return;
    }

    // If objectSelector isn't null, then the objects might be folded up. If that's the case, whatever's represented by
    // the timeSelector doesn't hold any relevance. We should check for folded objects BEFORE even thinking about looking
    // at the timeSelector
    if (objectSelector != null) {
      final Object[] maybeFoldedObjects = objectSelector.getObjectVector();
      final boolean[] timeNullityVector = timeSelector.getNullVector();
      final long[] timeVector = timeSelector.getLongVector();
      for (int i = 0; i < numRows; ++i) {
        int position = positions[i] + positionOffset;
        int row = rows == null ? i : rows[i];
        // All the information about the object would be in the single selector. This method will check the folding of the object selector,
        // casting, and everything........
        PairType pair = readPairFromVectorSelectors(timeNullityVector, timeVector, maybeFoldedObjects, row);
        if (pair != null) {
          long lastTime = buf.getLong(position);
          if (selectionPredicate.apply(pair.lhs, lastTime)) {
            if (pair.rhs != null) {
              putValue(buf, position, pair.lhs, pair.rhs);
            } else if (useDefault) {
              putDefaultValue(buf, position, pair.lhs);
            } else {
              putNull(buf, position, pair.lhs);
            }
          }
        }
      }
    } else {
      // No object selector, no folding present. Check the timeSelector before checking the valueSelector
      final boolean[] timeNullityVector = timeSelector.getNullVector();
      final long[] timeVector = timeSelector.getLongVector();
      final boolean[] valueNullityVector = valueSelector.getNullVector();

      for (int i = 0; i < numRows; ++i) {
        int position = positions[i] + positionOffset;
        int row = rows == null ? i : rows[i];
        long lastTime = buf.getLong(position);
        if (timeNullityVector != null && timeNullityVector[row]) {
          // Don't aggregate values where time isn't present
          continue;
        }
        if (selectionPredicate.apply(timeVector[row], lastTime)) {
          if (valueNullityVector == null || !valueNullityVector[row]) {
            putValue(buf, position, timeVector[row], valueSelector, row);
          } else if (useDefault) {
            putDefaultValue(buf, position, timeVector[row]);
          } else {
            putNull(buf, position, timeVector[row]);
          }
        }
      }
    }
  }

  /**
   * Sets the value at the position. Subclasses can assume that the value isn't null
   *
   * 'position' refers to the location where the value of the pair will get updated (as opposed to the beginning of
   * the serialized pair)
   *
   * It is only used if objectSelector is supplied
   */
  protected abstract void putValue(ByteBuffer buf, int position, long time, RhsType value);

  /**
   * Sets the value represented by the valueSelector at the given index. A slightly redundant method to {@link #putValue(ByteBuffer, int, long, Object)}
   * to avoid autoboxing for the numeric types. Subclasses can assume that valueSelector.getNullVector[index] is false (i.e.
   * the value represented at the index isn't null)
   *
   * It is used if valueSelector is supplied
   */
  protected abstract void putValue(ByteBuffer buf, int position, long time, VectorValueSelector valueSelector, int index);

  /**
   * Sets the default value for the type in the byte buffer at the given position. It is only used when replaceNullWithDefault = true,
   * therefore the callers don't need to handle any other case.
   *
   * 'position' refers to the location where the value of the pair will get updated (as opposed to the beginning of
   * the serialized pair)
   */
  protected abstract void putDefaultValue(ByteBuffer buf, int position, long time);

  protected abstract void putNull(ByteBuffer buf, int position, long time);

  protected abstract PairType readPairFromVectorSelectors(
      boolean[] timeNullityVector,
      long[] timeVector,
      Object[] maybeFoldedObjects,
      int index
  );

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
