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

package org.apache.druid.query.aggregation.last;

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
public abstract class NumericLastVectorAggregator<RhsType, PairType extends SerializablePair<Long, RhsType>>
    implements VectorAggregator
{
  private static final byte IS_NULL_BYTE = (byte) 1;
  private static final byte IS_NOT_NULL_BYTE = (byte) 0;

  static final int NULLITY_OFFSET = Long.BYTES;
  static final int VALUE_OFFSET = NULLITY_OFFSET + Byte.BYTES;

  @Nullable
  private final VectorValueSelector valueSelector;
  @Nullable
  private final VectorObjectSelector objectSelector;
  private final boolean useDefault = NullHandling.replaceWithDefault();
  private final VectorValueSelector timeSelector;
  private long lastTime;


  NumericLastVectorAggregator(
      VectorValueSelector timeSelector,
      @Nullable VectorValueSelector valueSelector,
      @Nullable VectorObjectSelector objectSelector
  )
  {
    Preconditions.checkArgument(
        (valueSelector != null && objectSelector == null) || (valueSelector == null && objectSelector != null),
        "exactly one of 'valueSelector' and 'objectSelector' must be provided"
    );
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    this.objectSelector = objectSelector;
    lastTime = Long.MIN_VALUE;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MIN_VALUE);
    buf.put(position + NULLITY_OFFSET, useDefault ? IS_NOT_NULL_BYTE : IS_NULL_BYTE);
    initValue(buf, position + VALUE_OFFSET);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {

    // If objectSelector isn't null, then the objects might be folded up. If that's the case, whatever's represented by
    // the timeSelector doesn't hold any relevance.
    if (objectSelector != null) {
      final Object[] maybeFoldedObjects = objectSelector.getObjectVector();
      final boolean[] timeNullityVector = timeSelector.getNullVector();
      final long[] timeVector = timeSelector.getLongVector();
      lastTime = buf.getLong(position);

      for (int index = endRow - 1; index >= startRow; --index) {
        PairType pair = readPairFromVectorSelectors(timeNullityVector, timeVector, maybeFoldedObjects, index);
        if (pair != null) {
          if (pair.lhs >= lastTime) {
            if (pair.rhs != null) {
              updateTimeAndValue(buf, position, pair.lhs, pair.rhs);
            } else if (useDefault) {
              updateTimeAndDefaultValue(buf, position, pair.lhs);
            } else {
              updateTimeAndNull(buf, position, pair.lhs);
            }
          }
        }
      }
    } else {
      // No object selector, no folding present. Check the timeSelector before checking the valueSelector
      final boolean[] timeNullityVector = timeSelector.getNullVector();
      final long[] timeVector = timeSelector.getLongVector();
      final boolean[] valueNullityVector = valueSelector.getNullVector();
      int selectedIndex = endRow - 1;
      long lastTime = buf.getLong(position);
      for (int index = endRow - 1; index >= startRow; --index) {
        if (timeNullityVector != null && timeNullityVector[index]) {
          // Don't aggregate values where time isn't present
          continue;
        }
        // Find the latest time inside the vector objects
        if (timeVector[selectedIndex] >= timeVector[index]) {
          selectedIndex = index;
        }

        // Compare the selectedIndex's value to the value on the buffer. This way, we write to the buffer only once
        // Weeds out empty vectors, where endRow == startRow
        if (selectedIndex >= startRow) {
          if (timeVector[selectedIndex] >= lastTime) {
            // Write the value here
            if (valueNullityVector != null && !valueNullityVector[selectedIndex]) {
              updateTimeAndValue(buf, position, timeVector[selectedIndex], valueSelector, selectedIndex);
            } else if (useDefault) {
              updateTimeAndDefaultValue(buf, position, timeVector[selectedIndex]);
            } else {
              updateTimeAndNull(buf, position, timeVector[index]);
            }
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
        long lastTime = buf.getLong(position);
        // All the information about the object would be in the single selector. This method will check the folding of the object selector,
        // casting, and everything........
        PairType pair = readPairFromVectorSelectors(timeNullityVector, timeVector, maybeFoldedObjects, row);
        if (pair != null) {
          if (pair.lhs >= lastTime) {
            if (pair.rhs != null) {
              updateTimeAndValue(buf, position, pair.lhs, pair.rhs);
            } else if (useDefault) {
              updateTimeAndDefaultValue(buf, position, pair.lhs);
            } else {
              updateTimeAndNull(buf, position, pair.lhs);
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
        if (timeVector[row] >= lastTime) {
          if (valueNullityVector != null && !valueNullityVector[row]) {
            updateTimeAndValue(buf, position, timeVector[row], valueSelector, row);
          } else if (useDefault) {
            updateTimeAndDefaultValue(buf, position, timeVector[row]);
          } else {
            updateTimeAndNull(buf, position, timeVector[row]);
          }
        }
      }
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    if (buf.get(position + NULLITY_OFFSET) == IS_NULL_BYTE) {
      return createPair(buf.getLong(position), null);
    }
    return createPair(buf.getLong(position), getValue(buf, position + VALUE_OFFSET));
  }

  private void updateTimeAndValue(ByteBuffer buf, int position, long time, RhsType value)
  {
    buf.putLong(position, time);
    buf.put(position + NULLITY_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    putValue(buf, position + VALUE_OFFSET, value);
  }

  private void updateTimeAndValue(ByteBuffer buf, int position, long time, VectorValueSelector valueSelector, int index)
  {
    buf.putLong(position, time);
    buf.put(position + NULLITY_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    putValue(buf, position + VALUE_OFFSET, valueSelector, index);
  }

  private void updateTimeAndDefaultValue(ByteBuffer buf, int position, long time)
  {
    buf.putLong(position, time);
    buf.put(position + NULLITY_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    putDefaultValue(buf, position + VALUE_OFFSET);
  }

  private void updateTimeAndNull(ByteBuffer buf, int position, long time)
  {
    buf.putLong(position, time);
    buf.put(position + NULLITY_OFFSET, IS_NULL_BYTE);
  }

  /**
   * Sets the initial value in the byte buffer at the given position
   *
   * 'position' refers to the location where the value of the pair will get updated (as opposed to the beginning of
   * the serialized pair)
   */
  abstract void initValue(ByteBuffer buf, int position);

  /**
   * Sets the value at the position. Subclasses can assume that the value isn't null
   *
   * 'position' refers to the location where the value of the pair will get updated (as opposed to the beginning of
   * the serialized pair)
   */
  abstract void putValue(ByteBuffer buf, int position, RhsType value);

  /**
   * Sets the value represented by the valueSelector at the given index. A slightly redundant method to {@link #putValue(ByteBuffer, int, Object)}
   * to avoid autoboxing for the numeric types. Subclasses can assume that valueSelector.getNullVector[index] is false (i.e.
   * the value represented at the index isn't null)
   */
  abstract void putValue(ByteBuffer buf, int position, VectorValueSelector valueSelector, int index);

  /**
   * Sets the default value for the type in the byte buffer at the given position. It is only used when replaceNullWithDefault = true,
   * therefore the callers don't need to handle any other case. It is separate from {@link #initValue} in case the initial value is
   * different from the default value.
   *
   * 'position' refers to the location where the value of the pair will get updated (as opposed to the beginning of
   * the serialized pair)
   */
  abstract void putDefaultValue(ByteBuffer buf, int position);

  /**
   * Gets the value at the given position. It is okay to box, because we'd be wrapping it in a SerializablePair
   *
   * 'position' refers to the location where the value of the pair will get updated (as opposed to the beginning of
   * the serialized pair)
   */
  abstract RhsType getValue(ByteBuffer buf, int position);

  abstract PairType readPairFromVectorSelectors(
      boolean[] timeNullityVector,
      long[] timeVector,
      Object[] maybeFoldedObjects,
      int index
  );

  abstract PairType createPair(long time, @Nullable RhsType value);

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
