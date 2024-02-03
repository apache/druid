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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.first.FirstLastUtils;
import org.apache.druid.query.aggregation.first.StringFirstLastUtils;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringLastVectorAggregator implements VectorAggregator
{
  private static final SerializablePairLongString INIT = new SerializablePairLongString(
      DateTimes.MIN.getMillis(),
      null
  );
  private final VectorValueSelector timeSelector;
  private final VectorObjectSelector valueSelector;
  private final int maxStringBytes;
  protected long lastTime;

  public StringLastVectorAggregator(
      @Nullable final VectorValueSelector timeSelector,
      final VectorObjectSelector valueSelector,
      final int maxStringBytes
  )
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    StringFirstLastUtils.writePair(buf, position, INIT, maxStringBytes);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    if (timeSelector == null) {
      return;
    }
    final long[] times = timeSelector.getLongVector();
    final boolean[] nullTimeVector = timeSelector.getNullVector();
    final Object[] objectsWhichMightBeStrings = valueSelector.getObjectVector();

    lastTime = buf.getLong(position);
    int index;
    for (int i = endRow - 1; i >= startRow; i--) {
      if (objectsWhichMightBeStrings[i] == null) {
        continue;
      }
      if (times[i] <= lastTime) {
        continue;
      }
      if (nullTimeVector != null && nullTimeVector[i]) {
        continue;
      }
      index = i;
      final boolean foldNeeded = FirstLastUtils.objectNeedsFoldCheck(objectsWhichMightBeStrings[index], SerializablePairLongString.class);
      if (foldNeeded) {
        // Less efficient code path when folding is a possibility (we must read the value selector first just in case
        // it's a foldable object).
        final SerializablePairLongString inPair = StringFirstLastUtils.readPairFromVectorSelectorsAtIndex(
            timeSelector,
            valueSelector,
            index
        );
        if (inPair != null) {
          final long lastTime = buf.getLong(position);
          if (inPair.lhs >= lastTime) {
            StringFirstLastUtils.writePair(
                buf,
                position,
                new SerializablePairLongString(inPair.lhs, inPair.rhs),
                maxStringBytes
            );
          }
        }
      } else {
        final long time = times[index];

        if (time >= lastTime) {
          final String value = DimensionHandlerUtils.convertObjectToString(objectsWhichMightBeStrings[index]);
          lastTime = time;
          StringFirstLastUtils.writePair(
              buf,
              position,
              new SerializablePairLongString(time, value),
              maxStringBytes
          );
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
    if (timeSelector == null) {
      return;
    }
    final long[] timeVector = timeSelector.getLongVector();
    final boolean[] nullTimeVector = timeSelector.getNullVector();
    final Object[] objectsWhichMightBeStrings = valueSelector.getObjectVector();

    // iterate once over the object vector to find first non null element and
    // determine if the type is Pair or not
    boolean foldNeeded = false;
    for (Object obj : objectsWhichMightBeStrings) {
      if (obj != null) {
        foldNeeded = FirstLastUtils.objectNeedsFoldCheck(obj, SerializablePairLongString.class);
        break;
      }
    }

    for (int i = 0; i < numRows; i++) {
      if (nullTimeVector != null && nullTimeVector[i]) {
        continue;
      }
      int position = positions[i] + positionOffset;
      int row = rows == null ? i : rows[i];
      long lastTime = buf.getLong(position);
      if (timeVector[row] >= lastTime) {
        if (foldNeeded) {
          final SerializablePairLongString inPair = StringFirstLastUtils.readPairFromVectorSelectorsAtIndex(
              timeSelector,
              valueSelector,
              row
          );
          if (inPair != null) {
            if (inPair.lhs >= lastTime) {
              StringFirstLastUtils.writePair(
                  buf,
                  position,
                  new SerializablePairLongString(inPair.lhs, inPair.rhs),
                  maxStringBytes
              );
            }
          }
        } else {
          final String value = DimensionHandlerUtils.convertObjectToString(objectsWhichMightBeStrings[row]);
          lastTime = timeVector[row];
          StringFirstLastUtils.writePair(
              buf,
              position,
              new SerializablePairLongString(lastTime, value),
              maxStringBytes
          );
        }
      }
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return StringFirstLastUtils.readPair(buf, position);
  }

  @Override
  public void close()
  {
    // nothing to close
  }
}

