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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.primitives.Longs;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Set of utility methods to faciliate implementation of {@link Grouper.KeySerde#bufferComparator()} and
 * {@link Grouper.KeySerde#bufferComparatorWithAggregators(AggregatorFactory[], int[])}
 */
public class GrouperBufferComparatorUtils
{
  public static Grouper.BufferComparator bufferComparator(
      boolean includeTimestamp,
      boolean sortByDimsFirst,
      int dimCount,
      Grouper.BufferComparator[] serdeHelperComparators
  )
  {
    if (includeTimestamp) {
      if (sortByDimsFirst) {
        return new Grouper.BufferComparator()
        {
          @Override
          public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
          {
            final int cmp = compareDimsInBuffersForNullFudgeTimestamp(
                serdeHelperComparators,
                lhsBuffer,
                rhsBuffer,
                lhsPosition,
                rhsPosition
            );
            if (cmp != 0) {
              return cmp;
            }

            return Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));
          }
        };
      } else {
        return new Grouper.BufferComparator()
        {
          @Override
          public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
          {
            final int timeCompare = Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));

            if (timeCompare != 0) {
              return timeCompare;
            }

            return compareDimsInBuffersForNullFudgeTimestamp(
                serdeHelperComparators,
                lhsBuffer,
                rhsBuffer,
                lhsPosition,
                rhsPosition
            );
          }
        };
      }
    } else {
      return new Grouper.BufferComparator()
      {
        @Override
        public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
        {
          for (int i = 0; i < dimCount; i++) {
            final int cmp = serdeHelperComparators[i].compare(
                lhsBuffer,
                rhsBuffer,
                lhsPosition,
                rhsPosition
            );

            if (cmp != 0) {
              return cmp;
            }
          }

          return 0;
        }
      };
    }
  }

  public static Grouper.BufferComparator bufferComparatorWithAggregators(
      AggregatorFactory[] aggregatorFactories,
      int[] aggregatorOffsets,
      DefaultLimitSpec limitSpec,
      List<DimensionSpec> dimensions,
      Grouper.BufferComparator[] dimComparators,
      boolean includeTimestamp,
      boolean sortByDimsFirst
  )
  {
    int dimCount = dimensions.size();
    final List<Boolean> needsReverses = new ArrayList<>();
    List<Grouper.BufferComparator> comparators = new ArrayList<>();
    Set<Integer> orderByIndices = new HashSet<>();

    int aggCount = 0;
    boolean needsReverse;
    for (OrderByColumnSpec orderSpec : limitSpec.getColumns()) {
      needsReverse = orderSpec.getDirection() != OrderByColumnSpec.Direction.ASCENDING;
      int dimIndex = OrderByColumnSpec.getDimIndexForOrderBy(orderSpec, dimensions);
      if (dimIndex >= 0) {
        comparators.add(dimComparators[dimIndex]);
        orderByIndices.add(dimIndex);
        needsReverses.add(needsReverse);
      } else {
        int aggIndex = OrderByColumnSpec.getAggIndexForOrderBy(orderSpec, Arrays.asList(aggregatorFactories));
        if (aggIndex >= 0) {
          final StringComparator stringComparator = orderSpec.getDimensionComparator();
          final String typeName = aggregatorFactories[aggIndex].getTypeName();
          final int aggOffset = aggregatorOffsets[aggIndex] - Integer.BYTES;

          aggCount++;

          final ValueType valueType = ValueType.fromString(typeName);
          if (!ValueType.isNumeric(valueType)) {
            throw new IAE("Cannot order by a non-numeric aggregator[%s]", orderSpec);
          }

          comparators.add(makeNullHandlingBufferComparatorForNumericData(
              aggOffset,
              makeNumericBufferComparator(valueType, aggOffset, true, stringComparator)
          ));
          needsReverses.add(needsReverse);
        }
      }
    }

    for (int i = 0; i < dimCount; i++) {
      if (!orderByIndices.contains(i)) {
        comparators.add(dimComparators[i]);
        needsReverses.add(false); // default to Ascending order if dim is not in an orderby spec
      }
    }


    final Grouper.BufferComparator[] adjustedSerdeHelperComparators = comparators.toArray(new Grouper.BufferComparator[0]);

    final int fieldCount = dimCount + aggCount;

    if (includeTimestamp) {
      if (sortByDimsFirst) {
        return new Grouper.BufferComparator()
        {
          @Override
          public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
          {
            final int cmp = compareDimsInBuffersForNullFudgeTimestampForPushDown(
                adjustedSerdeHelperComparators,
                needsReverses,
                fieldCount,
                lhsBuffer,
                rhsBuffer,
                lhsPosition,
                rhsPosition
            );
            if (cmp != 0) {
              return cmp;
            }

            return Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));
          }
        };
      } else {
        return new Grouper.BufferComparator()
        {
          @Override
          public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
          {
            final int timeCompare = Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));

            if (timeCompare != 0) {
              return timeCompare;
            }

            int cmp = compareDimsInBuffersForNullFudgeTimestampForPushDown(
                adjustedSerdeHelperComparators,
                needsReverses,
                fieldCount,
                lhsBuffer,
                rhsBuffer,
                lhsPosition,
                rhsPosition
            );

            return cmp;
          }
        };
      }
    } else {
      return new Grouper.BufferComparator()
      {
        @Override
        public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
        {
          for (int i = 0; i < fieldCount; i++) {
            final int cmp;
            if (needsReverses.get(i)) {
              cmp = adjustedSerdeHelperComparators[i].compare(
                  rhsBuffer,
                  lhsBuffer,
                  rhsPosition,
                  lhsPosition
              );
            } else {
              cmp = adjustedSerdeHelperComparators[i].compare(
                  lhsBuffer,
                  rhsBuffer,
                  lhsPosition,
                  rhsPosition
              );
            }

            if (cmp != 0) {
              return cmp;
            }
          }

          return 0;
        }
      };
    }
  }

  private static int compareDimsInBuffersForNullFudgeTimestampForPushDown(
      Grouper.BufferComparator[] serdeHelperComparators,
      List<Boolean> needsReverses,
      int dimCount,
      ByteBuffer lhsBuffer,
      ByteBuffer rhsBuffer,
      int lhsPosition,
      int rhsPosition
  )
  {
    for (int i = 0; i < dimCount; i++) {
      final int cmp;
      if (needsReverses.get(i)) {
        cmp = serdeHelperComparators[i].compare(
            rhsBuffer,
            lhsBuffer,
            rhsPosition + Long.BYTES,
            lhsPosition + Long.BYTES
        );
      } else {
        cmp = serdeHelperComparators[i].compare(
            lhsBuffer,
            rhsBuffer,
            lhsPosition + Long.BYTES,
            rhsPosition + Long.BYTES
        );
      }
      if (cmp != 0) {
        return cmp;
      }
    }

    return 0;
  }

  private static int compareDimsInBuffersForNullFudgeTimestamp(
      Grouper.BufferComparator[] serdeHelperComparators,
      ByteBuffer lhsBuffer,
      ByteBuffer rhsBuffer,
      int lhsPosition,
      int rhsPosition
  )
  {
    for (Grouper.BufferComparator comparator : serdeHelperComparators) {
      final int cmp = comparator.compare(
          lhsBuffer,
          rhsBuffer,
          lhsPosition + Long.BYTES,
          rhsPosition + Long.BYTES
      );
      if (cmp != 0) {
        return cmp;
      }
    }

    return 0;
  }

  private static Grouper.BufferComparator makeNumericBufferComparator(
      ValueType valueType,
      int keyBufferPosition,
      boolean pushLimitDown,
      @Nullable StringComparator stringComparator
  )
  {
    switch (valueType) {
      case LONG:
        return makeBufferComparatorForLong(keyBufferPosition, pushLimitDown, stringComparator);
      case FLOAT:
        return makeBufferComparatorForFloat(keyBufferPosition, pushLimitDown, stringComparator);
      case DOUBLE:
        return makeBufferComparatorForDouble(keyBufferPosition, pushLimitDown, stringComparator);
      default:
        throw new IAE("invalid type: %s", valueType);
    }
  }

  public static Grouper.BufferComparator makeBufferComparatorForLong(
      int keyBufferPosition,
      boolean pushLimitDown,
      @Nullable StringComparator stringComparator
  )
  {
    if (isPrimitiveComparable(pushLimitDown, stringComparator)) {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Longs.compare(
          lhsBuffer.getLong(lhsPosition + keyBufferPosition),
          rhsBuffer.getLong(rhsPosition + keyBufferPosition)
      );
    } else {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
        long lhs = lhsBuffer.getLong(lhsPosition + keyBufferPosition);
        long rhs = rhsBuffer.getLong(rhsPosition + keyBufferPosition);

        return stringComparator.compare(String.valueOf(lhs), String.valueOf(rhs));
      };
    }
  }

  public static Grouper.BufferComparator makeBufferComparatorForDouble(
      int keyBufferPosition,
      boolean pushLimitDown,
      @Nullable StringComparator stringComparator
  )
  {
    if (isPrimitiveComparable(pushLimitDown, stringComparator)) {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Double.compare(
          lhsBuffer.getDouble(lhsPosition + keyBufferPosition),
          rhsBuffer.getDouble(rhsPosition + keyBufferPosition)
      );
    } else {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
        double lhs = lhsBuffer.getDouble(lhsPosition + keyBufferPosition);
        double rhs = rhsBuffer.getDouble(rhsPosition + keyBufferPosition);
        return stringComparator.compare(String.valueOf(lhs), String.valueOf(rhs));
      };
    }
  }

  public static Grouper.BufferComparator makeBufferComparatorForFloat(
      int keyBufferPosition,
      boolean pushLimitDown,
      @Nullable StringComparator stringComparator
  )
  {
    if (isPrimitiveComparable(pushLimitDown, stringComparator)) {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Float.compare(
          lhsBuffer.getFloat(lhsPosition + keyBufferPosition),
          rhsBuffer.getFloat(rhsPosition + keyBufferPosition)
      );
    } else {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
        float lhs = lhsBuffer.getFloat(lhsPosition + keyBufferPosition);
        float rhs = rhsBuffer.getFloat(rhsPosition + keyBufferPosition);
        return stringComparator.compare(String.valueOf(lhs), String.valueOf(rhs));
      };
    }
  }

  public static Grouper.BufferComparator makeNullHandlingBufferComparatorForNumericData(
      int keyBufferPosition,
      Grouper.BufferComparator delegate
  )
  {
    return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
      boolean isLhsNull = (lhsBuffer.get(lhsPosition + keyBufferPosition) == NullHandling.IS_NULL_BYTE);
      boolean isRhsNull = (rhsBuffer.get(rhsPosition + keyBufferPosition) == NullHandling.IS_NULL_BYTE);
      if (isLhsNull && isRhsNull) {
        // Both are null
        return 0;
      }
      // only lhs is null
      if (isLhsNull) {
        return -1;
      }
      // only rhs is null
      if (isRhsNull) {
        return 1;
      }
      return delegate.compare(
          lhsBuffer,
          rhsBuffer,
          lhsPosition,
          rhsPosition
      );
    };
  }

  private static boolean isPrimitiveComparable(boolean pushLimitDown, @Nullable StringComparator stringComparator)
  {
    return !pushLimitDown || stringComparator == null || stringComparator.equals(StringComparators.NUMERIC);
  }
}
