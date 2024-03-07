package org.apache.druid.query.groupby.epinephelinae.column;

import org.apache.druid.query.DimensionComparisonUtils;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;

// Used only by primitives right now, however specialized complex types can reuse this once we have a way to extract
// the required info
// Not thread safe because does weird stuff with buffer's position while reading
@NotThreadSafe
public class FixedWidthGroupByColumnSelectorStrategy<T> implements GroupByColumnSelectorStrategy
{

  final int keySize;
  @Nullable
  final DimensionToIdConverter dimensionToIdConverter;
  final boolean isPrimitive;
  final ColumnType columnType;
  final NullableTypeStrategy<T> nullableTypeStrategy;

  public FixedWidthGroupByColumnSelectorStrategy(
      int keySize,
      @Nullable DimensionToIdConverter dimensionToIdConverter,
      boolean isPrimitive,
      ColumnType columnType
  )
  {
    this.keySize = keySize;
    this.dimensionToIdConverter = dimensionToIdConverter;
    this.isPrimitive = isPrimitive;
    this.columnType = columnType;
    this.nullableTypeStrategy = columnType.getNullableStrategy();
  }

  @Override
  public int getGroupingKeySize()
  {
    return keySize;
  }

  @Override
  public void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus,
      ByteBuffer key,
      ResultRow resultRow,
      int keyBufferPosition
  )
  {
    resultRow.set(
        selectorPlus.getResultRowPosition(),
        nullableTypeStrategy.read(key, keyBufferPosition)
    );
  }

  @Override
  public int initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    // It is expected of the primitive selectors to be returning default value of the implementation here. In the
    // getObject(), if it returns null, it won't
//    if (selectorIsNull(selector)) {
//      valuess[columnIndex] = null;
//    } else {
      // Here the primitive selectors should have returned correct values - float shouldn't return longs and vice versa
      // Perhaps we'd require a cast as well, which is done implicitly when we call the .getLong/.getFloat/.getDouble
//      valuess[columnIndex] = selector.getObject();
//    }

    valuess[columnIndex] = getValue(selector);
    return 0;
  }

  @Override
  public void initGroupingKeyColumnValue(
      int keyBufferPosition,
      int dimensionIndex,
      Object rowObj,
      ByteBuffer keyBuffer,
      int[] stack
  )
  {
    if (rowObj == null) {
      nullableTypeStrategy.write(keyBuffer, keyBufferPosition, null, keySize);
    } else {
      nullableTypeStrategy.write(keyBuffer, keyBufferPosition, (T) rowObj, keySize);
      stack[dimensionIndex] = 1;
    }
  }

  @Override
  public boolean checkRowIndexAndAddValueToGroupingKey(
      int keyBufferPosition, Object rowObj, int rowValIdx, ByteBuffer keyBuffer
  )
  {
    return false;
  }

  @Override
  public int writeToKeyBuffer(
      int keyBufferPosition,
      ColumnValueSelector selector,
      ByteBuffer keyBuffer
  )
  {
    nullableTypeStrategy.write(keyBuffer, keyBufferPosition, getValue(selector), keySize);
    return 0;
  }

  @Override
  public Grouper.BufferComparator bufferComparator(
      int keyBufferPosition,
      @Nullable StringComparator stringComparator
  )
  {
    return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
      T lhs = nullableTypeStrategy.read(lhsBuffer, lhsPosition + keyBufferPosition);
      T rhs = nullableTypeStrategy.read(rhsBuffer, rhsPosition + keyBufferPosition);
      if (stringComparator != null
          && !DimensionComparisonUtils.isNaturalComparator(columnType.getType(), stringComparator)) {
        return stringComparator.compare(String.valueOf(lhs), String.valueOf(rhs));
      }
      // Nulls are allowed while comparing
      return nullableTypeStrategy.compare(lhs, rhs);
    };
  }


  @Override
  public void reset()
  {
    // Nothing to reset
  }

  // unifies the primitive and th
  private boolean selectorIsNull(ColumnValueSelector columnValueSelector)
  {
    if (isPrimitive && columnValueSelector.isNull()) {
      return true;
    }
    return !isPrimitive && (columnValueSelector.getObject() == null);
  }

  // Handles primitives as well, also might case
  @Nullable
  private T getValue(ColumnValueSelector columnValueSelector)
  {
    if (selectorIsNull(columnValueSelector)) {
      return null;
    }
    // case is safe
    return (T) DimensionHandlerUtils.convertObjectToType(columnValueSelector.getObject(), columnType);
  }

}
