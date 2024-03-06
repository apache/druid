package org.apache.druid.query.groupby.epinephelinae.column;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.DimensionComparisonUtils;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class KeyMappingGroupByColumnSelectorStrategy<DimensionType> implements GroupByColumnSelectorStrategy
{
  @Nullable
  final KeyToId<Object> keyToId;
  final ColumnType columnType;
  final NullableTypeStrategy<DimensionType> nullableTypeStrategy;
  final Object defaultValue;
  final KeyMapper<DimensionType> keyMapper;

  public KeyMappingGroupByColumnSelectorStrategy(
      @Nullable final KeyToId<Object> keyToId,
      final ColumnType columnType,
      final NullableTypeStrategy<DimensionType> nullableTypeStrategy,
      final Object defaultValue,
      final KeyMapper<DimensionType> keyMapper
  )
  {
    this.keyToId = keyToId;
    this.columnType = columnType;
    this.nullableTypeStrategy = nullableTypeStrategy;
    this.defaultValue = defaultValue;
    this.keyMapper = keyMapper;
  }

  @Override
  public int getGroupingKeySize()
  {
    return Integer.BYTES;
  }

  @Override
  public void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus,
      ByteBuffer key,
      ResultRow resultRow,
      int keyBufferPosition
  )
  {
    final int id = key.getInt(keyBufferPosition);
    if (id != GROUP_BY_MISSING_VALUE) {
      resultRow.set(selectorPlus.getResultRowPosition(), keyMapper.idToKey(id));
    } else {
      resultRow.set(selectorPlus.getResultRowPosition(), defaultValue);
    }
  }

  @Override
  public int initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    Pair<Object, Integer> multiValueHolderAndSizeIncrease = keyToId.getMultiValueHolder(selector, null);
    valuess[columnIndex] = multiValueHolderAndSizeIncrease.lhs;
    return multiValueHolderAndSizeIncrease.rhs;
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
    int rowSize = keyToId.multiValueSize(rowObj);
    if (rowSize == 0) {
      keyBuffer.putInt(keyBufferPosition, GROUP_BY_MISSING_VALUE);
    } else {
      // No need to check here, since we'd have already accounted for it when we call
      // initColumnValues
      keyBuffer.putInt(keyBufferPosition, keyToId.getIndividualValueDictId(rowObj, 0).lhs);
    }
  }

  @Override
  public boolean checkRowIndexAndAddValueToGroupingKey(
      int keyBufferPosition,
      Object rowObj,
      int rowValIdx,
      ByteBuffer keyBuffer
  )
  {
    int rowSize = keyToId.multiValueSize(rowObj);
    if (rowValIdx < rowSize) {
      keyBuffer.putInt(
          keyBufferPosition,
          keyToId.getIndividualValueDictId(rowObj, rowValIdx).lhs
      );
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int writeToKeyBuffer(int keyBufferPosition, ColumnValueSelector selector, ByteBuffer keyBuffer)
  {
    Object multiValueHolder = keyToId.getMultiValueHolder(selector, null);
    int multiValueSize = keyToId.multiValueSize(multiValueHolder);
    Preconditions.checkState(multiValueSize < 2, "Not supported for multi-value dimensions");
    Pair<Integer, Integer> dictIdAndSizeIncrease = keyToId.getIndividualValueDictId(multiValueHolder, 0);
    final int dictId = multiValueSize == 1
                       ? dictIdAndSizeIncrease.lhs
                       : GROUP_BY_MISSING_VALUE;
    keyBuffer.putInt(keyBufferPosition, dictId);
    return dictIdAndSizeIncrease.rhs;
  }

  @Override
  public Grouper.BufferComparator bufferComparator(int keyBufferPosition, @Nullable StringComparator stringComparator)
  {
    boolean usesNaturalComparator =
        stringComparator == null
        || DimensionComparisonUtils.isNaturalComparator(columnType.getType(), stringComparator);
    if (keyMapper.canCompareIds() && usesNaturalComparator) {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Integer.compare(
          lhsBuffer.getInt(lhsPosition + keyBufferPosition),
          rhsBuffer.getInt(rhsPosition + keyBufferPosition)
      );
    } else {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
        Object lhsObject = keyMapper.idToKey(lhsBuffer.getInt(lhsPosition + keyBufferPosition));
        Object rhsObject = keyMapper.idToKey(rhsBuffer.getInt(rhsPosition + keyBufferPosition));
        if (usesNaturalComparator) {
          return nullableTypeStrategy.compare(
              (DimensionType) DimensionHandlerUtils.convertObjectToType(lhsObject, columnType),
              (DimensionType) DimensionHandlerUtils.convertObjectToType(rhsObject, columnType)
          );
        } else {
          return stringComparator.compare(String.valueOf(lhsObject), String.valueOf(rhsObject));
        }
      };
    }
  }

  @Override
  public void reset()
  {

  }

  // Doesn't handle GROUP_BY_MISSING_VALUE, should be done by the callers
  public interface KeyMapper<KeyType>
  {
    KeyType idToKey(int id);

    boolean canCompareIds();
  }
}
