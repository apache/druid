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
  final int mappedKeySize;
  @Nullable
  final MultiValueHelper<Object> multiValueHelper;
  final boolean isPrimitive;
  final ColumnType columnType;
  final NullableTypeStrategy<DimensionType> nullableTypeStrategy;
  final Object defaultValue;
  final KeyMapper<DimensionType> keyMapper;

  @Override
  public int getGroupingKeySize()
  {
    return mappedKeySize;
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
    Pair<Object, Integer> multiValueHolderAndSizeIncrease = multiValueHelper.getMultiValueHolder(selector, null);
    valuess[columnIndex] = multiValueHelper.getMultiValueHolder(selector, null);
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
    int rowSize = multiValueHelper.multiValueSize(rowObj);
    if (rowSize == 0) {
      keyBuffer.putInt(keyBufferPosition, GROUP_BY_MISSING_VALUE);
    } else {
      keyBuffer.putInt(keyBufferPosition, multiValueHelper.getIndividualValueDictId(rowObj, 0));
    }
  }

  @Override
  public boolean checkRowIndexAndAddValueToGroupingKey( int keyBufferPosition, Object rowObj, int rowValIdx, ByteBuffer keyBuffer)
  {
    int rowSize = multiValueHelper.multiValueSize(rowObj);
    if (rowValIdx < rowSize) {
      keyBuffer.putInt(
          keyBufferPosition,
          multiValueHelper.getIndividualValueDictId(rowObj, rowValIdx)
      );
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int writeToKeyBuffer(int keyBufferPosition, ColumnValueSelector selector, ByteBuffer keyBuffer)
  {
    Object multiValueHolder = multiValueHelper.getMultiValueHolder(selector, null);
    int multiValueSize = multiValueHelper.multiValueSize(multiValueHolder);
    Preconditions.checkState(multiValueSize < 2, "Not supported for multi-value dimensions");
    final int dictId = multiValueSize == 1
                       ? multiValueHelper.getIndividualValueDictId(multiValueHolder, 0)
                       : GROUP_BY_MISSING_VALUE;
    keyBuffer.putInt(keyBufferPosition, dictId);
    return 0;
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
