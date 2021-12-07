package org.apache.druid.query.groupby.epinephelinae.column;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ArrayGroupByColumnSelectorStrategy
    implements GroupByColumnSelectorStrategy
{
  private static final int GROUP_BY_MISSING_VALUE = -1;


  // contains string <-> id for each element of the multi value grouping column
  // for eg : [a,b,c] is the col value. dictionaryToInt will contain { a <-> 1, b <-> 2, c <-> 3}
  private final BiMap<String, Integer> dictionaryToInt = HashBiMap.create();

  // stores each row as a integer array where the int represents the value in dictionaryToInt
  // for eg : [a,b,c] would be converted to [1,2,3]
  private final List<int[]> indexedIntArrays = new ArrayList<>();

  private final Object2IntOpenHashMap<int[]> reverseDictionary = new Object2IntOpenHashMap<>();


  @Override
  public int getGroupingKeySize()
  {
    return Integer.BYTES;
  }

  public ArrayGroupByColumnSelectorStrategy()
  {
    reverseDictionary.defaultReturnValue(GROUP_BY_MISSING_VALUE);
  }

  @Override
  public void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus, ByteBuffer key, ResultRow resultRow, int keyBufferPosition
  )
  {
    final int id = key.getInt(keyBufferPosition);

    // GROUP_BY_MISSING_VALUE is used to indicate empty rows
    if (id != GROUP_BY_MISSING_VALUE) {
      final int[] value = indexedIntArrays.get(id);
      String[] rowValues = new String[value.length];
      for (int i = 0; i < value.length; i++) {
        rowValues[i] = dictionaryToInt.inverse().get(value[i]);

      }
      resultRow.set(selectorPlus.getResultRowPosition(), rowValues);
    } else {
      resultRow.set(selectorPlus.getResultRowPosition(), NullHandling.defaultStringValues());
    }

  }

  @Override
  public void initColumnValues(
      ColumnValueSelector selector, int columnIndex, Object[] valuess
  )
  {
    throw new UOE(String.format(
        "%s does not implement initColumnValues()",
        ArrayGroupByColumnSelectorStrategy.class.getSimpleName()
    ));
  }

  @Override
  public void initGroupingKeyColumnValue(
      int keyBufferPosition, int columnIndex, Object rowObj, ByteBuffer keyBuffer, int[] stack
  )
  {
    throw new UOE(String.format(
        "%s does not implement initGroupingKeyColumnValue()",
        ArrayGroupByColumnSelectorStrategy.class.getSimpleName()
    ));
  }

  @Override
  public boolean checkRowIndexAndAddValueToGroupingKey(
      int keyBufferPosition, Object rowObj, int rowValIdx, ByteBuffer keyBuffer
  )
  {
    throw new UOE(String.format(
        "%s does not implement checkRowIndexAndAddValueToGroupingKey()",
        ArrayGroupByColumnSelectorStrategy.class.getSimpleName()
    ));
  }

  @Override
  public Object getOnlyValue(ColumnValueSelector selector)
  {
    final DimensionSelector dimSelector = (DimensionSelector) selector;
    final IndexedInts indexedRow = dimSelector.getRow();

    final int rowSize = indexedRow.size();
    if (rowSize == 0) {
      return GROUP_BY_MISSING_VALUE;
    }

    final int[] intRepresentation = new int[rowSize];
    //TODO(karan): remove intial check of null
    String firstValue = dimSelector.lookupName(indexedRow.get(0));
    if (firstValue == null && rowSize == 1) {
      return GROUP_BY_MISSING_VALUE;
    }
    intRepresentation[0] = addToIndexedDictionary(firstValue);
    for (int i = 1; i < rowSize; i++) {
      intRepresentation[i] = addToIndexedDictionary(dimSelector.lookupName(indexedRow.get(i)));
    }


    final int dictId = reverseDictionary.getInt(intRepresentation);
    if (dictId < 0) {
      final int dictionarySize = indexedIntArrays.size();
      indexedIntArrays.add(intRepresentation);
      reverseDictionary.put(intRepresentation, dictionarySize);
      return dictionarySize;
    } else {
      return dictId;
    }
  }

  private int addToIndexedDictionary(String value)
  {
    final Integer dictId = dictionaryToInt.get(value);
    if (dictId == null) {
      final int size = dictionaryToInt.size();
      dictionaryToInt.put(value, dictionaryToInt.size());
      return size;
    } else {
      return dictId;
    }
  }

  @Override
  public void writeToKeyBuffer(int keyBufferPosition, Object obj, ByteBuffer keyBuffer)
  {
    keyBuffer.putInt(keyBufferPosition, (int) obj);
  }

  @Override
  public Grouper.BufferComparator bufferComparator(
      int keyBufferPosition, @Nullable StringComparator stringComparator
  )
  {
    final StringComparator comparator = stringComparator == null ? StringComparators.LEXICOGRAPHIC : stringComparator;
    return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
      int[] lhs = indexedIntArrays.get(lhsBuffer.getInt(lhsPosition + keyBufferPosition));
      int[] rhs = indexedIntArrays.get(rhsBuffer.getInt(rhsPosition + keyBufferPosition));

      int minLength = Math.min(lhs.length, rhs.length);
      //noinspection ArrayEquality
      if (lhs == rhs) {
        return 0;
      } else {
        for (int i = 0; i < minLength; i++) {
          final int cmp = comparator.compare(
              dictionaryToInt.inverse().get(lhs[i]),
              dictionaryToInt.inverse().get(rhs[i])
          );
          if (cmp == 0) {
            continue;
          }
          return cmp;
        }
        if (lhs.length == rhs.length) {
          return 0;
        } else if (lhs.length < rhs.length) {
          return -1;
        }
        return 1;
      }
    };
  }
}

