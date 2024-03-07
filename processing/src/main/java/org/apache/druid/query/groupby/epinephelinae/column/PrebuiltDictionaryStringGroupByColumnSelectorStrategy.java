package org.apache.druid.query.groupby.epinephelinae.column;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

public class PrebuiltDictionaryStringGroupByColumnSelectorStrategy extends KeyMappingGroupByColumnSelectorStrategy
{
  public PrebuiltDictionaryStringGroupByColumnSelectorStrategy(
      @Nullable KeyToId keyToId,
      ColumnType columnType,
      NullableTypeStrategy nullableTypeStrategy,
      Object defaultValue,
      KeyMapper keyMapper
  )
  {
    DimensionSelector dimS;
    KeyToId<Object> keyToId1 = new KeyToId<Object>()
    {
      @Override
      public Pair<Object, Integer> getMultiValueHolder(ColumnValueSelector selector, Object reusableValue)
      {
        return Pair.of(((DimensionSelector) selector).getRow(), 0);
      }

      @Override
      public int multiValueSize(Object multiValueHolder)
      {
        return ((IndexedInts) multiValueHolder).size();
      }

      @Override
      public Pair<Integer, Integer> getIndividualValueDictId(Object multiValueHolder, int index)
      {
        return Pair.of(((IndexedInts) multiValueHolder).get(index), 0);
      }
    };

    KeyMapper<String> keyMapper1 = new KeyMapper<String>()
    {
      @Override
      public String idToKey(int id)
      {
        return dimS.lookupName(id);
      }

      @Override
      public boolean canCompareIds()
      {
        return false;
      }
    };
  }
}
