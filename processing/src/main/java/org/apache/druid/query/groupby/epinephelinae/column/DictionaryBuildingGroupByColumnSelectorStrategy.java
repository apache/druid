package org.apache.druid.query.groupby.epinephelinae.column;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.groupby.epinephelinae.DictionaryBuilding;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;

import javax.annotation.Nullable;
import java.util.List;

public class DictionaryBuildingGroupByColumnSelectorStrategy extends KeyMappingGroupByColumnSelectorStrategy
{

  NullableTypeStrategy nullableTypeStrategy;
  private final List<String> dictionary = DictionaryBuilding.createDictionary();
  private final Object2IntMap<String> reverseDictionary = DictionaryBuilding.createTreeSortedReverseDictionary(
      nullableTypeStrategy);

  public DictionaryBuildingGroupByColumnSelectorStrategy(
      @Nullable KeyToId keyToId,
      ColumnType columnType,
      NullableTypeStrategy nullableTypeStrategy,
      Object defaultValue,
      KeyMapper keyMapper
  )
  {
    // For Strings
    KeyToId<Object> keyToId1 = new KeyToId<Object>()
    {
      @Override
      public Pair<Object, Integer> getMultiValueHolder(
          ColumnValueSelector selector,
          Object reusableValue
      )
      {
        return null;
      }

      @Override
      public int multiValueSize(Object multiValueHolder)
      {
        return 0;
      }

      @Override
      public Pair<Integer, Integer> getIndividualValueDictId(
          Object multiValueHolder,
          int index
      )
      {
        return null;
      }
    };

    // For other types
    KeyToId<Object> keyToId2 = new KeyToId<Object>()
    {
      @Override
      public Pair<Object, Integer> getMultiValueHolder(
          ColumnValueSelector selector,
          Object reusableValue
      )
      {
        return
      }

      @Override
      public int multiValueSize(Object multiValueHolder)
      {
        return 0;
      }

      @Override
      public Pair<Integer, Integer> getIndividualValueDictId(Object multiValueHolder, int index)
      {
        return null;
      }
    };

    KeyMapper<Object> keyMapper1 = new KeyMapper<Object>()
    {
      @Override
      public Object idToKey(int id)
      {
        if (id != GROUP_BY_MISSING_VALUE) {
          return dictionary.get(id);
        }
        return defaultValue;
      }

      @Override
      public boolean canCompareIds()
      {
        return false;
      }
    };
  }
}
