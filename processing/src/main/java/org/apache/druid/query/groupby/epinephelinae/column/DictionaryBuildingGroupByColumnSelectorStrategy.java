package org.apache.druid.query.groupby.epinephelinae.column;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.groupby.epinephelinae.DictionaryBuilding;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.function.Function;

@NotThreadSafe
public class DictionaryBuildingGroupByColumnSelectorStrategy extends KeyMappingGroupByColumnSelectorStrategy
{

  NullableTypeStrategy nullableTypeStrategy;
  private final List<Object> dictionary = DictionaryBuilding.createDictionary();
  private final Object2IntMap<Object> reverseDictionary = DictionaryBuilding.createTreeSortedReverseDictionary(
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
      final Function<Object, Integer> footprintCompute;
      @Override
      public Pair<Object, Integer> getMultiValueHolder(
          ColumnValueSelector selector,
          Object reusableValue // Optimisation to not create and allocate something new
      )
      {
        final DimensionSelector dimensionSelector = (DimensionSelector) selector;
        final IndexedInts row = dimensionSelector.getRow();
        int footprintIncrease = 0;
        ArrayBasedIndexedInts newRow = (ArrayBasedIndexedInts) reusableValue;
        if (newRow == null) {
          newRow = new ArrayBasedIndexedInts();
        }
        int rowSize = row.size();
        newRow.ensureSize(rowSize);
        for(int i = 0; i < rowSize; ++i) {
          final String value = dimensionSelector.lookupName(row.get(i));
          final int dictId = reverseDictionary.getInt(value);
          if (dictId < 0) {
            final int nextId = dictionary.size();
            dictionary.add(value);
            reverseDictionary.put(value, nextId);
            newRow.setValue(i, nextId);
            footprintIncrease += DictionaryBuilding.estimateEntryFootprint(footprintCompute.apply(value));
          } else {
            newRow.setValue(i, dictId);
          }
        }
        newRow.setSize(rowSize);
        return Pair.of(newRow, footprintIncrease);
      }

      @Override
      public int multiValueSize(Object multiValueHolder)
      {
        return ((IndexedInts) multiValueHolder).size();
      }

      @Override
      public Pair<Integer, Integer> getIndividualValueDictId(Object multiValueHolder, int index)
      {
        // Already converted it to the dictionary id
        return Pair.of(((IndexedInts) multiValueHolder).get(index), 0);
      }
    };

    // For other types
    KeyToId<Object> keyToId2 = new KeyToId<Object>()
    {
      final Function<Object, Integer> footprintCompute;

      // Assert that Object in the return type will be properly casted
      @Override
      public Pair<Object, Integer> getMultiValueHolder(
          ColumnValueSelector selector,
          Object reusableValue
      )
      {
        final Object value = DimensionHandlerUtils.convertObjectToType(selector.getObject(), columnType);
        final int dictId = reverseDictionary.getInt(value);
        int footprintIncrease = 0;
        if (dictId < 0) {
          final int size = dictionary.size();
          dictionary.add(value);
          reverseDictionary.put(value, size);
          footprintIncrease = DictionaryBuilding.estimateEntryFootprint(footprintCompute.apply(value));

        }
        return Pair.of(value, footprintIncrease);
      }

      @Override
      public int multiValueSize(Object multiValueHolder)
      {
        return multiValueHolder == null ? 0 : 1;
      }

      @Override
      public Pair<Integer, Integer> getIndividualValueDictId(Object multiValueHolder, int index)
      {
        assert index == 0;
        int dictId = reverseDictionary.getInt(multiValueHolder);
        int footprintIncrease = 0;
        if (dictId < 0) {
          final int size = dictionary.size();
          dictionary.add(multiValueHolder);
          reverseDictionary.put(multiValueHolder, size);
          dictId = size;
          // TODO(laksh): confirm if this is the same for sorted dictionaries as well
          footprintIncrease = DictionaryBuilding.estimateEntryFootprint(footprintCompute.apply(multiValueHolder));
        }
        return Pair.of(dictId, footprintIncrease);
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

  @Override
  public void reset()
  {
    dictionary.clear();
    reverseDictionary.clear();
  }
}
