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

package org.apache.druid.query.groupby.epinephelinae.column;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.groupby.epinephelinae.DictionaryBuilding;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;

import java.util.List;

public class DictionaryBuildingGroupByColumnSelectorStrategy<DimensionType, DimensionHolderType>
    extends KeyMappingGroupByColumnSelectorStrategy<DimensionType, DimensionHolderType>
{

  private final List<DimensionType> dictionary;
  private final Object2IntMap<DimensionType> reverseDictionary;

  private DictionaryBuildingGroupByColumnSelectorStrategy(
      DimensionToIdConverter<DimensionHolderType> dimensionToIdConverter,
      ColumnType columnType,
      NullableTypeStrategy<DimensionType> nullableTypeStrategy,
      DimensionType defaultValue,
      IdToDimensionConverter<DimensionType> idToDimensionConverter,
      List<DimensionType> dictionary,
      Object2IntMap<DimensionType> reverseDictionary
  )
  {
    super(dimensionToIdConverter, columnType, nullableTypeStrategy, defaultValue, idToDimensionConverter);
    this.dictionary = dictionary;
    this.reverseDictionary = reverseDictionary;
  }

  public static GroupByColumnSelectorStrategy forType(final ColumnType columnType)
  {
    // Any way to use the generics here instead of <Object>
    if (columnType.equals(ColumnType.STRING)) {
      return forString();
    } else if (columnType.equals(ColumnType.DOUBLE) || columnType.equals(ColumnType.FLOAT) || columnType.equals(
        ColumnType.LONG)) {
      throw DruidException.defensive("Could used a fixed width strategy");
    }

    return forArrayAndComplexTypes(columnType);
  }

  private static GroupByColumnSelectorStrategy forString()
  {
    final List<String> dictionary = DictionaryBuilding.createDictionary();
    final Object2IntMap<String> reverseDictionary =
        DictionaryBuilding.createTreeSortedReverseDictionary(ColumnType.STRING.getNullableStrategy());
    return new DictionaryBuildingGroupByColumnSelectorStrategy<>(
        new StringDimensionToIdConverter(dictionary, reverseDictionary),
        ColumnType.STRING,
        ColumnType.STRING.getNullableStrategy(),
        NullHandling.defaultStringValue(),
        new DictionaryIdToDimensionConverter<>(dictionary),
        dictionary,
        reverseDictionary
    );
  }

  // Nothing different about primitive and non-primitive types, however the primitive types are fixed width, therefore
  // don't need to use dictionary building strategy. Also, it simplifies the generics because now everything can be treated
  // as Object
  private static GroupByColumnSelectorStrategy forArrayAndComplexTypes(final ColumnType columnType)
  {
    // No concept of multi values, therefore DimensionType == DimensionHolderType == Object. For rogue selectors, which
    // can return weird representation of arrays, we cast it using DimensionHandlerUtils, therefore the type might not be strictly
    // same, but it would be what the callers expect
    final List<Object> dictionary = DictionaryBuilding.createDictionary();
    final Object2IntMap<Object> reverseDictionary =
        DictionaryBuilding.createTreeSortedReverseDictionary(columnType.getNullableStrategy());
    return new DictionaryBuildingGroupByColumnSelectorStrategy<>(
        new UniValueDimensionToIdConverter(dictionary, reverseDictionary, columnType, columnType.getNullableStrategy()),
        columnType,
        columnType.getNullableStrategy(),
        null,
        new DictionaryIdToDimensionConverter<>(dictionary),
        dictionary,
        reverseDictionary
    );
  }

  private static class StringDimensionToIdConverter implements DimensionToIdConverter<IndexedInts>
  {

    private final List<String> dictionary;
    private final Object2IntMap<String> reverseDictionary;

    public StringDimensionToIdConverter(
        List<String> dictionary,
        Object2IntMap<String> reverseDictionary
    )
    {
      this.dictionary = dictionary;
      this.reverseDictionary = reverseDictionary;
    }

    @Override
    public Pair<IndexedInts, Integer> getMultiValueHolder(
        final ColumnValueSelector selector,
        final IndexedInts reusableValue
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
      for (int i = 0; i < rowSize; ++i) {
        final String value = dimensionSelector.lookupName(row.get(i));
        final int dictId = reverseDictionary.getInt(value);
        if (dictId < 0) {
          final int nextId = dictionary.size();
          dictionary.add(value);
          reverseDictionary.put(value, nextId);
          newRow.setValue(i, nextId);
          footprintIncrease += DictionaryBuilding.estimateEntryFootprint(
              (value == null ? 0 : value.length()) * Character.BYTES
          );
        } else {
          newRow.setValue(i, dictId);
        }
      }
      newRow.setSize(rowSize);
      return Pair.of(newRow, footprintIncrease);
    }

    @Override
    public int multiValueSize(IndexedInts multiValueHolder)
    {
      return multiValueHolder.size();
    }

    @Override
    public Pair<Integer, Integer> getIndividualValueDictId(IndexedInts multiValueHolder, int index)
    {
      // Already converted it to the dictionary id
      return Pair.of(multiValueHolder.get(index), 0);
    }
  }

  private static class UniValueDimensionToIdConverter implements DimensionToIdConverter<Object>
  {
    private final List<Object> dictionary;
    private final Object2IntMap<Object> reverseDictionary;
    private final ColumnType columnType;
    private final NullableTypeStrategy nullableTypeStrategy;

    public UniValueDimensionToIdConverter(
        final List<Object> dictionary,
        final Object2IntMap<Object> reverseDictionary,
        final ColumnType columnType,
        final NullableTypeStrategy nullableTypeStrategy
    )
    {
      this.dictionary = dictionary;
      this.reverseDictionary = reverseDictionary;
      this.columnType = columnType;
      this.nullableTypeStrategy = nullableTypeStrategy;
    }

    @Override
    public Pair<Object, Integer> getMultiValueHolder(ColumnValueSelector selector, Object reusableValue)
    {
      final Object value = DimensionHandlerUtils.convertObjectToType(selector.getObject(), columnType);
      final int dictId = reverseDictionary.getInt(value);
      int footprintIncrease = 0;
      if (dictId < 0) {
        final int size = dictionary.size();
        dictionary.add(value);
        reverseDictionary.put(value, size);
        footprintIncrease = DictionaryBuilding.estimateEntryFootprint(nullableTypeStrategy.estimateSizeBytes(value));

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
      // Even if called again, then this is no-op
      if (dictId < 0) {
        final int size = dictionary.size();
        dictionary.add(multiValueHolder);
        reverseDictionary.put(multiValueHolder, size);
        dictId = size;
        // TODO(laksh): confirm if this is the same for sorted dictionaries as well
        footprintIncrease = DictionaryBuilding.estimateEntryFootprint(nullableTypeStrategy.estimateSizeBytes(
            multiValueHolder));
      }
      return Pair.of(dictId, footprintIncrease);

    }
  }

  private static class DictionaryIdToDimensionConverter<DimensionType> implements IdToDimensionConverter<DimensionType>
  {
    private final List<DimensionType> dictionary;

    public DictionaryIdToDimensionConverter(List<DimensionType> dictionary)
    {
      this.dictionary = dictionary;
    }

    // Don't need to handle default id value
    @Override
    public DimensionType idToKey(int id)
    {
      return dictionary.get(id);
    }

    @Override
    public boolean canCompareIds()
    {
      return false;
    }
  }

  @Override
  public void reset()
  {
    dictionary.clear();
    reverseDictionary.clear();
  }
}
