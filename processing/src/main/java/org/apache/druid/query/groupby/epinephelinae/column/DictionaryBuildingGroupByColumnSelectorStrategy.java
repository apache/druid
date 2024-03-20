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
import org.apache.druid.query.groupby.epinephelinae.DictionaryBuilding;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;

/**
 * Strategy for grouping dimensions which can have variable-width objects, and aren't backed by prebuilt dictionaries. It
 * encapsulates the dictionary building logic, along with providing the implementations for dimension to dictionary id
 * encoding-decoding.
 * <p>
 * This strategy can handle any dimension that can be addressed on a reverse-dictionary. Reverse dictionary uses
 * a sorted map, rather than a hashmap.
 * TODO(laksh): Benchmark results
 * <p>
 * This is the most expensive of all the strategies, and hence must be used only when other strategies aren't valid.
 */
@NotThreadSafe
public class DictionaryBuildingGroupByColumnSelectorStrategy<DimensionType, DimensionHolderType>
    extends KeyMappingGroupByColumnSelectorStrategy<DimensionType, DimensionHolderType>
{

  /**
   * Dictionary for mapping the dimension value to an index. i-th position in the dictionary holds the value represented
   * by the dictionaryId "i".
   * Therefore, if a value has a dictionary id "i", dictionary.get(i) = value
   */
  private final List<DimensionType> dictionary;

  /**
   * Reverse dictionary for faster lookup into the dictionary, and reusing pre-existing dictionary ids.
   * <p>
   * An entry of form (value, i) in the reverse dictionary represents that "value" is present at the i-th location in the
   * {@link #dictionary}.
   * Absence of mapping of a "value" (denoted by returning {@link GroupByColumnSelectorStrategy#GROUP_BY_MISSING_VALUE})
   * represents that the value is absent in the dictionary
   */
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

  /**
   * Creates an implementation of the strategy for the given type
   */
  public static GroupByColumnSelectorStrategy forType(final ColumnType columnType)
  {
    if (columnType.equals(ColumnType.STRING)) {
      // String types are handled specially because they can have multi-value dimensions
      return forString();
    } else if (
      // Defensive check, primitives should be using a faster fixed-width strategy
        columnType.equals(ColumnType.DOUBLE)
        || columnType.equals(ColumnType.FLOAT)
        || columnType.equals(ColumnType.LONG)) {
      throw DruidException.defensive("Could used a fixed width strategy");
    }

    // Catch-all for all other types, that can only have single-valued dimensions
    return forArrayAndComplexTypes(columnType);
  }

  /**
   * Implementation of the dictionary building strategy for string types.
   */
  private static GroupByColumnSelectorStrategy forString()
  {
    final List<String> dictionary = DictionaryBuilding.createDictionary();
    final Object2IntMap<String> reverseDictionary =
        DictionaryBuilding.createReverseDictionary();
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

  /**
   * Implemenatation of dictionary building strategy for types other than strings (since they can be multi-valued and need
   * to be handled separately) and numeric primitives (since they can be handled by fixed-width strategy).
   * This also means that we handle array and complex types here, which simplifies the generics a lot, as everything can be
   * treated as Object in this class.
   * <p>
   * Also, there isn't any concept of multi-values here, therefore Dimension == DimensionHolderType == Object. We still
   * homogenize rogue selectors which can return non-standard implementation of arrays (like Long[] for long arrays instead of
   * Object[]) to what the callers would expect (i.e. Object[] in this case).
   */
  private static GroupByColumnSelectorStrategy forArrayAndComplexTypes(final ColumnType columnType)
  {
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

  /**
   * Encodes the multi-valued string dimension to the ids. It replaces the original IndexedInts, with the one containing
   * the global dictionary ids, This removes an extra redirection involved while looking up the value.
   *
   * Therefore, if the input dimension column has two rows, with dimensions like:
   *
   * (Input)
   * Column1 - [1, 2] - lookupName(1) = foo, lookupName(2) = bar
   * Column2 - [1, 2, 2] - lookupName(1) = baz, lookupName(2) = foo
   *
   * The multi-value holders for the column, after conversion would look like:
   * Column1 - [1, 2]
   * Column2 - [3, 1]
   *
   * And the dictionary-reverse dictionary would look like:
   * Dictionary: [foo, bar, baz]
   * Reverse dictionary: (foo, 1), (bar, 2), (baz, 3)
   *
   * Converting a value from the returned row to the dictId is as simple as fetching the int present at the given location.
   */
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
    public MemoryEstimate<IndexedInts> getMultiValueHolder(
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
      return new MemoryEstimate<>(newRow, footprintIncrease);
    }

    @Override
    public int multiValueSize(IndexedInts multiValueHolder)
    {
      return multiValueHolder.size();
    }

    @Override
    public MemoryEstimate<Integer> getIndividualValueDictId(IndexedInts multiValueHolder, int index)
    {
      // Already converted it to the dictionary id
      return new MemoryEstimate<>(multiValueHolder.get(index), 0);
    }
  }

  private static class UniValueDimensionToIdConverter implements DimensionToIdConverter<Object>
  {
    private final List<Object> dictionary;
    private final Object2IntMap<Object> reverseDictionary;
    private final ColumnType columnType;
    @SuppressWarnings("rawtypes")
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
    public MemoryEstimate<Object> getMultiValueHolder(ColumnValueSelector selector, Object reusableValue)
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
      return new MemoryEstimate<>(value, footprintIncrease);
    }

    @Override
    public int multiValueSize(Object multiValueHolder)
    {
      //noinspection VariableNotUsedInsideIf
      return multiValueHolder == null ? 0 : 1;
    }

    @Override
    public MemoryEstimate<Integer> getIndividualValueDictId(Object multiValueHolder, int index)
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
        // MultiValueHOlder is always expected to handle the type, once the coercion is complete
        //noinspection unchecked
        footprintIncrease = DictionaryBuilding.estimateEntryFootprint(
            nullableTypeStrategy.estimateSizeBytes(multiValueHolder)
        );
      }
      return new MemoryEstimate<>(dictId, footprintIncrease);

    }
  }

  /**
   * Defers to the dictionary we have built to decode the dictionary id
   */
  private static class DictionaryIdToDimensionConverter<DimensionType> implements IdToDimensionConverter<DimensionType>
  {
    private final List<DimensionType> dictionary;

    public DictionaryIdToDimensionConverter(List<DimensionType> dictionary)
    {
      this.dictionary = dictionary;
    }

    @Override
    public DimensionType idToKey(int id)
    {
      // No need to handle GROUP_BY_MISSING_VALUE, by contract
      return dictionary.get(id);
    }

    @Override
    public boolean canCompareIds()
    {
      // Dictionaries are built on the fly, and ids are assigned in the order in which the value is added to the
      // dictionary.
      return false;
    }
  }

  @Override
  public void reset()
  {
    super.reset();
    // Clean up the dictionaries
    dictionary.clear();
    reverseDictionary.clear();
  }
}
