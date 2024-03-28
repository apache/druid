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
import org.apache.druid.error.DruidException;
import org.apache.druid.query.groupby.epinephelinae.DictionaryBuildingUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;

/**
 * Strategy for grouping dimensions which can have variable-width objects, and aren't backed by prebuilt dictionaries. It
 * encapsulates the dictionary building logic, along with providing the implementations for dimension to dictionary id
 * encoding-decoding.
 * <p>
 * This strategy can handle any dimension that can be addressed on a reverse-dictionary. Reverse dictionary uses
 * a sorted map, rather than a hashmap.
 * <p>
 * This is the most expensive of all the strategies, and hence must be used only when other strategies aren't valid.
 */
@NotThreadSafe
public class DictionaryBuildingGroupByColumnSelectorStrategy<DimensionType>
    extends KeyMappingGroupByColumnSelectorStrategy<DimensionType>
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
      DimensionToIdConverter<DimensionType> dimensionToIdConverter,
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
      throw DruidException.defensive("Should use special variant which handles multi-value dimensions");
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
    final List<Object> dictionary = DictionaryBuildingUtils.createDictionary();
    final Object2IntMap<Object> reverseDictionary =
        DictionaryBuildingUtils.createReverseDictionary(columnType.getNullableStrategy());
    return new DictionaryBuildingGroupByColumnSelectorStrategy<>(
        new UniValueDimensionToIdConverter(dictionary, reverseDictionary, columnType.getNullableStrategy()),
        columnType,
        columnType.getNullableStrategy(),
        null,
        new DictionaryIdToDimensionConverter<>(dictionary),
        dictionary,
        reverseDictionary
    );
  }

  private static class UniValueDimensionToIdConverter implements DimensionToIdConverter<Object>
  {
    private final List<Object> dictionary;
    private final Object2IntMap<Object> reverseDictionary;
    @SuppressWarnings("rawtypes")
    private final NullableTypeStrategy nullableTypeStrategy;

    public UniValueDimensionToIdConverter(
        final List<Object> dictionary,
        final Object2IntMap<Object> reverseDictionary,
        final NullableTypeStrategy nullableTypeStrategy
    )
    {
      this.dictionary = dictionary;
      this.reverseDictionary = reverseDictionary;
      this.nullableTypeStrategy = nullableTypeStrategy;
    }

    @Override
    public MemoryEstimate<Integer> lookupId(Object multiValueHolder)
    {
      int dictId = reverseDictionary.getInt(multiValueHolder);
      int footprintIncrease = 0;
      // Even if called again, then this is no-op
      if (dictId < 0) {
        final int size = dictionary.size();
        dictionary.add(multiValueHolder);
        reverseDictionary.put(multiValueHolder, size);
        dictId = size;
        // MultiValueHOlder is always expected to handle the type, once the coercion is complete
        //noinspection unchecked
        footprintIncrease = DictionaryBuildingUtils.estimateEntryFootprint(
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
