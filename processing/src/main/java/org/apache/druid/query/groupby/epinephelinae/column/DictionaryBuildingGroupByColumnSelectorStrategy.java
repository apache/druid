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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.groupby.epinephelinae.DictionaryBuildingUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
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

  private DictionaryBuildingGroupByColumnSelectorStrategy(
      DimensionIdCodec<DimensionType> dimensionIdCodec,
      ColumnType columnType,
      NullableTypeStrategy<DimensionType> nullableTypeStrategy,
      DimensionType defaultValue
  )
  {
    super(dimensionIdCodec, columnType, nullableTypeStrategy, defaultValue);
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

    if (ColumnType.STRING_ARRAY.equals(columnType)) {
      forStringArrays();
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
    return new DictionaryBuildingGroupByColumnSelectorStrategy<>(
        new UniValueDimensionIdCodec(columnType.getNullableStrategy()),
        columnType,
        columnType.getNullableStrategy(),
        null
    );
  }

  private static GroupByColumnSelectorStrategy forStringArrays()
  {
    return new DictionaryBuildingGroupByColumnSelectorStrategy<>(
        new StringArrayDimensionIdCodec(),
        ColumnType.STRING_ARRAY,
        ColumnType.STRING_ARRAY.getNullableStrategy(),
        null
    );
  }

  private static class UniValueDimensionIdCodec implements DimensionIdCodec<Object>
  {
    /**
     * Dictionary for mapping the dimension value to an index. i-th position in the dictionary holds the value represented
     * by the dictionaryId "i".
     * Therefore, if a value has a dictionary id "i", dictionary.get(i) = value
     * If a -> 0, b -> 1, c -> 2 (value -> dictionaryId), then the dictionary would be laid out like: [a, b, c]
     */
    private final List<Object> dictionary;

    /**
     * Reverse dictionary for faster lookup into the dictionary, and reusing pre-existing dictionary ids.
     * <p>
     * An entry of form (value, i) in the reverse dictionary represents that "value" is present at the i-th location in the
     * {@link #dictionary}.
     * Absence of mapping of a "value" (denoted by returning {@link GroupByColumnSelectorStrategy#GROUP_BY_MISSING_VALUE})
     * represents that the value is absent in the dictionary
     * If a -> 0, b -> 1, c -> 2 (value -> dictionaryId), then the reverse dictionary would have the entries (a, 0), (b, 1),
     * (c, 2)
     */
    private final Object2IntMap<Object> reverseDictionary;

    @SuppressWarnings("rawtypes")
    private final NullableTypeStrategy nullableTypeStrategy;

    public UniValueDimensionIdCodec(final NullableTypeStrategy nullableTypeStrategy)
    {
      this.dictionary = DictionaryBuildingUtils.createDictionary();
      this.reverseDictionary = DictionaryBuildingUtils.createReverseDictionary(nullableTypeStrategy);
      this.nullableTypeStrategy = nullableTypeStrategy;
    }

    @Override
    public MemoryFootprint<Integer> lookupId(Object dimension)
    {
      int dictId = reverseDictionary.getInt(dimension);
      int footprintIncrease = 0;
      // Even if called again, then this is no-op
      if (dictId < 0) {
        final int size = dictionary.size();
        dictionary.add(dimension);
        reverseDictionary.put(dimension, size);
        dictId = size;
        // MultiValueHOlder is always expected to handle the type, once the coercion is complete
        //noinspection unchecked
        footprintIncrease = DictionaryBuildingUtils.estimateEntryFootprint(
            nullableTypeStrategy.estimateSizeBytes(dimension)
        );
      }
      return new MemoryFootprint<>(dictId, footprintIncrease);
    }

    @Override
    public Object idToKey(int id)
    {
      if (id >= dictionary.size()) {
        throw DruidException.defensive("Unknown dictionary id [%d]", id);
      }
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

    @Override
    public void reset()
    {
      dictionary.clear();
      reverseDictionary.clear();
    }
  }

  /**
   * {@link DimensionIdCodec} for string arrays. Dictionary building for string arrays is optimised to have a dual
   * dictionary - one that maps the string values to an id, and another which maps an array of these ids, to the returned
   * dictionary id. This reduces the amount of heap memory required to build the dictionaries
   */
  private static class StringArrayDimensionIdCodec implements DimensionIdCodec<Object>
  {
    // contains string <-> id for each element of the multi value grouping column
    // for eg : [a,b,c] is the col value. dictionaryToInt will contain { a <-> 1, b <-> 2, c <-> 3}
    private final BiMap<String, Integer> elementBiDictionary = HashBiMap.create();

    // stores each row as an integer array where the int represents the value in dictionaryToInt
    // for eg : [a,b,c] would be converted to [1,2,3] and assigned a integer value 1.
    // [1,2,3] <-> 1
    private final BiMap<ArrayList<Integer>, Integer> arrayBiDictionary = HashBiMap.create();

    @Override
    public MemoryFootprint<Integer> lookupId(Object dimension)
    {
      // dimension IS non-null, by contract of this method
      Object[] stringArray = (Object[]) dimension;
      ArrayList<Integer> dictionaryEncodedStringArray = new ArrayList<>();
      int estimatedFootprint = 0;
      for (Object element : stringArray) {
        String elementCasted = (String) element;
        Integer elementDictId = elementBiDictionary.get(elementCasted);
        if (elementDictId == null) {
          elementDictId = elementBiDictionary.size();
          elementBiDictionary.put(elementCasted, elementDictId);
          // We're not using the dictionary and reverseDictionary from DictionaryBuilding, but the BiMap is close enough
          // that we expect this footprint calculation to still be useful.
          estimatedFootprint +=
              DictionaryBuildingUtils.estimateEntryFootprint(elementCasted == null
                                                             ? 0
                                                             : elementCasted.length() * Character.BYTES);
        }
        dictionaryEncodedStringArray.add(elementDictId);
      }

      Integer arrayDictId = arrayBiDictionary.get(dictionaryEncodedStringArray);
      if (arrayDictId == null) {
        arrayDictId = arrayBiDictionary.size();
        arrayBiDictionary.put(dictionaryEncodedStringArray, arrayDictId);
        estimatedFootprint +=
            DictionaryBuildingUtils.estimateEntryFootprint(dictionaryEncodedStringArray.size() * Integer.BYTES);
      }
      return new MemoryFootprint<>(arrayDictId, estimatedFootprint);
    }

    @Override
    public Object idToKey(int id)
    {
      ArrayList<Integer> dictionaryEncodedStringArray = arrayBiDictionary.inverse().get(id);
      final Object[] stringRepresentation = new Object[dictionaryEncodedStringArray.size()];
      for (int i = 0; i < dictionaryEncodedStringArray.size(); ++i) {
        stringRepresentation[i] = elementBiDictionary.inverse().get(dictionaryEncodedStringArray.get(i));
      }
      return stringRepresentation;
    }

    @Override
    public boolean canCompareIds()
    {
      return false;
    }

    @Override
    public void reset()
    {
      arrayBiDictionary.clear();
      elementBiDictionary.clear();
    }
  }
}
