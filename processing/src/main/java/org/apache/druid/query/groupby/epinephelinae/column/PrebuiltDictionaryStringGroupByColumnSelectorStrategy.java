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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

/**
 * Implementation of {@link KeyMappingGroupByColumnSelectorStrategy} that relies on a prebuilt dictionary to map the
 * dimension to the dictionaryId. It is more like a helper class, that handles the different ways that dictionaries can be
 * provided for different types. Currently, it only handles String dimensions. Array dimensions are also backed by dictionaries,
 * but not exposed via the ColumnValueSelector interface, hence this strategy cannot handle array dimensions.
 */
public class PrebuiltDictionaryStringGroupByColumnSelectorStrategy
{

  /**
   * Create the strategy for the provided column type
   */
  public static GroupByColumnSelectorStrategy forType(
      final ColumnType columnType,
      final ColumnValueSelector columnValueSelector,
      final ColumnCapabilities columnCapabilities
  )
  {
    if (columnType.equals(ColumnType.STRING)) {
      return forString(columnValueSelector, columnCapabilities);
    } else {
      // This will change with array columns
      throw DruidException.defensive("Only string columns expose prebuilt dictionaries");
    }
  }

  private static GroupByColumnSelectorStrategy forString(
      final ColumnValueSelector columnValueSelector,
      final ColumnCapabilities columnCapabilities
  )
  {
    return new KeyMappingGroupByColumnSelectorStrategy<>(
        new StringDimensionToIdConverter(),
        ColumnType.STRING,
        ColumnType.STRING.getNullableStrategy(),
        NullHandling.defaultStringValue(),
        new StringIdToDimensionConverter((DimensionSelector) columnValueSelector, columnCapabilities)
    );
  }

  /**
   * Dimension to id converter for string dimensions and {@link DimensionSelector}, where the dictionaries are prebuilt.
   * The callers must ensure that's the case by checking that {@link DimensionSelector#getValueCardinality()} is known
   * and {@link DimensionSelector#nameLookupPossibleInAdvance()} is true.
   */
  private static class StringDimensionToIdConverter implements DimensionToIdConverter<IndexedInts>
  {
    @Override
    public MemoryEstimate<IndexedInts> getMultiValueHolder(
        final ColumnValueSelector selector,
        final IndexedInts reusableValue
    )
    {
      return new MemoryEstimate<>(((DimensionSelector) selector).getRow(), 0);
    }

    @Override
    public int multiValueSize(IndexedInts multiValueHolder)
    {
      return multiValueHolder.size();
    }

    @Override
    public MemoryEstimate<Integer> getIndividualValueDictId(IndexedInts multiValueHolder, int index)
    {
      // dictId is already encoded in the indexedInt supplied by the column value selector
      return new MemoryEstimate<>(multiValueHolder.get(index), 0);
    }
  }

  /**
   * ID to dimension converter for {@link DimensionSelector} with prebuilt dictionary
   */
  private static class StringIdToDimensionConverter implements IdToDimensionConverter<String>
  {

    final DimensionSelector dimensionSelector;

    @Nullable
    final ColumnCapabilities columnCapabilities;

    public StringIdToDimensionConverter(
        final DimensionSelector dimensionSelector,
        @Nullable final ColumnCapabilities columnCapabilities
    )
    {
      this.dimensionSelector = dimensionSelector;
      this.columnCapabilities = columnCapabilities;
    }

    @Override
    public String idToKey(int id)
    {
      // Converting back to the value is as simple as looking up the value in the prebuilt dictionary
      return dimensionSelector.lookupName(id);
    }

    @Override
    public boolean canCompareIds()
    {
      return columnCapabilities != null
             && columnCapabilities.hasBitmapIndexes()
             && (columnCapabilities.areDictionaryValuesSorted()
                                   .and(columnCapabilities.areDictionaryValuesUnique())).isTrue();
    }
  }
}
