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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

// Note: Avoiding anonymous classes
// This is more of a helper class, as it just creates an instance of the KeyMappingGroupingColumnSelectorStrategy
public class PrebuiltDictionaryStringGroupByColumnSelectorStrategy
{

  public static GroupByColumnSelectorStrategy forType(
      final ColumnType columnType,
      final ColumnValueSelector columnValueSelector,
      final ColumnCapabilities columnCapabilities
  )
  {
    if (columnType.equals(ColumnType.STRING)) {
      return forString(columnValueSelector, columnCapabilities);
    } else {
      // This can change with array columns
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

  private static class StringDimensionToIdConverter implements DimensionToIdConverter<IndexedInts>
  {
    @Override
    public Pair<IndexedInts, Integer> getMultiValueHolder(
        final ColumnValueSelector selector,
        final IndexedInts reusableValue
    )
    {
      return Pair.of(((DimensionSelector) selector).getRow(), 0);
    }

    @Override
    public int multiValueSize(IndexedInts multiValueHolder)
    {
      return multiValueHolder.size();
    }

    @Override
    public Pair<Integer, Integer> getIndividualValueDictId(IndexedInts multiValueHolder, int index)
    {
      return Pair.of(multiValueHolder.get(index), 0);
    }
  }

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
