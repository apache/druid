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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A String strategy that builds an internal String<->Integer dictionary for
 * DimensionSelectors that return false for nameLookupPossibleInAdvance()
 */
public class DictionaryBuildingStringGroupByColumnSelectorStrategy extends StringGroupByColumnSelectorStrategy
{
  private static final int GROUP_BY_MISSING_VALUE = -1;

  private int nextId = 0;
  private final List<String> dictionary = new ArrayList<>();
  private final Object2IntOpenHashMap<String> reverseDictionary = new Object2IntOpenHashMap<>();

  {
    reverseDictionary.defaultReturnValue(-1);
  }

  public DictionaryBuildingStringGroupByColumnSelectorStrategy()
  {
    super(null);
  }

  @Override
  public void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus,
      ByteBuffer key,
      ResultRow resultRow,
      int keyBufferPosition
  )
  {
    final int id = key.getInt(keyBufferPosition);

    // GROUP_BY_MISSING_VALUE is used to indicate empty rows, which are omitted from the result map.
    if (id != GROUP_BY_MISSING_VALUE) {
      final String value = dictionary.get(id);
      resultRow.set(selectorPlus.getResultRowPosition(), value);
    } else {
      resultRow.set(selectorPlus.getResultRowPosition(), NullHandling.defaultStringValue());
    }
  }

  @Override
  public void initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    final DimensionSelector dimSelector = (DimensionSelector) selector;
    final IndexedInts row = dimSelector.getRow();
    ArrayBasedIndexedInts newRow = (ArrayBasedIndexedInts) valuess[columnIndex];
    if (newRow == null) {
      newRow = new ArrayBasedIndexedInts();
      valuess[columnIndex] = newRow;
    }
    int rowSize = row.size();
    newRow.ensureSize(rowSize);
    for (int i = 0; i < rowSize; i++) {
      final String value = dimSelector.lookupName(row.get(i));
      final int dictId = reverseDictionary.getInt(value);
      if (dictId < 0) {
        dictionary.add(value);
        reverseDictionary.put(value, nextId);
        newRow.setValue(i, nextId);
        nextId++;
      } else {
        newRow.setValue(i, dictId);
      }
    }
    newRow.setSize(rowSize);
  }

  @Override
  public Object getOnlyValue(ColumnValueSelector selector)
  {
    final DimensionSelector dimSelector = (DimensionSelector) selector;
    final IndexedInts row = dimSelector.getRow();

    Preconditions.checkState(row.size() < 2, "Not supported for multi-value dimensions");

    if (row.size() == 0) {
      return GROUP_BY_MISSING_VALUE;
    }

    final String value = dimSelector.lookupName(row.get(0));
    final int dictId = reverseDictionary.getInt(value);
    if (dictId < 0) {
      dictionary.add(value);
      reverseDictionary.put(value, nextId);
      return nextId++;
    } else {
      return dictId;
    }
  }

  @Override
  public Grouper.BufferComparator bufferComparator(int keyBufferPosition, @Nullable StringComparator stringComparator)
  {
    final StringComparator realComparator = stringComparator == null ?
                                            StringComparators.LEXICOGRAPHIC :
                                            stringComparator;
    return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
      String lhsStr = dictionary.get(lhsBuffer.getInt(lhsPosition + keyBufferPosition));
      String rhsStr = dictionary.get(rhsBuffer.getInt(rhsPosition + keyBufferPosition));
      return realComparator.compare(lhsStr, rhsStr);
    };
  }
}
