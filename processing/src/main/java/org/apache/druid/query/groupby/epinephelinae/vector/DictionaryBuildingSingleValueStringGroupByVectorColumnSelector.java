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

package org.apache.druid.query.groupby.epinephelinae.vector;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.query.groupby.epinephelinae.DictionaryBuildingUtils;
import org.apache.druid.query.groupby.epinephelinae.column.DictionaryBuildingGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.DimensionIdCodec;
import org.apache.druid.query.groupby.epinephelinae.column.MemoryFootprint;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorObjectSelector;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link GroupByVectorColumnSelector} that builds an internal String<->Integer dictionary, used for grouping
 * single-valued STRING columns which are not natively dictionary encoded, e.g. expression virtual columns.
 *
 * @see DictionaryBuildingComplexGroupByVectorColumnSelector similar selector for complex columns
 * @see DictionaryBuildingGroupByColumnSelectorStrategy#forType(ColumnType) which creates the nonvectorized version
 */
public class DictionaryBuildingSingleValueStringGroupByVectorColumnSelector
    extends DictionaryBuildingGroupByVectorColumnSelector<String>
{
  public DictionaryBuildingSingleValueStringGroupByVectorColumnSelector(final VectorObjectSelector selector)
  {
    super(selector, new StringDimensionIdCodec());
  }

  @Override
  protected String convertValue(final Object rawValue)
  {
    return DimensionHandlerUtils.convertObjectToString(rawValue);
  }

  private static class StringDimensionIdCodec implements DimensionIdCodec<String>
  {
    private final List<String> dictionary = new ArrayList<>();
    private final Object2IntMap<String> reverseDictionary = new Object2IntOpenHashMap<>();

    StringDimensionIdCodec()
    {
      reverseDictionary.defaultReturnValue(-1);
    }

    @Override
    public MemoryFootprint<Integer> lookupId(final String value)
    {
      int dictId = reverseDictionary.getInt(value);
      int footprintIncrease = 0;
      if (dictId < 0) {
        dictId = dictionary.size();
        dictionary.add(value);
        reverseDictionary.put(value, dictId);
        footprintIncrease =
            DictionaryBuildingUtils.estimateEntryFootprint(value == null ? 0 : value.length() * Character.BYTES);
      }
      return new MemoryFootprint<>(dictId, footprintIncrease);
    }

    @Override
    public String idToKey(final int id)
    {
      return dictionary.get(id);
    }

    @Override
    public boolean canCompareIds()
    {
      return false;
    }

    @Override
    public void reset()
    {
      dictionary.clear();
      reverseDictionary.clear();
    }
  }
}
