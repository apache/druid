/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.data.IndexedInts;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FloatWrappingDimensionSelector implements DimensionSelector
{
  private final FloatColumnSelector delegate;
  private final ExtractionFn exFn;
  private final Map<String, Integer> valueToId = Maps.newHashMap();
  private final List<String> idToValue = Lists.newArrayList();
  private int curId;

  public FloatWrappingDimensionSelector(FloatColumnSelector selector, ExtractionFn extractionFn)
  {
    this.delegate = selector;
    this.exFn = extractionFn;
    this.curId = 0;
  }

  @Override
  public IndexedInts getRow()
  {
    float rowValFloat = delegate.get();
    String rowVal = exFn != null ? exFn.apply(rowValFloat) : String.valueOf(rowValFloat) ;
    Integer id = valueToId.get(rowVal);
    if (id == null) {
      idToValue.add(rowVal);
      valueToId.put(rowVal, curId);
      id = curId;
      curId++;
    }
    final int finalId = id;
    return new IndexedInts()
    {
      @Override
      public int size()
      {
        return 1;
      }

      @Override
      public int get(int index)
      {
        return finalId;
      }

      @Override
      public void fill(int index, int[] toFill)
      {
        throw new UnsupportedOperationException("wrapped float does not support fill");
      }

      @Override
      public void close() throws IOException
      {

      }

      @Override
      public IntIterator iterator()
      {
        return IntIterators.singleton(finalId);
      }
    };
  }

  @Override
  public int getValueCardinality()
  {
    return DimensionSelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public String lookupName(int id)
  {
    return idToValue.get(id);
  }

  @Override
  public int lookupId(String name)
  {
    throw new UnsupportedOperationException("wrapped float column does not support lookups");
  }
}
