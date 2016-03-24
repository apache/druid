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

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedLongs;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class NullDimensionSelector implements DimensionSelector
{
  private final ValueType type;
  private ColumnCapabilitiesImpl capabilities;

  public NullDimensionSelector(ValueType type)
  {
    this.type = type;
    this.capabilities = new ColumnCapabilitiesImpl();
    capabilities.setType(type);
    if (type == ValueType.STRING) {
      capabilities.setHasBitmapIndexes(true);
      capabilities.setDictionaryEncoded(true);
    }
  }

  private static final IndexedInts SINGLETON = new IndexedInts() {
    @Override
    public int size() {
      return 1;
    }

    @Override
    public int get(int index) {
      return 0;
    }

    @Override
    public Iterator<Integer> iterator() {
      return Iterators.singletonIterator(0);
    }

    @Override
    public void fill(int index, int[] toFill)
    {
      throw new UnsupportedOperationException("NullDimensionSelector does not support fill");
    }

    @Override
    public void close() throws IOException
    {

    }
  };

  @Override
  public IndexedInts getRow()
  {
    return SINGLETON;
  }

  @Override
  public int getValueCardinality()
  {
    return 1;
  }

  @Override
  public String lookupName(int id)
  {
    return null;
  }

  @Override
  public int lookupId(String name)
  {
    return Strings.isNullOrEmpty(name) ? 0 : -1;
  }

  @Override
  public IndexedLongs getLongRow()
  {
    return new IndexedLongs()
    {
      @Override
      public int size()
      {
        return 1;
      }

      @Override
      public long get(int index)
      {
        return 0L;
      }

      @Override
      public void fill(int index, long[] toFill)
      {
        throw new UnsupportedOperationException("NullDimensionSelector does not support fill");
      }

      @Override
      public int binarySearch(long key)
      {
        return 0;
      }

      @Override
      public int binarySearch(long key, int from, int to)
      {
        return 0;
      }

      @Override
      public void close() throws IOException
      {

      }
    };
  }

  @Override
  public Comparable getExtractedValueLong(long val)
  {
    return 0L;
  }

  @Override
  public IndexedFloats getFloatRow()
  {
    return new IndexedFloats()
    {
      @Override
      public int size()
      {
        return 1;
      }

      @Override
      public float get(int index)
      {
        return 0.0f;
      }

      @Override
      public void fill(int index, float[] toFill)
      {
        throw new UnsupportedOperationException("NullDimensionSelector does not support fill");
      }

      @Override
      public void close() throws IOException
      {

      }
    };
  }

  @Override
  public Comparable getExtractedValueFloat(float val)
  {
    return 0.0f;
  }

  @Override
  public Comparable getComparableRow()
  {
    return null;
  }

  @Override
  public Comparable getExtractedValueComparable(Comparable val)
  {
    return val;
  }

  @Override
  public ColumnCapabilities getDimCapabilities()
  {
    return capabilities;
  }
}
