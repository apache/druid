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

package io.druid.segment.virtual;

import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

import java.io.IOException;

public abstract class AbstractSingleValueVirtualDimensionSelector implements DimensionSelector
{
  private static final IndexedInts ALWAYS_ZERO = new IndexedInts()
  {
    @Override
    public int size()
    {
      return 1;
    }

    @Override
    public int get(int index)
    {
      return 0;
    }

    @Override
    public IntIterator iterator()
    {
      return IntIterators.singleton(0);
    }

    @Override
    public void fill(int index, int[] toFill)
    {
      throw new UnsupportedOperationException("Cannot fill");
    }

    @Override
    public void close() throws IOException
    {

    }
  };

  protected abstract String getValue();

  @Override
  public IndexedInts getRow()
  {
    return ALWAYS_ZERO;
  }

  @Override
  public int getValueCardinality()
  {
    return DimensionSelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public String lookupName(int id)
  {
    return getValue();
  }

  @Override
  public int lookupId(String name)
  {
    throw new UnsupportedOperationException("Cannot lookupId on virtual dimensions");
  }
}
