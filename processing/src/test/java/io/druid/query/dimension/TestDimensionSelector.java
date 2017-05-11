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

package io.druid.query.dimension;

import com.google.common.base.Predicate;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelectorUtils;
import io.druid.segment.IdLookup;
import io.druid.segment.data.ArrayBasedIndexedInts;
import io.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

/**
 * Test dimension selector that has cardinality=26
 * encoding 0 -> a, 1 -> b, ...
 * row -> [c,e,g]
 */
class TestDimensionSelector implements DimensionSelector
{
  public final static TestDimensionSelector instance = new TestDimensionSelector();

  private TestDimensionSelector()
  {

  }

  @Override
  public IndexedInts getRow()
  {
    return ArrayBasedIndexedInts.of(new int[]{2, 4, 6});
  }

  @Override
  public ValueMatcher makeValueMatcher(String value)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
  }

  @Override
  public ValueMatcher makeValueMatcher(Predicate<String> predicate)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
  }

  @Override
  public int getValueCardinality()
  {
    return 26;
  }

  @Override
  public String lookupName(int id)
  {
    return String.valueOf((char) (id + 'a'));
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return true;
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return new IdLookup()
    {
      @Override
      public int lookupId(String name)
      {
        return name.charAt(0) - 'a';
      }
    };
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // Don't care about runtime shape in tests
  }
}
