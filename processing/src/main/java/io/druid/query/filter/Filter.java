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

package io.druid.query.filter;

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.filter.Filters;

import java.util.List;

/**
 */
public interface Filter
{
  ValueMatcher makeMatcher(ValueMatcherFactory factory);

  // return true only when getBitmapIndex() is implemented
  boolean supportsBitmap();

  // bitmap based filter will be applied whenever it's possible
  ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector);

  // used when bitmap filter cannot be applied
  ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory);

  abstract class WithDictionary implements Filter
  {
    @Override
    public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
    {
      throw new UnsupportedOperationException("makeMatcher");
    }

    @Override
    public boolean supportsBitmap()
    {
      return true;
    }
  }

  abstract class WithoutDictionary implements Filter
  {
    public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector) {
      throw new UnsupportedOperationException("getBitmapIndex");
    }

    @Override
    public boolean supportsBitmap()
    {
      return false;
    }
  }

  // marker for and/or/not
  interface Relational extends Filter
  {
    List<Filter> getChildren();
  }
}
