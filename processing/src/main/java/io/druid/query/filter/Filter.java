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

import java.util.List;

/**
 */
public interface Filter
{
  // this should be supported, at least
  ValueMatcher makeMatcher(ValueMatcherFactory factory);

  // return true only when getBitmapIndex() is implemented
  boolean supportsBitmap();

  // optional. bitmap based filter will be applied first whenever it's possible
  ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector);

  // optional. if bitmap filter cannot be supported, this should be implemented instead
  ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory);

  // extend this
  abstract class AbstractFilter implements Filter
  {
    @Override
    public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsBitmap()
    {
      return true;
    }
  }

  // marker for and/or/not
  interface RelationalFilter {
    List<Filter> getChildren();
  }
}
