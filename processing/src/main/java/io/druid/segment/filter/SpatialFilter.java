/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.druid.segment.filter;

import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.search.Bound;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;

/**
 */
public class SpatialFilter implements Filter
{
  private final String dimension;
  private final Bound bound;

  public SpatialFilter(
      String dimension,
      Bound bound
  )
  {
    this.dimension = dimension;
    this.bound = bound;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    Iterable<ImmutableBitmap> search = selector.getSpatialIndex(dimension).search(bound);
    for (ImmutableBitmap immutableBitmap : search) {
      System.out.println(immutableBitmap);
    }
    return selector.getBitmapFactory().union(search);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(
        dimension,
        bound
    );
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException();
  }

}
