/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package com.metamx.druid.index.brita;

import com.metamx.collections.spatial.search.Bound;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import it.uniroma3.mat.extendedset.intset.IntSet;

import java.util.Iterator;

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
  public ImmutableConciseSet goConcise(final BitmapIndexSelector selector)
  {
    return ImmutableConciseSet.union(selector.getSpatialIndex(dimension).search(bound));
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(
        dimension,
        bound
    );
  }
}
