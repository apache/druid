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
    final Iterator<ImmutableConciseSet> dimValueIndexesIter = selector.getSpatialIndex(dimension).search(bound)
                                                                      .iterator();
    ImmutableConciseSet retVal = ImmutableConciseSet.union(
        new Iterable<ImmutableConciseSet>()
        {
          @Override
          public Iterator<ImmutableConciseSet> iterator()
          {
            return new Iterator<ImmutableConciseSet>()
            {
              private IntSet.IntIterator iter;

              @Override
              public boolean hasNext()
              {
                return dimValueIndexesIter.hasNext() || iter.hasNext();
              }

              @Override
              public ImmutableConciseSet next()
              {
                if (iter != null && !iter.hasNext()) {
                  iter = null;
                }
                if (iter == null) {
                  ImmutableConciseSet immutableConciseSet = dimValueIndexesIter.next();
                  iter = immutableConciseSet.iterator();
                }
                return selector.getConciseInvertedIndex(dimension, iter.next());
              }

              @Override
              public void remove()
              {
                throw new UnsupportedOperationException();
              }
            };
          }
        }
    );

    return retVal;
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
