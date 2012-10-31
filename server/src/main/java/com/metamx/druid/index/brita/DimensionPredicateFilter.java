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

import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.metamx.common.guava.FunctionalIterable;

/**
 */
class DimensionPredicateFilter implements Filter
{
  private final String dimension;
  private final Predicate<String> predicate;

  public DimensionPredicateFilter(
      String dimension,
      Predicate<String> predicate
  )
  {
    this.dimension = dimension;
    this.predicate = predicate;
  }

  @Override
  public ImmutableConciseSet goConcise(final InvertedIndexSelector selector)
  {
    return ImmutableConciseSet.union(
        FunctionalIterable.create(selector.getDimensionValues(dimension))
                          .filter(predicate)
                          .transform(
                              new Function<String, ImmutableConciseSet>()
                              {
                                @Override
                                public ImmutableConciseSet apply(@Nullable String input)
                                {
                                  return selector.getConciseInvertedIndex(dimension, input);
                                }
                              }
                          )
    );
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(dimension, predicate);
  }
}
