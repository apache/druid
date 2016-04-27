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

package io.druid.segment.filter;

import com.google.common.base.Predicate;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.BitmapIndex;

import java.util.Comparator;
import java.util.Iterator;

public class BoundFilter implements Filter
{
  private final BoundDimFilter boundDimFilter;
  private final Comparator<String> comparator;
  private final ExtractionFn extractionFn;

  public BoundFilter(final BoundDimFilter boundDimFilter)
  {
    this.boundDimFilter = boundDimFilter;
    this.comparator = boundDimFilter.isAlphaNumeric()
                      ? StringComparators.ALPHANUMERIC
                      : StringComparators.LEXICOGRAPHIC;
    this.extractionFn = boundDimFilter.getExtractionFn();
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    if (boundDimFilter.isAlphaNumeric() || extractionFn != null) {
      return Filters.matchPredicate(
          boundDimFilter.getDimension(),
          selector,
          new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              return doesMatch(input);
            }
          }
      );
    } else {
      final BitmapIndex bitmapIndex = selector.getBitmapIndex(boundDimFilter.getDimension());

      if (bitmapIndex == null || bitmapIndex.getCardinality() == 0) {
        if (doesMatch(null)) {
          return selector.getBitmapFactory()
                         .complement(selector.getBitmapFactory().makeEmptyImmutableBitmap(), selector.getNumRows());
        } else {
          return selector.getBitmapFactory().makeEmptyImmutableBitmap();
        }
      }

      // search for start, end indexes in the bitmaps; then include all bitmaps between those points

      final int startIndex; // inclusive
      final int endIndex; // exclusive

      if (!boundDimFilter.hasLowerBound()) {
        startIndex = 0;
      } else {
        final int found = bitmapIndex.getIndex(boundDimFilter.getLower());
        if (found >= 0) {
          startIndex = boundDimFilter.isLowerStrict() ? found + 1 : found;
        } else {
          startIndex = -(found + 1);
        }
      }

      if (!boundDimFilter.hasUpperBound()) {
        endIndex = bitmapIndex.getCardinality();
      } else {
        final int found = bitmapIndex.getIndex(boundDimFilter.getUpper());
        if (found >= 0) {
          endIndex = boundDimFilter.isUpperStrict() ? found : found + 1;
        } else {
          endIndex = -(found + 1);
        }
      }

      return selector.getBitmapFactory().union(
          new Iterable<ImmutableBitmap>()
          {
            @Override
            public Iterator<ImmutableBitmap> iterator()
            {
              return new Iterator<ImmutableBitmap>()
              {
                int currIndex = startIndex;

                @Override
                public boolean hasNext()
                {
                  return currIndex < endIndex;
                }

                @Override
                public ImmutableBitmap next()
                {
                  return bitmapIndex.getBitmap(currIndex++);
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
    }
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(
        boundDimFilter.getDimension(),
        new Predicate<String>()
        {
          @Override
          public boolean apply(String input)
          {
            return doesMatch(input);
          }
        }
    );
  }

  private boolean doesMatch(String input)
  {
    if (extractionFn != null) {
      input = extractionFn.apply(input);
    }

    if (input == null) {
      return (!boundDimFilter.hasLowerBound()
              || (boundDimFilter.getLower().isEmpty() && !boundDimFilter.isLowerStrict())) // lower bound allows null
             && (!boundDimFilter.hasUpperBound()
                 || !boundDimFilter.getUpper().isEmpty()
                 || !boundDimFilter.isUpperStrict()) // upper bound allows null
          ;
    }
    int lowerComparing = 1;
    int upperComparing = 1;
    if (boundDimFilter.hasLowerBound()) {
      lowerComparing = comparator.compare(input, boundDimFilter.getLower());
    }
    if (boundDimFilter.hasUpperBound()) {
      upperComparing = comparator.compare(boundDimFilter.getUpper(), input);
    }
    if (boundDimFilter.isLowerStrict() && boundDimFilter.isUpperStrict()) {
      return ((lowerComparing > 0)) && (upperComparing > 0);
    } else if (boundDimFilter.isLowerStrict()) {
      return (lowerComparing > 0) && (upperComparing >= 0);
    } else if (boundDimFilter.isUpperStrict()) {
      return (lowerComparing >= 0) && (upperComparing > 0);
    }
    return (lowerComparing >= 0) && (upperComparing >= 0);
  }
}
