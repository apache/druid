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

import com.google.common.base.Strings;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.LikeDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.Indexed;

import java.util.Iterator;

public class LikeFilter implements Filter
{
  private final String dimension;
  private final ExtractionFn extractionFn;
  private final LikeDimFilter.LikeMatcher likeMatcher;

  public LikeFilter(
      final String dimension,
      final ExtractionFn extractionFn,
      final LikeDimFilter.LikeMatcher likeMatcher
  )
  {
    this.dimension = dimension;
    this.extractionFn = extractionFn;
    this.likeMatcher = likeMatcher;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    if (extractionFn == null && likeMatcher.getSuffixMatch() == LikeDimFilter.LikeMatcher.SuffixMatch.MATCH_EMPTY) {
      // dimension equals prefix
      return selector.getBitmapIndex(dimension, likeMatcher.getPrefix());
    } else if (extractionFn == null && !likeMatcher.getPrefix().isEmpty()) {
      // dimension startsWith prefix and is accepted by likeMatcher.matchesSuffixOnly
      final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);

      if (bitmapIndex == null) {
        // Treat this as a column full of nulls
        return likeMatcher.matches(null) ? Filters.allTrue(selector) : Filters.allFalse(selector);
      }

      // search for start, end indexes in the bitmaps; then include all matching bitmaps between those points
      final Indexed<String> dimValues = selector.getDimensionValues(dimension);

      final String lower = Strings.nullToEmpty(likeMatcher.getPrefix());
      final String upper = Strings.nullToEmpty(likeMatcher.getPrefix()) + Character.MAX_VALUE;
      final int startIndex; // inclusive
      final int endIndex; // exclusive

      final int lowerFound = bitmapIndex.getIndex(lower);
      startIndex = lowerFound >= 0 ? lowerFound : -(lowerFound + 1);

      final int upperFound = bitmapIndex.getIndex(upper);
      endIndex = upperFound >= 0 ? upperFound + 1 : -(upperFound + 1);

      // Union bitmaps for all matching dimension values in range.
      // Use lazy iterator to allow unioning bitmaps one by one and avoid materializing all of them at once.
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
                  while (currIndex < endIndex && !likeMatcher.matchesSuffixOnly(dimValues.get(currIndex))) {
                    currIndex++;
                  }

                  if (currIndex == endIndex) {
                    return bitmapIndex.getBitmapFactory().makeEmptyImmutableBitmap();
                  }

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
    } else {
      // fallback
      return Filters.matchPredicate(
          dimension,
          selector,
          likeMatcher.predicateFactory(extractionFn).makeStringPredicate()
      );
    }
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(dimension, likeMatcher.predicateFactory(extractionFn));
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension) != null;
  }
}
