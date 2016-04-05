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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 */
public class InFilter implements Filter
{
  private final String dimension;
  private final Set<String> values;
  private final ExtractionFn extractionFn;

  public InFilter(String dimension, Set<String> values, ExtractionFn extractionFn)
  {
    this.dimension = dimension;
    this.values = values;
    this.extractionFn = extractionFn;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    if (extractionFn == null) {
      return selector.getBitmapFactory().union(
          Iterables.transform(
              values, new Function<String, ImmutableBitmap>()
              {
                @Override
                public ImmutableBitmap apply(String value)
                {
                  return selector.getBitmapIndex(dimension, value);
                }
              }
          )
      );
    } else {
      Iterable<String> allDimVals = selector.getDimensionValues(dimension);
      if (allDimVals == null) {
        allDimVals = Lists.newArrayList((String) null);
      }

      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      for (String dimVal : allDimVals) {
        if (values.contains(Strings.nullToEmpty(extractionFn.apply(dimVal)))) {
          bitmaps.add(selector.getBitmapIndex(dimension, dimVal));
        }
      }
      return selector.getBitmapFactory().union(bitmaps);
    }
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return factory.makeValueMatcher(
        dimension, new Predicate<String>()
        {
          @Override
          public boolean apply(String input)
          {
            if (extractionFn != null) {
              input = extractionFn.apply(input);
            }
            return values.contains(Strings.nullToEmpty(input));
          }
        }
    );
  }
}
