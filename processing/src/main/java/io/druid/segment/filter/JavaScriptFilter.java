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
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.collections.IterableUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.ExtractionFns;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.data.Indexed;
import org.mozilla.javascript.Context;

import java.util.Arrays;

public class JavaScriptFilter extends Filter.AbstractFilter
{
  private static final Iterable<String> EMPTY_STR_DIM_VAL = Arrays.asList((String) null);

  private final JavaScriptPredicate predicate;
  private final Function<Object[], Object[]> extractor;
  private final String[] dimensions;

  public JavaScriptFilter(String[] dimensions, String script, ExtractionFn[] extractionFns)
  {
    this.dimensions = dimensions;
    this.extractor = ExtractionFns.toTransform(extractionFns);
    this.predicate = new JavaScriptPredicate(script) {
      @Override
      final boolean applyInContext(Context cx, Object[] input)
      {
        return Context.toBoolean(fnApply.call(cx, scope, scope, extractor.apply(input)));
      }
    };
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    // suboptimal, since we need create one context per call to predicate.apply()
    return factory.makeValueMatcher(dimensions, predicate);
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    Iterable<String>[] dimValuesList = new Iterable[dimensions.length];
    for (int idx = 0; idx < dimensions.length; idx++) {
      Indexed<String> dimValues = selector.getDimensionValues(dimensions[idx]);
      if (dimValues == null || dimValues.size() == 0) {
        dimValuesList[idx] = EMPTY_STR_DIM_VAL;
      } else {
        dimValuesList[idx] = dimValues;
      }
    }

    ImmutableBitmap bitmap = selector.getBitmapFactory().makeEmptyImmutableBitmap();

    final Context cx = Context.enter();
    try {
      // for dim X with [a,b,c] + dim Y with [1,2,3], we make cartesian of X and Y, which is in this case
      // {(a,1), (a,2), (a,3), (b,1), (b,2), (b,3), (c,1), (c,2), (c,3)}
      // and apply filter to them and find all possible rows that matched
      // This can be very costly if cardinality of dimension is high. For that case it would be safer
      // to use `byRow=true`, which applies filter by scanning by-row basis.
      for (String[] param : IterableUtils.cartesian(String.class, dimValuesList)) {
        if (predicate.applyInContext(cx, param)) {
          ImmutableBitmap overlap = null;
          for (int idx = 0; idx < dimensions.length; idx++) {
            ImmutableBitmap dimBitMap = selector.getBitmapIndex(dimensions[idx], param[idx]);
            overlap = overlap == null ? dimBitMap : overlap.intersection(dimBitMap);
          }
          bitmap = bitmap.union(overlap);
        }
      }
      return bitmap;
    }
    finally {
      Context.exit();
    }
  }
}
