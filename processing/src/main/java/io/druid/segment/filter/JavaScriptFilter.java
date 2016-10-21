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
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import org.mozilla.javascript.Context;

public class JavaScriptFilter implements Filter
{
  private final String dimension;
  private final JavaScriptDimFilter.JavaScriptPredicateFactory predicateFactory;

  public JavaScriptFilter(
      String dimension,
      JavaScriptDimFilter.JavaScriptPredicateFactory predicate
  )
  {
    this.dimension = dimension;
    this.predicateFactory = predicate;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    final Context cx = Context.enter();
    try {
      final Predicate<String> contextualPredicate = new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return predicateFactory.applyInContext(cx, input);
        }
      };

      return Filters.matchPredicate(dimension, selector, contextualPredicate);
    }
    finally {
      Context.exit();
    }
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    // suboptimal, since we need create one context per call to predicate.apply()
    return factory.makeValueMatcher(dimension, predicateFactory);
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension) != null;
  }
}
