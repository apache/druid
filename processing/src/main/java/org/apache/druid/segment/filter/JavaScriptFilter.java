/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.filter;

import com.google.common.base.Predicate;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
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
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    final Context cx = Context.enter();
    try {
      return Filters.matchPredicate(dimension, selector, bitmapResultFactory, makeStringPredicate(cx));
    }
    finally {
      Context.exit();
    }
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector indexSelector)
  {
    final Context cx = Context.enter();
    try {
      return Filters.estimateSelectivity(dimension, indexSelector, makeStringPredicate(cx));
    }
    finally {
      Context.exit();
    }
  }

  private Predicate<String> makeStringPredicate(final Context context)
  {
    return new Predicate<String>()
    {
      @Override
      public boolean apply(String input)
      {
        return predicateFactory.applyInContext(context, input);
      }
    };
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    // suboptimal, since we need create one context per call to predicate.apply()
    return Filters.makeValueMatcher(factory, dimension, predicateFactory);
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension) != null;
  }

  @Override
  public boolean supportsSelectivityEstimation(
      ColumnSelector columnSelector, BitmapIndexSelector indexSelector
  )
  {
    return Filters.supportsSelectivityEstimation(this, dimension, columnSelector, indexSelector);
  }
}
