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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.mozilla.javascript.Context;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

public class JavaScriptFilter implements Filter
{
  private final String dimension;
  private final JavaScriptDimFilter.JavaScriptPredicateFactory predicateFactory;
  private final FilterTuning filterTuning;

  public JavaScriptFilter(
      String dimension,
      JavaScriptDimFilter.JavaScriptPredicateFactory predicate,
      FilterTuning filterTuning
  )
  {
    this.dimension = dimension;
    this.predicateFactory = predicate;
    this.filterTuning = filterTuning;
  }

  @Override
  public <T> T getBitmapResult(ColumnIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
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
  public double estimateSelectivity(ColumnIndexSelector indexSelector)
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

  @Nullable
  @Override
  public ColumnIndexCapabilities getIndexCapabilities(ColumnIndexSelector selector)
  {
    return Filters.getCapabilitiesWithFilterTuning(selector, dimension, StringValueSetIndex.class, filterTuning);
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, ColumnIndexSelector indexSelector)
  {
    return Filters.supportsSelectivityEstimation(this, dimension, columnSelector, indexSelector);
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JavaScriptFilter that = (JavaScriptFilter) o;
    return Objects.equals(dimension, that.dimension) &&
           Objects.equals(predicateFactory, that.predicateFactory) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, predicateFactory, filterTuning);
  }
}
