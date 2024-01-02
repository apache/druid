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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.index.BitmapColumnIndex;

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

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(dimension, selector, filterTuning)) {
      return null;
    }
    return Filters.makePredicateIndex(dimension, selector, predicateFactory);
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    // suboptimal, since we need create one context per call to predicate.apply()
    return Filters.makeValueMatcher(factory, dimension, predicateFactory);
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
