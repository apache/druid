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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This filter is to select the rows where the {@link #dimension} has the {@link #value}. The value can be null.
 * In SQL-compatible null handling mode, this filter is equivalent to {@code dimension = value}
 * or {@code dimension IS NULL} when the value is null.
 * In default null handling mode, this filter is equivalent to {@code dimension = value} or
 * {@code dimension = ''} when the value is null.
 */
public class SelectorFilter implements Filter
{
  private final String dimension;
  private final String value;
  @Nullable
  private final FilterTuning filterTuning;

  public SelectorFilter(
      String dimension,
      String value
  )
  {
    this(dimension, value, null);
  }

  public SelectorFilter(
      String dimension,
      String value,
      @Nullable FilterTuning filterTuning
  )
  {
    this.dimension = dimension;
    this.value = value;
    this.filterTuning = filterTuning;
  }

  @Override
  public <T> T getBitmapResult(ColumnIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    final StringValueSetIndex valueSetIndex = selector.as(dimension, StringValueSetIndex.class);
    if (valueSetIndex == null) {
      return NullHandling.isNullOrEquivalent(value)
             ? bitmapResultFactory.wrapAllTrue(Filters.allTrue(selector))
             : bitmapResultFactory.wrapAllFalse(Filters.allFalse(selector));
    }
    return bitmapResultFactory.wrapDimensionValue(
        valueSetIndex.getBitmapForValue(value)
    );
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, dimension, value);
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(final VectorColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeVectorProcessor(
        dimension,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(value);
  }

  @Nullable
  @Override
  public ColumnIndexCapabilities getIndexCapabilities(ColumnIndexSelector selector)
  {
    return Filters.checkFilterTuning(
        selector,
        dimension,
        selector.getIndexCapabilities(dimension, StringValueSetIndex.class),
        filterTuning
    );
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, ColumnIndexSelector indexSelector)
  {
    return Filters.supportsSelectivityEstimation(this, dimension, columnSelector, indexSelector);
  }

  @Override
  public double estimateSelectivity(ColumnIndexSelector selector)
  {
    return (double) selector.as(dimension, StringValueSetIndex.class).getBitmapForValue(value).size()
           / selector.getNumRows();
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    String rewriteDimensionTo = columnRewrites.get(dimension);

    if (rewriteDimensionTo == null) {
      throw new IAE(
          "Received a non-applicable rewrite: %s, filter's dimension: %s",
          columnRewrites,
          dimension
      );
    }

    return new SelectorFilter(
        rewriteDimensionTo,
        value
    );
  }

  @Override
  public String toString()
  {
    return StringUtils.format("%s = %s", dimension, value);
  }

  public String getDimension()
  {
    return dimension;
  }

  public String getValue()
  {
    return value;
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
    SelectorFilter that = (SelectorFilter) o;
    return Objects.equals(getDimension(), that.getDimension()) &&
           Objects.equals(getValue(), that.getValue()) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getDimension(), getValue(), filterTuning);
  }
}
