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
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.index.AllTrueBitmapColumnIndex;
import org.apache.druid.segment.index.AllUnknownBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
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
  @Nullable
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(dimension, selector, filterTuning)) {
      return null;
    }
    final boolean isNull = NullHandling.isNullOrEquivalent(value);
    final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(dimension);
    if (indexSupplier == null) {
      if (isNull) {
        return new AllTrueBitmapColumnIndex(selector);
      }
      return new AllUnknownBitmapColumnIndex(selector);
    }
    if (isNull) {
      final NullValueIndex nullValueIndex = indexSupplier.as(NullValueIndex.class);
      if (nullValueIndex == null) {
        return null;
      }
      return nullValueIndex.get();
    } else {
      final StringValueSetIndexes valueSetIndexes = indexSupplier.as(StringValueSetIndexes.class);
      if (valueSetIndexes == null) {
        // column exists, but has no index
        return null;
      }
      return valueSetIndexes.forValue(value);
    }
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeProcessor(
        dimension,
        new StringConstantValueMatcherFactory(value),
        factory
    );
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
