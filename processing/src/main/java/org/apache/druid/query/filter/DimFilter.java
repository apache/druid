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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.RangeSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.query.extraction.ExtractionFn;

import javax.annotation.Nullable;
import java.util.Set;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "and", value = AndDimFilter.class),
    @JsonSubTypes.Type(name = "or", value = OrDimFilter.class),
    @JsonSubTypes.Type(name = "not", value = NotDimFilter.class),
    @JsonSubTypes.Type(name = "selector", value = SelectorDimFilter.class),
    @JsonSubTypes.Type(name = "columnComparison", value = ColumnComparisonDimFilter.class),
    @JsonSubTypes.Type(name = "extraction", value = ExtractionDimFilter.class),
    @JsonSubTypes.Type(name = "regex", value = RegexDimFilter.class),
    @JsonSubTypes.Type(name = "search", value = SearchQueryDimFilter.class),
    @JsonSubTypes.Type(name = "javascript", value = JavaScriptDimFilter.class),
    @JsonSubTypes.Type(name = "spatial", value = SpatialDimFilter.class),
    @JsonSubTypes.Type(name = "in", value = InDimFilter.class),
    @JsonSubTypes.Type(name = "bound", value = BoundDimFilter.class),
    @JsonSubTypes.Type(name = "interval", value = IntervalDimFilter.class),
    @JsonSubTypes.Type(name = "like", value = LikeDimFilter.class),
    @JsonSubTypes.Type(name = "expression", value = ExpressionDimFilter.class),
    @JsonSubTypes.Type(name = "true", value = TrueDimFilter.class),
    @JsonSubTypes.Type(name = "false", value = FalseDimFilter.class),
    @JsonSubTypes.Type(name = "null", value = NullFilter.class),
    @JsonSubTypes.Type(name = "equals", value = EqualityFilter.class),
    @JsonSubTypes.Type(name = "range", value = RangeFilter.class),
    @JsonSubTypes.Type(name = "isfalse", value = IsFalseDimFilter.class),
    @JsonSubTypes.Type(name = "istrue", value = IsTrueDimFilter.class),
    @JsonSubTypes.Type(name = "arrayContainsElement", value = ArrayContainsElementFilter.class)
})
public interface DimFilter extends Cacheable
{
  /**
   * Returns an optimized version of this filter.
   *
   * @param mayIncludeUnknown whether the optimized filter may need to operate in "includeUnknown" mode.
   *                          See {@link NullHandling#useThreeValueLogic()}.
   */
  DimFilter optimize(boolean mayIncludeUnknown);

  /**
   * @return Return a Filter that implements this DimFilter, after applying optimizations to this DimFilter.
   * A typical implementation will return the result of `optimize().toFilter()`
   * See abstract base class {@link AbstractOptimizableDimFilter} for a common implementation shared by
   * current DimFilters.
   *
   * The Filter returned by this method across multiple calls must be the same object: parts of the query stack
   * compare Filters, and returning the same object allows these checks to avoid deep comparisons.
   * (see {@link org.apache.druid.segment.join.HashJoinSegmentStorageAdapter#makeCursors for an example}
   *
   * @param mayIncludeUnknown whether the optimized filter may need to operate in "includeUnknown" mode.
   *                          See {@link NullHandling#useThreeValueLogic()}.
   */
  Filter toOptimizedFilter(boolean mayIncludeUnknown);

  /**
   * Returns a Filter that implements this DimFilter. This does not generally involve optimizing the DimFilter,
   * so it does make sense to optimize first and then call toFilter on the resulting DimFilter.
   *
   * @return a Filter that implements this DimFilter, or null if this DimFilter is a no-op.
   */
  Filter toFilter();

  /**
   * Returns a RangeSet that represents the possible range of the input dimension for this DimFilter.This is
   * applicable to filters that use dimensions such as select, in, bound, and logical filters such as and, or, not.
   *
   * Null represents that the range cannot be determined, and will be returned for filters such as javascript and regex
   * where there's no easy way to determine the filtered range. It is treated the same way as an all range in most
   * cases, however there are some subtle difference at logical filters such as not filter, where complement of all
   * is nothing while complement of null is still null.
   *
   * @param dimension name of the dimension to get range for
   * @return a RangeSet that represent the possible range of the input dimension, or null if it is not possible to
   * determine for this DimFilter.
   */
  @Nullable
  RangeSet<String> getDimensionRangeSet(String dimension);

  /**
   * @return a HashSet that represents all columns' name which the DimFilter required to do filter.
   */
  Set<String> getRequiredColumns();

  /**
   * Wrapper for {@link StringBuilder} to re-use common patterns in custom {@link DimFilter#toString()} implementations
   */
  class DimFilterToStringBuilder
  {
    private final StringBuilder builder;

    public DimFilterToStringBuilder()
    {
      this.builder = new StringBuilder();
    }

    /**
     * Append dimension name OR {@link ExtractionFn#toString()} with dimension wrapped in parenthesis
     */
    DimFilterToStringBuilder appendDimension(String dimension, @Nullable ExtractionFn extractionFn)
    {
      if (extractionFn != null) {
        builder.append(extractionFn).append("(");
      }

      builder.append(dimension);

      if (extractionFn != null) {
        builder.append(")");
      }
      return this;
    }

    /**
     * Add "=" expression
     */
    DimFilterToStringBuilder appendEquals(String value)
    {
      builder.append(" = ").append(value);
      return this;
    }

    /**
     * Add filter tuning to {@link #builder} if tuning exists
     */
    DimFilterToStringBuilder appendFilterTuning(@Nullable FilterTuning tuning)
    {
      if (tuning != null) {
        builder.append(" (filterTuning=").append(tuning).append(")");
      }

      return this;
    }

    /**
     * Generic passthrough to {@link StringBuilder#append}
     */
    <T> DimFilterToStringBuilder append(T s)
    {
      builder.append(s);
      return this;
    }

    public String build()
    {
      return builder.toString();
    }
  }
}
