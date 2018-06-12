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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.RangeSet;
import io.druid.java.util.common.Cacheable;

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
    @JsonSubTypes.Type(name = "noop", value = NoopDimFilter.class)
})
public interface DimFilter extends Cacheable
{
  /**
   * @return Returns an optimized filter.
   * returning the same filter can be a straightforward default implementation.
   */
  DimFilter optimize();

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
  RangeSet<String> getDimensionRangeSet(String dimension);
}
