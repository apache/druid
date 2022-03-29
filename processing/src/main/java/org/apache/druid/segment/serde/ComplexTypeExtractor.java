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

package org.apache.druid.segment.serde;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.query.aggregation.AggregatorFactory;

import javax.annotation.Nullable;

public interface ComplexTypeExtractor<T>
{
  Class<? extends T> extractedClass();

  /**
   * 'Extract' a complex value type from an {@link InputRow} for the specified metric name, for complex 'metric' types.
   * This method is only called when by the default implementation of
   * {@link #extractValue(InputRow, String, AggregatorFactory)}, and may be implemented by any complex 'metric' whose
   * initialization is not dependent on the {@link AggregatorFactory}, and is used to allow coercion of values
   * into the correct complex type expected by the complex aggregator when making column selectors for queries on
   * {@link org.apache.druid.segment.incremental.IncrementalIndex}.
   */
  @Nullable
  T extractValue(InputRow inputRow, String metricName);

  /**
   * 'Extract' a complex value type from an {@link InputRow} for the specified metric name, for complex 'metric' types
   * which must be initialized based on some information available in their {@link AggregatorFactory}. This method
   * is called when creating column selectors for {@link org.apache.druid.segment.incremental.IncrementalIndex} on top
   * of a base {@link org.apache.druid.segment.RowBasedColumnSelectorFactory}, to allow coercion of values into the
   * correct complex type expected by the complex aggregator.
   */
  @Nullable
  default T extractValue(InputRow inputRow, String metricName, AggregatorFactory agg)
  {
    return extractValue(inputRow, metricName);
  }

  /**
   * 'Coerce' a complex value type for use when creating column selectors for queries on a
   * {@link org.apache.druid.segment.RowBasedColumnSelectorFactory}. By default, this method is a direct value
   * passthrough.
   *
   * This method is used for selectors for all complex types from a
   * {@link org.apache.druid.segment.RowBasedColumnSelectorFactory}, but for 'realtime' queries for complex 'metrics'
   * (on {@link org.apache.druid.segment.incremental.IncrementalIndex}), this method will be not be used in favor of a
   * customized selector which uses {@link #extractValue(InputRow, String, AggregatorFactory)} instead.
   *
   * Realtime queries on complex types which are not metrics (e.g. dimensions) will still use this method even for
   * realtime queries.
   */
  @Nullable
  default T coerceValue(@Nullable Object value)
  {
    return (T) value;
  }
}
