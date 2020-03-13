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

package org.apache.druid.segment;

import org.apache.druid.query.dimension.ColumnSelectorStrategyFactory;
import org.apache.druid.segment.column.ValueType;

/**
 * Class that encapsulates knowledge about how to create "column processors", which are... objects that process columns
 * and want to have type-specific logic. Used by {@link ColumnProcessors#makeProcessor}.
 *
 * Column processors can be any type "T". The idea is that a ColumnProcessorFactory embodies the logic for wrapping
 * and processing selectors of various types, and so enables nice code design, where type-dependent code is not
 * sprinkled throughout.
 *
 * @see VectorColumnProcessorFactory the vectorized version
 * @see ColumnProcessors#makeProcessor which uses these, and which is responsible for
 * determining which type of selector to use for a given column
 * @see ColumnSelectorStrategyFactory which serves a similar purpose and may be replaced by this in the future
 * @see DimensionHandlerUtils#createColumnSelectorPluses which accepts {@link ColumnSelectorStrategyFactory} and is
 * similar to {@link ColumnProcessors#makeProcessor}
 */
public interface ColumnProcessorFactory<T>
{
  /**
   * This default type will be used when the underlying column has an unknown type.
   *
   * This allows a column processor factory to specify what type it prefers to deal with (the most 'natural' type for
   * whatever it is doing) when all else is equal.
   */
  ValueType defaultType();

  /**
   * Create a processor for a string column.
   *
   * @param selector   dimension selector
   * @param multiValue whether the selector *might* have multiple values
   */
  T makeDimensionProcessor(DimensionSelector selector, boolean multiValue);

  /**
   * Create a processor for a float column.
   *
   * @param selector float selector
   */
  T makeFloatProcessor(BaseFloatColumnValueSelector selector);

  /**
   * Create a processor for a double column.
   *
   * @param selector double selector
   */
  T makeDoubleProcessor(BaseDoubleColumnValueSelector selector);

  /**
   * Create a processor for a long column.
   *
   * @param selector long selector
   */
  T makeLongProcessor(BaseLongColumnValueSelector selector);

  /**
   * Create a processor for a complex column.
   *
   * @param selector object selector
   */
  T makeComplexProcessor(BaseObjectColumnValueSelector<?> selector);
}
