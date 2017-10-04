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

package io.druid.segment;

import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;

/**
 * Factory class for MetricSelectors
 */
public interface ColumnSelectorFactory
{
  DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec);
  FloatColumnSelector makeFloatColumnSelector(String columnName);
  LongColumnSelector makeLongColumnSelector(String columnName);
  DoubleColumnSelector makeDoubleColumnSelector(String columnName);

  @Nullable
  ObjectColumnSelector makeObjectColumnSelector(String columnName);

  /**
   * Returns capabilities of a particular column, if known. May be null if the column doesn't exist, or if
   * the column does exist but the capabilities are unknown. The latter is possible with dynamically discovered
   * columns.
   *
   * @param column column name
   *
   * @return capabilities, or null
   */
  @Nullable
  ColumnCapabilities getColumnCapabilities(String column);
}
