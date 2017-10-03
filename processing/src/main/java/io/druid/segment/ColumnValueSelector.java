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

import javax.annotation.Nullable;

/**
 * Base type for interfaces that manage column value selection, e.g. DimensionSelector, LongColumnSelector
 *
 * This interface has methods to get the value in all primitive types, that have corresponding basic aggregators in
 * Druid: Sum, Min, Max, etc: {@link #getFloat()}, {@link #getDouble()} and {@link #getLong()} to support "polymorphic"
 * rollup aggregation during index merging. Additionally, it also has a {@link #isNull()} method which can be used to
 * check whether the column has null value or not.
 */
public interface ColumnValueSelector<T>
{
  /**
   * @return float column value, 0.0F if the value is null.
   */
  float getFloat();

  /**
   * @return double column value, 0.0D if the value is null.
   */
  double getDouble();

  /**
   * @return long column value, 0L if the value is null.
   */
  long getLong();

  /**
   * checks if the column value is null.
   *
   * @return true if the column value for is null
   */
  boolean isNull();

  @Nullable
  T getObject();

  Class<T> classOfObject();
}
