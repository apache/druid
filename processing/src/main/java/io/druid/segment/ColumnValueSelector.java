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
 * rollup aggregation during index merging.
 */
public interface ColumnValueSelector<T>
{
  float getFloat();

  double getDouble();

  long getLong();

  @Nullable
  T getObject();

  Class<T> classOfObject();
}
