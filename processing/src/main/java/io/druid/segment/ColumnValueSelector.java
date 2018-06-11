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

import io.druid.guice.annotations.PublicApi;

/**
 * Base type for interfaces that manage column value selection, e.g. {@link DimensionSelector}, {@link
 * LongColumnSelector}.
 *
 * This interface has methods to get the value in all primitive types, that have corresponding basic aggregators in
 * Druid: Sum, Min, Max, etc: {@link #getFloat()}, {@link #getDouble()} and {@link #getLong()} to support "polymorphic"
 * rollup aggregation during index merging.
 *
 * "Absent" column, i. e. that always returns zero from {@link #getLong()}, {@link #getFloat()} and {@link #getDouble()}
 * methods and null from {@link #getObject()}, should always be an instance of {@link NilColumnValueSelector}.
 * `selector instanceof NilColumnValueSelector` is the recommended way to check for this condition.
 */
@PublicApi
public interface ColumnValueSelector<T> extends BaseLongColumnValueSelector, BaseDoubleColumnValueSelector,
    BaseFloatColumnValueSelector, BaseObjectColumnValueSelector<T>
{
  ColumnValueSelector[] EMPTY_ARRAY = new ColumnValueSelector[0];
}
