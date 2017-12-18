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

import javax.annotation.Nullable;

/**
 * Object value selecting polymorphic "part" of the {@link ColumnValueSelector} interface. Users of {@link
 * ColumnValueSelector#getObject()} are encouraged to reduce the parameter/field/etc. type to
 * BaseObjectColumnValueSelector to make it impossible to accidently call any method other than {@link #getObject()}.
 *
 * All implementations of this interface MUST also implement {@link ColumnValueSelector}.
 */
@PublicApi
public interface BaseObjectColumnValueSelector<T>
{
  /**
   * Returns the column value at the current position.
   *
   * IMPORTANT. The returned object could generally be reused inside the implementation of
   * BaseObjectColumnValueSelector, i. e. this method could always return the same object for the same selector. Users
   * of this API, such as {@link io.druid.query.aggregation.Aggregator#aggregate()}, {@link
   * io.druid.query.aggregation.BufferAggregator#aggregate}, {@link io.druid.query.aggregation.AggregateCombiner#reset},
   * {@link io.druid.query.aggregation.AggregateCombiner#fold} should be prepared for that and not storing the object
   * returned from this method in their state, assuming that the object will remain unchanged even when the position of
   * the selector changes. This may not be the case.
   */
  @Nullable
  T getObject();

  Class<T> classOfObject();
}
