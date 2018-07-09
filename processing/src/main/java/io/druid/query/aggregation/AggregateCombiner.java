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

package io.druid.query.aggregation;

import io.druid.guice.annotations.ExtensionPoint;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnValueSelector;

/**
 * AggregateCombiner is used to fold rollup aggregation results from serveral "rows" of different indexes during index
 * merging (see {@link io.druid.segment.IndexMerger}).
 *
 * The state of the implementations of this interface is an aggregation value (either a primitive or an object), that
 * could be queried via {@link ColumnValueSelector}'s methods. Before {@link #reset} is ever called on an
 * AggregateCombiner, it's state is undefined and {@link ColumnValueSelector}'s methods could return something random,
 * or null, or throw an exception.
 *
 * This interface would probably better be called "AggregateFolder", but somebody may confuse it with "folder" as
 * "directory" synonym.
 *
 * @see AggregatorFactory#makeAggregateCombiner()
 * @see LongAggregateCombiner
 * @see DoubleAggregateCombiner
 * @see ObjectAggregateCombiner
 */
@ExtensionPoint
public interface AggregateCombiner<T> extends ColumnValueSelector<T>
{
  /**
   * Resets this AggregateCombiner's state value to the value of the given selector, e. g. after calling this method
   * combiner.get*() should return the same value as selector.get*().
   *
   * If the selector is an {@link io.druid.segment.ObjectColumnSelector}, the object returned from {@link
   * io.druid.segment.ObjectColumnSelector#getObject()} must not be modified, and must not become a subject for
   * modification during subsequent {@link #fold} calls.
   */
  @SuppressWarnings("unused") // Going to be used when https://github.com/druid-io/druid/projects/2 is complete
  void reset(ColumnValueSelector selector);

  /**
   * Folds this AggregateCombiner's state value with the value of the given selector and saves it in this
   * AggregateCombiner's state, e. g. after calling combiner.fold(selector), combiner.get*() should return the value
   * that would be the result of {@link AggregatorFactory#combine
   * aggregatorFactory.combine(combiner.get*(), selector.get*())} call.
   *
   * Unlike {@link AggregatorFactory#combine}, if the selector is an {@link io.druid.segment.ObjectColumnSelector}, the
   * object returned from {@link io.druid.segment.ObjectColumnSelector#getObject()} must not be modified, and must not
   * become a subject for modification during subsequent fold() calls.
   *
   * Since the state of AggregateCombiner is undefined before {@link #reset} is ever called on it, the effects of
   * calling fold() are also undefined in this case.
   *
   * @see AggregatorFactory#combine
   */
  @SuppressWarnings("unused") // Going to be used when https://github.com/druid-io/druid/projects/2 is complete
  void fold(ColumnValueSelector selector);

  @Override
  default boolean isNull()
  {
    return false;
  }

  @Override
  default void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // Usually AggregateCombiner has nothing to inspect
  }
}
