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

package org.apache.druid.query.aggregation;

import java.nio.ByteBuffer;

/**
 * Capability marker for {@link VectorAggregator}s that can efficiently aggregate a contiguous row range while
 * skipping null inputs themselves (such as via SIMD masking) rather than relying on the
 * {@link NullableNumericVectorAggregator} wrapper to filter rows into the per-row scatter-gather
 * {@link VectorAggregator#aggregate(ByteBuffer, int, int[], int[], int) aggregate} variant.
 *
 * When the wrapper sees the input batch has nulls (i.e. {@code selector.getNullVector() != null}) and the
 * delegate is an instance of this interface, it routes the call here instead of falling back to the
 * scatter-gather path.
 */
public interface NullAwareVectorAggregator extends VectorAggregator
{
  /**
   * Aggregate rows {@code [startRow, endRow)} into the slot at {@code position}, skipping rows where
   * {@code nullVector[i] == true}. Implementations should use {@link jdk.incubator.vector.VectorMask} (or
   * equivalent) to avoid per-row branching.
   *
   * @return {@code true} if at least one non-null row was aggregated; {@code false} if every row in the range
   *         was null. {@link NullableNumericVectorAggregator} uses this to set or leave the null-marker byte
   *         that precedes the delegate's state.
   */
  boolean aggregate(ByteBuffer buf, int position, int startRow, int endRow, boolean[] nullVector);
}
