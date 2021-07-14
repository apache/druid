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

import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * A wrapper around a non-null-aware {@link VectorAggregator} whose input is not nullable, but which
 * should be null valued if no values are aggregated at all.
 *
 * The result of this aggregator will be null only if no values are aggregated at all, otherwise the result
 * would be the aggregated value of the delegate aggregator. This class is only used when SQL compatible null
 * handling is enabled.
 *
 * @see NullableNumericAggregatorFactory#factorizeVector(VectorColumnSelectorFactory)
 * @see NonnullNumericAggregator for the non-vectorized heap version.
 * @see NonnullNumericBufferAggregator for the non-vectorized version.
 */
public final class NonnullNumericVectorAggregator extends NullAwareNumericVectorAggregator
{
  NonnullNumericVectorAggregator(
      VectorAggregator delegate,
      VectorValueSelector selector
  )
  {
    super(delegate, selector);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    doAggregate(buf, position, startRow, endRow);
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    doAggregate(buf, numRows, positions, rows, positionOffset);
  }
}
