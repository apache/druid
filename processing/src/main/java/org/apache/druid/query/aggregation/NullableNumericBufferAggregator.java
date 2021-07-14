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

import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.segment.BaseNullableColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;

/**
 * Null-aware numeric {@link BufferAggregator} which can handle null valued inputs.
 *
 * The result of this aggregator will be null if all the values to be aggregated are null values or no values are
 * aggregated at all. If any of the values are non-null, the result will be the aggregated value of the delegate
 * aggregator. This class is only used when SQL compatible null handling is enabled.
 *
 * @see NullableNumericAggregatorFactory#factorizeBuffered(ColumnSelectorFactory)
 * @see NullableNumericAggregator for the non-vectorized heap version.
 * @see NullableNumericVectorAggregator the vectorized version.
 */
@PublicApi
public final class NullableNumericBufferAggregator extends NullAwareNumericBufferAggregator
{
  public NullableNumericBufferAggregator(BufferAggregator delegate, BaseNullableColumnValueSelector nullSelector)
  {
    super(delegate, nullSelector);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    boolean isNotNull = !nullSelector.isNull();
    if (isNotNull) {
      doAggregate(buf, position);
    }
  }
}
