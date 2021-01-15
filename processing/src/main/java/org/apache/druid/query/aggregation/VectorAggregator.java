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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * An object that can aggregate metrics into a ByteBuffer, from vectorized column selectors. Its aggregation-related
 * methods (namely, "aggregate" and "get") do not take the actual input values to aggregate, because it is assumed that
 * the VectorAggregator was given something that it can use to get at the current batch of data.
 *
 * Unlike {@link BufferAggregator}, none of the methods in this class are annotated with
 * {@link org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop} because vectorized query engines do not use
 * monomorphic-processing-style specialization.
 *
 * @see BufferAggregator, the nonvectorized version.
 */
public interface VectorAggregator extends BaseBufferAggregator
{
  /**
   * Aggregate a range of rows into a single aggregation slot.
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the current aggregate value is stored
   * @param startRow first row of the range within the current batch to aggregate (inclusive)
   * @param endRow   end row of the range (exclusive)
   */
  void aggregate(ByteBuffer buf, int position, int startRow, int endRow);

  /**
   * Aggregate a list of rows ("rows") into a list of aggregation slots ("positions").
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * @param buf            byte buffer storing the byte array representation of the aggregate
   * @param numRows        number of rows to aggregate
   * @param positions      array of aggregate value positions within the buffer; must be at least as long as "numRows"
   * @param rows           array of row numbers within the current row batch; must be at least as long as "numRows". If
   *                       null, the aggregator will aggregate rows from 0 (inclusive) to numRows (exclusive).
   * @param positionOffset an offset to apply to each value from "positions"
   */
  void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset);
}
