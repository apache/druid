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

package org.apache.druid.query.topn;

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;

public final class Generic2AggPooledTopNScannerPrototype implements Generic2AggPooledTopNScanner
{
  /**
   * Any changes to this method should be coordinated with {@link TopNUtils}, {@link
   * PooledTopNAlgorithm#computeSpecializedScanAndAggregateImplementations} and downstream methods.
   *
   * It should be checked with a tool like https://github.com/AdoptOpenJDK/jitwatch that C2 compiler output for this
   * method doesn't have any method calls in the while loop, i. e. all method calls are inlined. To be able to see
   * assembly of this method in JITWatch and other similar tools, {@link
   * PooledTopNAlgorithm#SPECIALIZE_GENERIC_TWO_AGG_POOLED_TOPN} should be turned off. Note that in this case the benchmark
   * should be "naturally monomorphic", i. e. execute this method always with the same runtime shape.
   *
   * If the while loop contains not inlined method calls, it should be considered as a performance bug.
   */
  @Override
  public long scanAndAggregate(
      DimensionSelector dimensionSelector,
      BufferAggregator aggregator1,
      int aggregator1Size,
      BufferAggregator aggregator2,
      int aggregator2Size,
      Cursor cursor,
      int[] positions,
      ByteBuffer resultsBuffer
  )
  {
    int totalAggregatorsSize = aggregator1Size + aggregator2Size;
    long processedRows = 0;
    int positionToAllocate = 0;
    while (!cursor.isDoneOrInterrupted()) {
      final IndexedInts dimValues = dimensionSelector.getRow();
      final int dimSize = dimValues.size();
      for (int i = 0; i < dimSize; i++) {
        int dimIndex = dimValues.get(i);
        int position = positions[dimIndex];
        if (position >= 0) {
          aggregator1.aggregate(resultsBuffer, position);
          aggregator2.aggregate(resultsBuffer, position + aggregator1Size);
        } else if (position == TopNAlgorithm.INIT_POSITION_VALUE) {
          positions[dimIndex] = positionToAllocate;
          position = positionToAllocate;
          aggregator1.init(resultsBuffer, position);
          aggregator1.aggregate(resultsBuffer, position);
          position += aggregator1Size;
          aggregator2.init(resultsBuffer, position);
          aggregator2.aggregate(resultsBuffer, position);
          positionToAllocate += totalAggregatorsSize;
        }
      }
      processedRows++;
      cursor.advanceUninterruptibly();
    }
    return processedRows;
  }
}
