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

import org.apache.druid.query.aggregation.SimpleDoubleBufferAggregator;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.historical.HistoricalColumnSelector;
import org.apache.druid.segment.historical.HistoricalCursor;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;

import java.nio.ByteBuffer;

public class HistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopNScannerPrototype
    implements Historical1AggPooledTopNScanner<
        SingleValueHistoricalDimensionSelector,
        HistoricalColumnSelector,
        SimpleDoubleBufferAggregator
    >
{
  /**
   * Any changes to this method should be coordinated with {@link TopNUtils}, {@link
   * PooledTopNAlgorithm#computeSpecializedScanAndAggregateImplementations} and downstream methods.
   *
   * It should be checked with a tool like https://github.com/AdoptOpenJDK/jitwatch that C2 compiler output for this
   * method doesn't have any method calls in the while loop, i. e. all method calls are inlined. To be able to see
   * assembly of this method in JITWatch and other similar tools, {@link
   * PooledTopNAlgorithm#SPECIALIZE_HISTORICAL_SINGLE_VALUE_DIM_SELECTOR_ONE_SIMPLE_DOUBLE_AGG_POOLED_TOPN} should be turned off.
   * Note that in this case the benchmark should be "naturally monomorphic", i. e. execute this method always with the
   * same runtime shape.
   *
   * If the while loop contains not inlined method calls, it should be considered as a performance bug.
   */
  @Override
  public long scanAndAggregate(
      SingleValueHistoricalDimensionSelector dimensionSelector,
      HistoricalColumnSelector metricSelector,
      SimpleDoubleBufferAggregator aggregator,
      int aggregatorSize,
      HistoricalCursor cursor,
      int[] positions,
      ByteBuffer resultsBuffer
  )
  {
    // See TopNUtils.copyOffset() for explanation
    Offset offset = (Offset) TopNUtils.copyOffset(cursor);
    long processedRows = 0;
    int positionToAllocate = 0;
    while (offset.withinBounds() && !Thread.currentThread().isInterrupted()) {
      int rowNum = offset.getOffset();
      int dimIndex = dimensionSelector.getRowValue(rowNum);
      int position = positions[dimIndex];
      if (position >= 0) {
        aggregator.aggregate(resultsBuffer, position, metricSelector.getDouble(rowNum));
      } else if (position == TopNAlgorithm.INIT_POSITION_VALUE) {
        positions[dimIndex] = positionToAllocate;
        aggregator.putFirst(resultsBuffer, positionToAllocate, metricSelector.getDouble(rowNum));
        positionToAllocate += aggregatorSize;
      }
      processedRows++;
      offset.increment();
    }
    return processedRows;
  }
}
