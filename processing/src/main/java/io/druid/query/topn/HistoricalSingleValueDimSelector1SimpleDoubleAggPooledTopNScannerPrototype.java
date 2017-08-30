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

package io.druid.query.topn;

import io.druid.query.aggregation.SimpleDoubleBufferAggregator;
import io.druid.segment.data.Offset;
import io.druid.segment.historical.HistoricalCursor;
import io.druid.segment.historical.HistoricalFloatColumnSelector;
import io.druid.segment.historical.SingleValueHistoricalDimensionSelector;

import java.nio.ByteBuffer;

public class HistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopNScannerPrototype
    implements Historical1AggPooledTopNScanner<
        SingleValueHistoricalDimensionSelector,
        HistoricalFloatColumnSelector,
        SimpleDoubleBufferAggregator
    >
{
  @Override
  public long scanAndAggregate(
      SingleValueHistoricalDimensionSelector dimensionSelector,
      HistoricalFloatColumnSelector metricSelector,
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
        aggregator.aggregate(resultsBuffer, position, metricSelector.get(rowNum));
      } else if (position == TopNAlgorithm.INIT_POSITION_VALUE) {
        positions[dimIndex] = positionToAllocate;
        aggregator.putFirst(resultsBuffer, positionToAllocate, metricSelector.get(rowNum));
        positionToAllocate += aggregatorSize;
      }
      processedRows++;
      offset.increment();
    }
    return processedRows;
  }
}
