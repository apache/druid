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

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;

public final class Generic1AggPooledTopNScannerPrototype implements Generic1AggPooledTopNScanner
{
  @Override
  public long scanAndAggregate(
      DimensionSelector dimensionSelector,
      BufferAggregator aggregator,
      int aggregatorSize,
      Cursor cursor,
      int[] positions,
      ByteBuffer resultsBuffer
  )
  {
    long scannedRows = 0;
    int positionToAllocate = 0;
    while (!cursor.isDoneOrInterrupted()) {
      final IndexedInts dimValues = dimensionSelector.getRow();
      final int dimSize = dimValues.size();
      for (int i = 0; i < dimSize; i++) {
        int dimIndex = dimValues.get(i);
        int position = positions[dimIndex];
        if (position >= 0) {
          aggregator.aggregate(resultsBuffer, position);
        } else if (position == TopNAlgorithm.INIT_POSITION_VALUE) {
          positions[dimIndex] = positionToAllocate;
          position = positionToAllocate;
          aggregator.init(resultsBuffer, position);
          aggregator.aggregate(resultsBuffer, position);
          positionToAllocate += aggregatorSize;
        }
      }
      scannedRows++;
      cursor.advanceUninterruptibly();
    }
    return scannedRows;
  }
}
