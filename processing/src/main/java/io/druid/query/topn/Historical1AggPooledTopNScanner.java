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
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.historical.HistoricalCursor;
import io.druid.segment.historical.HistoricalDimensionSelector;

import java.nio.ByteBuffer;

public interface Historical1AggPooledTopNScanner<
    DimensionSelectorType extends HistoricalDimensionSelector,
    MetricSelectorType extends ColumnValueSelector,
    BufferAggregatorType extends BufferAggregator>
{
  /**
   * @param aggregatorSize number of bytes required by aggregator for a single aggregation
   * @param positions a cache for positions in resultsBuffer, where specific (indexed) dimension values are aggregated
   * @return number of scanned rows, i. e. number of steps made with the given cursor
   */
  long scanAndAggregate(
      DimensionSelectorType dimensionSelector,
      MetricSelectorType metricSelector,
      BufferAggregatorType aggregator,
      int aggregatorSize,
      HistoricalCursor cursor,
      int[] positions,
      ByteBuffer resultsBuffer
  );
}
