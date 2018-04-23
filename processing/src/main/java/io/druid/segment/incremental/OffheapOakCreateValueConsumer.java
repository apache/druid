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

package io.druid.segment.incremental;

import io.druid.data.input.InputRow;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.util.Map;
import java.util.function.Consumer;
import java.nio.ByteBuffer;

public class OffheapOakCreateValueConsumer implements Consumer<ByteBuffer>
{

  AggregatorFactory[] metrics;
  boolean reportParseExceptions;
  InputRow row;
  ThreadLocal<InputRow> rowContainer;
  BufferAggregator[] aggs;
  Map<String, ColumnSelectorFactory> selectors;
  int[] aggOffsetInBuffer;

  public OffheapOakCreateValueConsumer(
          AggregatorFactory[] metrics,
          boolean reportParseExceptions,
          InputRow row,
          ThreadLocal<InputRow> rowContainer,
          BufferAggregator[] aggs,
          Map<String, ColumnSelectorFactory> selectors,
          int[] aggOffsetInBuffer
  )
  {
    this.metrics = metrics;
    this.reportParseExceptions = reportParseExceptions;
    this.row = row;
    this.rowContainer = rowContainer;
    this.aggs = aggs;
    this.selectors = selectors;
    this.aggOffsetInBuffer = aggOffsetInBuffer;
  }

  @Override
  public void accept(ByteBuffer byteBuffer)
  {
    if (metrics.length > 0 && aggs[0] == null) {
      // note: creation of Aggregators is done lazily when at least one row from input is available
      // so that FilteredAggregators could be initialized correctly.
      rowContainer.set(row);
      for (int i = 0; i < metrics.length; i++) {
        final AggregatorFactory agg = metrics[i];
        aggs[i] = agg.factorizeBuffered(selectors.get(agg.getName()));
      }
      rowContainer.set(null);
    }

    for (int i = 0; i < metrics.length; i++) {
      aggs[i].init(byteBuffer, aggOffsetInBuffer[i]);
    }

    OffheapOakIncrementalIndex.aggregate(metrics, reportParseExceptions, row, rowContainer, byteBuffer, aggOffsetInBuffer, aggs);
  }
}
