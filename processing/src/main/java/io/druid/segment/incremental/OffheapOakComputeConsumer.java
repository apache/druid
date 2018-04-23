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
import oak.WritableOakBuffer;

import java.util.function.Consumer;

/**
 * For sending as input parameter to the Oak's putifAbsentComputeIfPresent method
 */
public class OffheapOakComputeConsumer implements Consumer<WritableOakBuffer>
{
  AggregatorFactory[] metrics;
  boolean reportParseExceptions;
  InputRow row;
  ThreadLocal<InputRow> rowContainer;
  int[] aggOffsetInBuffer;
  BufferAggregator[] aggs;
  boolean executed; // for figuring out whether a put or a compute was executed

  public OffheapOakComputeConsumer(
      AggregatorFactory[] metrics,
      boolean reportParseExceptions,
      InputRow row,
      ThreadLocal<InputRow> rowContainer,
      int[] aggOffsetInBuffer,
      BufferAggregator[] aggs
  )
  {
    this.metrics = metrics;
    this.reportParseExceptions = reportParseExceptions;
    this.row = row;
    this.rowContainer = rowContainer;
    this.aggOffsetInBuffer = aggOffsetInBuffer;
    this.aggs = aggs;
    this.executed = false;
  }

  @Override
  public void accept(WritableOakBuffer writableOakBuffer)
  {
    OffheapOakIncrementalIndex.aggregate(metrics, reportParseExceptions, row, rowContainer,
            writableOakBuffer.getByteBuffer(), aggOffsetInBuffer, aggs);
    executed = true;

  }

  public boolean executed()
  {
    return executed;
  }
}
