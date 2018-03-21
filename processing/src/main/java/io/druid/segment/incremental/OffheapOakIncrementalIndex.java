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

package processing.src.main.java.io.druid.segment.incremental;

import com.google.common.base.Supplier;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.incremental.IncrementalIndex;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OffheapOakIncrementalIndex extends
    io.druid.segment.incremental.InternalDataIncrementalIndex<BufferAggregator>
{

  OffheapOakIncrementalIndex(
      io.druid.segment.incremental.IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics, boolean reportParseExceptions,
      boolean concurrentEventAdd)
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions,
        concurrentEventAdd);
  }

  // stub
  @Override
  public Iterable<io.druid.data.input.Row> iterableWithPostAggregations(
      List<io.druid.query.aggregation.PostAggregator> postAggs, boolean descending)
  {
    return null;
  }

  // stub
  @Override
  protected long getMinTimeMillis()
  {
    return 0;
  }

  // stub
  @Override
  protected long getMaxTimeMillis()
  {
    return 0;
  }

  // stub
  @Override
  public int getLastRowIndex()
  {
    return 0;
  }

  // stub
  @Override
  protected BufferAggregator[] getAggsForRow(int rowOffset)
  {
    return null;
  }

  // stub
  @Override
  protected Object getAggVal(BufferAggregator agg, int rowOffset, int aggPosition)
  {
    return null;
  }

  // stub
  @Override
  protected float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return 0;
  }

  // stub
  @Override
  protected long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return 0;
  }

  // stub
  @Override
  protected Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return null;
  }

  // stub
  @Override
  protected double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    return 0;
  }

  // stub
  @Override
  protected boolean isNull(int rowOffset, int aggOffset)
  {
    return false;
  }

  // stub
  @Override
  public Iterable<IncrementalIndex.TimeAndDims> timeRangeIterable(
      boolean descending, long timeStart, long timeEnd)
  {
    return null;
  }

  // stub
  @Override
  public Iterable<IncrementalIndex.TimeAndDims> keySet()
  {
    return null;
  }

  // stub
  @Override
  public boolean canAppendRow()
  {
    return false;
  }

  // stub
  @Override
  public String getOutOfRowsReason()
  {
    return null;
  }

  // Note: This method needs to be thread safe.
  // stub, going to be removed!
  @Override
  protected Integer addToFacts(
      io.druid.query.aggregation.AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      io.druid.data.input.InputRow row,
      AtomicInteger numEntries,
      IncrementalIndex.TimeAndDims key,
      ThreadLocal<io.druid.data.input.InputRow> rowContainer,
      Supplier<io.druid.data.input.InputRow> rowSupplier,
      boolean skipMaxRowsInMemoryCheck
  )
  {
    return 0;
  }

  // stub, going to be removed!
  @Override
  protected BufferAggregator[] initAggs(
      io.druid.query.aggregation.AggregatorFactory[] metrics,
      Supplier<io.druid.data.input.InputRow> rowSupplier,
      boolean deserializeComplexMetrics,
      boolean concurrentEventAdd
  )
  {
    return null;
  }
}
