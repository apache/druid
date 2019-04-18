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

package org.apache.druid.query.aggregation.bloom;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseNullableColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * All bloom filter aggregations are done using {@link ByteBuffer}, so this base class implements both {@link Aggregator}
 * and {@link BufferAggregator}.
 *
 * If used as an {@link Aggregator} the caller MUST specify the 'onHeap' parameter in the
 * constructor as "true", or else the "collector" will not be allocated and null pointer exceptions will make things sad.
 *
 * If used as a {@link BufferAggregator}, the "collector" buffer is not necessary, and should be called with "false",
 * but at least nothing dramatic will happen like incorrect use in the {@link Aggregator} case.
 *
 * {@link BloomFilterAggregatorFactory} and {@link BloomFilterMergeAggregatorFactory}, which should be the creators of
 * all implementations of {@link BaseBloomFilterAggregator} outside of tests, should be sure to set the 'onHeap' value
 * to "true" and "false" respectively for
 * {@link org.apache.druid.query.aggregation.AggregatorFactory#factorize} and
 * {@link org.apache.druid.query.aggregation.AggregatorFactory#factorizeBuffered}
 *
 * @param <TSelector> type of {@link BaseNullableColumnValueSelector} that feeds this aggregator, likely either values
 *                  to add to a bloom filter, or other bloom filters to merge into this bloom filter.
 */
public abstract class BaseBloomFilterAggregator<TSelector extends BaseNullableColumnValueSelector>
    implements BufferAggregator, Aggregator
{
  @Nullable
  private final ByteBuffer collector;
  protected final int maxNumEntries;
  protected final TSelector selector;

  /**
   * @param selector selector that feeds values to the aggregator
   * @param maxNumEntries maximum number of entries that can be added to a bloom filter before accuracy degrades rapidly
   * @param onHeap allocate a ByteBuffer "collector" to use as an {@link Aggregator}
   */
  BaseBloomFilterAggregator(TSelector selector, int maxNumEntries, boolean onHeap)
  {
    this.selector = selector;
    this.maxNumEntries = maxNumEntries;
    if (onHeap) {
      BloomKFilter bloomFilter = new BloomKFilter(maxNumEntries);
      this.collector = ByteBuffer.allocate(BloomKFilter.computeSizeBytes(maxNumEntries));
      BloomKFilter.serialize(collector, bloomFilter);
    } else {
      collector = null;
    }
  }

  abstract void bufferAdd(ByteBuffer buf);

  @Override
  public void init(ByteBuffer buf, int position)
  {
    final ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    BloomKFilter filter = new BloomKFilter(maxNumEntries);
    BloomKFilter.serialize(mutationBuffer, filter);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final int oldPosition = buf.position();
    try {
      buf.position(position);
      bufferAdd(buf);
    }
    finally {
      buf.position(oldPosition);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    int sizeBytes = BloomKFilter.computeSizeBytes(maxNumEntries);
    mutationBuffer.limit(position + sizeBytes);

    ByteBuffer resultCopy = ByteBuffer.allocate(sizeBytes);
    resultCopy.put(mutationBuffer.slice());
    resultCopy.rewind();
    return resultCopy;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("BloomFilterAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("BloomFilterAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("BloomFilterAggregator does not support getDouble()");
  }

  @Override
  public void aggregate()
  {
    aggregate(collector, 0);
  }

  @Nullable
  @Override
  public Object get()
  {
    return collector;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("BloomFilterAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("BloomFilterAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("BloomFilterAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // nothing to close
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
