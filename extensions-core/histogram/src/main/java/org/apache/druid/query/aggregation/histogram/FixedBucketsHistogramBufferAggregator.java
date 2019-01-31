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

package org.apache.druid.query.aggregation.histogram;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;

public class FixedBucketsHistogramBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector selector;

  private FixedBucketsHistogram histogram;

  public FixedBucketsHistogramBufferAggregator(
      BaseObjectColumnValueSelector selector,
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode
  )
  {
    this.selector = selector;
    this.histogram = new FixedBucketsHistogram(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode
    );
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.put(histogram.toBytesFull(false));
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    FixedBucketsHistogram h0 = FixedBucketsHistogram.fromByteBufferFullNoSerdeHeader(mutationBuffer);

    Object val = selector.getObject();
    if (val == null) {
      if (NullHandling.replaceWithDefault()) {
        h0.incrementMissing();
      } else {
        h0.add(NullHandling.defaultDoubleValue());
      }
    } else if (val instanceof String) {
      h0.combineHistogram(FixedBucketsHistogram.fromBase64((String) val));
    } else if (val instanceof FixedBucketsHistogram) {
      h0.combineHistogram((FixedBucketsHistogram) val);
    } else {
      Double x = ((Number) val).doubleValue();
      h0.add(x);
    }

    mutationBuffer.position(position);
    mutationBuffer.put(h0.toBytesFull(false));
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    return FixedBucketsHistogram.fromByteBufferFullNoSerdeHeader(mutationBuffer);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("FixedBucketsHistogramBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("FixedBucketsHistogramBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("FixedBucketsHistogramBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
