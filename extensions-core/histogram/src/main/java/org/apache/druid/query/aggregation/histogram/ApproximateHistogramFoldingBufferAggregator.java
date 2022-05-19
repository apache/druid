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

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;

public class ApproximateHistogramFoldingBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector<ApproximateHistogram> selector;
  private final ApproximateHistogramFoldingBufferAggregatorHelper innerAggregator;

  public ApproximateHistogramFoldingBufferAggregator(
      BaseObjectColumnValueSelector<ApproximateHistogram> selector,
      int resolution,
      float lowerLimit,
      float upperLimit
  )
  {
    this.selector = selector;
    this.innerAggregator = new ApproximateHistogramFoldingBufferAggregatorHelper(resolution, lowerLimit, upperLimit);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    innerAggregator.init(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    ApproximateHistogram hNext = selector.getObject();
    innerAggregator.aggregate(buf, position, hNext);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return innerAggregator.get(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("ApproximateHistogramFoldingBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("ApproximateHistogramFoldingBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("ApproximateHistogramFoldingBufferAggregator does not support getDouble()");
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
