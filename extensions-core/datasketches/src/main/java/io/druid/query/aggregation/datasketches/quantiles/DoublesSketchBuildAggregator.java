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

package io.druid.query.aggregation.datasketches.quantiles;

import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ColumnValueSelector;

public class DoublesSketchBuildAggregator implements Aggregator
{

  private final ColumnValueSelector<Double> valueSelector;
  private final int size;

  private UpdateDoublesSketch sketch;

  public DoublesSketchBuildAggregator(final ColumnValueSelector<Double> valueSelector, final int size)
  {
    this.valueSelector = valueSelector;
    this.size = size;
    sketch = UpdateDoublesSketch.builder().setK(size).build();
  }

  @Override
  public synchronized void aggregate()
  {
    sketch.update(valueSelector.getDouble());
  }

  @Override
  public synchronized Object get()
  {
    return sketch;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public synchronized void reset()
  {
    sketch = UpdateDoublesSketch.builder().setK(size).build();
  }

  @Override
  public synchronized void close()
  {
    sketch = null;
  }

}
