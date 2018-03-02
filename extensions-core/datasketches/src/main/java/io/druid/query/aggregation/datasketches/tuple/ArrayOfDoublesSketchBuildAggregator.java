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

package io.druid.query.aggregation.datasketches.tuple;

import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.BaseDoubleColumnValueSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.util.List;

public class ArrayOfDoublesSketchBuildAggregator implements Aggregator
{

  private final DimensionSelector keySelector;
  private final BaseDoubleColumnValueSelector[] valueSelectors;
  private final double[] values; // for sketch update call
  private ArrayOfDoublesUpdatableSketch sketch;

  public ArrayOfDoublesSketchBuildAggregator(
      final DimensionSelector keySelector,
      final List<BaseDoubleColumnValueSelector> valueSelectors,
      final int nominalEntries
  )
  {
    this.keySelector = keySelector;
    this.valueSelectors = valueSelectors.toArray(new BaseDoubleColumnValueSelector[valueSelectors.size()]);
    values = new double[valueSelectors.size()];
    sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(nominalEntries)
        .setNumberOfValues(valueSelectors.size()).build();
  }

  @Override
  public synchronized void aggregate()
  {
    final IndexedInts keys = keySelector.getRow();
    for (int i = 0; i < valueSelectors.length; i++) {
      values[i] = valueSelectors[i].getDouble();
    }
    for (int i = 0; i < keys.size(); i++) {
      final String key = keySelector.lookupName(keys.get(i));
      sketch.update(key, values);
    }
  }

  @Override
  public synchronized Object get()
  {
    return sketch.compact();
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    sketch = null;
  }

}
