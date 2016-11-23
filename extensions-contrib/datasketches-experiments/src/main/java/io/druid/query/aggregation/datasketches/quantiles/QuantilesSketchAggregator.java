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

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesUnion;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

public class QuantilesSketchAggregator implements Aggregator
{
  private final ObjectColumnSelector selector;
  private final String name;

  private DoublesUnion quantilesSketchUnion;

  public QuantilesSketchAggregator(String name, ObjectColumnSelector selector, int size)
  {
    this.name = name;
    this.selector = selector;
    this.quantilesSketchUnion = QuantilesSketchUtils.buildUnion(size);
  }

  @Override
  public void aggregate()
  {
    updateQuantilesSketch(quantilesSketchUnion, selector.get());
  }

  @Override
  public void reset()
  {
    quantilesSketchUnion.reset();
  }

  @Override
  public Object get()
  {
    return quantilesSketchUnion.getResult();
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
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {
    quantilesSketchUnion = null;
  }

  static void updateQuantilesSketch(DoublesUnion quantilesSketchUnion, Object update)
  {
    if (update == null) {
      return;
    }

    //would need to handle Memory when off-heap is supported.
    if (update instanceof DoublesSketch) {
      if (!((DoublesSketch) update).isEmpty()) {
        quantilesSketchUnion.update((DoublesSketch) update);
      }
    } else if (update instanceof Number) {
      quantilesSketchUnion.update(((Number) update).doubleValue());
    } else if (update instanceof String) {
      quantilesSketchUnion.update(Double.parseDouble((String) update));
    } else {
      throw new ISE("Illegal type received while quantiles sketch merging [%s]", update.getClass());
    }
  }
}
