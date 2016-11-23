/*
 *
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 *
 */

package io.druid.query.aggregation.datasketches.quantiles;

import com.yahoo.sketches.quantiles.DoublesSketch;
import io.druid.segment.ObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 */
public class QuantilesSketchBufferAggregatorTest
{
  @Test
  public void testAggregation() throws Exception
  {
    final int maxOffheapSize = 1<<12;
    QuantilesSketchBufferAggregator aggregator = new QuantilesSketchBufferAggregator(
        new ObjectColumnSelector()
        {
          private double next = 1.0d;

          @Override
          public Class classOfObject()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object get()
          {
            return next++;
          }
        },
        1024,
        maxOffheapSize
    );

    ByteBuffer bb = ByteBuffer.allocate(maxOffheapSize);

    aggregator.init(bb, 0);
    int n = 100000;
    for (int i = 0; i < n; i++) {
      aggregator.aggregate(bb, 0);
    }

    Assert.assertEquals((byte) 1, bb.get(0));
    DoublesSketch sketch = (DoublesSketch) aggregator.get(bb, 0);
    Assert.assertEquals(n, sketch.getN());
  }
}
