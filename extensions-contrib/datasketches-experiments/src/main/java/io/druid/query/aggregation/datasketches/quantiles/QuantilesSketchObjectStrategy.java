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

import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import io.druid.java.util.common.IAE;
import io.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;

public class QuantilesSketchObjectStrategy implements ObjectStrategy
{

  private static final DoublesSketch EMPTY_SKETCH = QuantilesSketchUtils.buildSketch(QuantilesSketchUtils.MIN_K);
  private static final byte[] EMPTY_BYTES = new byte[]{};

  @Override
  public int compare(Object s1, Object s2)
  {
    if (s1 instanceof DoublesSketch) {
      if (s2 instanceof DoublesSketch) {
        return QuantilesSketchAggregatorFactory.COMPARATOR.compare((DoublesSketch) s1, (DoublesSketch) s2);
      } else {
        return -1;
      }
    }

    //would need to handle Memory here once off-heap is supported.
    throw new IAE("Unknwon class[%s], toString[%s]", s1.getClass(), s1);
  }

  @Override
  public Class<? extends DoublesSketch> getClazz()
  {
    return DoublesSketch.class;
  }

  @Override
  public Object fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    if (numBytes == 0) {
      return EMPTY_SKETCH;
    }

    //would need to handle Memory here once off-heap is supported.
    return DoublesSketch.heapify(
        new MemoryRegion(
            new NativeMemory(buffer),
            buffer.position(),
            numBytes
        )
    );
  }

  @Override
  public byte[] toBytes(Object obj)
  {
    //would need to handle Memory here once off-heap is supported
    if (obj instanceof DoublesSketch) {
      DoublesSketch sketch = (DoublesSketch) obj;
      if (sketch.isEmpty()) {
        return EMPTY_BYTES;
      }
      return sketch.toByteArray();
    } else if (obj == null) {
      return EMPTY_BYTES;
    } else {
      throw new IAE("Unknown class[%s], toString[%s]", obj.getClass(), obj);
    }
  }
}
