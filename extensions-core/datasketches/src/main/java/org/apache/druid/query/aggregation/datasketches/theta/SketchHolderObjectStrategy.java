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

package org.apache.druid.query.aggregation.datasketches.theta;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SketchHolderObjectStrategy implements ObjectStrategy<SketchHolder>
{

  @Override
  public int compare(SketchHolder s1, SketchHolder s2)
  {
    return SketchHolder.COMPARATOR.compare(s1, s2);
  }

  @Override
  public Class<SketchHolder> getClazz()
  {
    return SketchHolder.class;
  }

  @Override
  public SketchHolder fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    if (numBytes == 0) {
      return SketchHolder.EMPTY;
    }

    return SketchHolder.of(Memory.wrap(buffer, ByteOrder.LITTLE_ENDIAN).region(buffer.position(), numBytes));
  }

  @Override
  public byte[] toBytes(@Nullable SketchHolder obj)
  {
    if (obj != null) {
      Sketch sketch = obj.getSketch();
      if (sketch.isEmpty()) {
        return ByteArrays.EMPTY_ARRAY;
      }
      return sketch.toByteArray();
    } else {
      return ByteArrays.EMPTY_ARRAY;
    }
  }
}
