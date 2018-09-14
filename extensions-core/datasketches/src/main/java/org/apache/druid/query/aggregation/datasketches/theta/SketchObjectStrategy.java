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

import com.yahoo.memory.Memory;
import com.yahoo.sketches.theta.Sketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;

public class SketchObjectStrategy implements ObjectStrategy
{

  private static final byte[] EMPTY_BYTES = new byte[]{};

  @Override
  public int compare(Object s1, Object s2)
  {
    return SketchHolder.COMPARATOR.compare(s1, s2);
  }

  @Override
  public Class<?> getClazz()
  {
    return Object.class;
  }

  @Override
  public Object fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    if (numBytes == 0) {
      return SketchHolder.EMPTY;
    }

    return SketchHolder.of(Memory.wrap(buffer).region(buffer.position(), numBytes));
  }

  @Override
  public byte[] toBytes(Object obj)
  {
    if (obj instanceof SketchHolder) {
      Sketch sketch = ((SketchHolder) obj).getSketch();
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
