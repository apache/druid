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

package org.apache.druid.query.aggregation.longunique;

import com.google.common.base.Throwables;
import org.apache.druid.segment.data.ObjectStrategy;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;


public class Roaring64NavigableMapObjectStrategy implements ObjectStrategy<Roaring64NavigableMap>
{
  private static final byte[] EMPTY_BYTES = new byte[]{};

  static Roaring64NavigableMapObjectStrategy STRATEGY = new Roaring64NavigableMapObjectStrategy();

  @Override
  public Class<? extends Roaring64NavigableMap> getClazz()
  {
    return Roaring64NavigableMap.class;
  }

  @Override
  public Roaring64NavigableMap fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    if (numBytes > 0) {
      buffer.limit(buffer.position() + numBytes);
      byte[] b = new byte[buffer.remaining()];
      buffer.get(b, 0, b.length);
      return LongUniqueAggregatorFactory.getRoaring64NavigableMap(b);
    }
    return new Roaring64NavigableMap();
  }

  @Override
  public byte[] toBytes(Roaring64NavigableMap val)
  {
    if (val == null || val.isEmpty()) {
      return EMPTY_BYTES;
    } else {
      try {
        val.runOptimize();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        val.serialize(new DataOutputStream(out));
        return out.toByteArray();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public int compare(Roaring64NavigableMap o1, Roaring64NavigableMap o2)
  {
    return LongUniqueAggregatorFactory.COMPARATOR.compare(o1, o2);
  }
}
