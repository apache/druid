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

package org.apache.druid.query.aggregation.datasketches.hll;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HllSketchObjectStrategy implements ObjectStrategy<HllSketch>
{

  static final HllSketchObjectStrategy STRATEGY = new HllSketchObjectStrategy();

  @Override
  public Class<HllSketch> getClazz()
  {
    return HllSketch.class;
  }

  @Override
  public int compare(final HllSketch sketch1, final HllSketch sketch2)
  {
    return HllSketchAggregatorFactory.COMPARATOR.compare(sketch1, sketch2);
  }

  @Override
  public HllSketch fromByteBuffer(final ByteBuffer buf, final int size)
  {
    return HllSketch.wrap(Memory.wrap(buf, ByteOrder.LITTLE_ENDIAN).region(buf.position(), size));
  }

  @Override
  public byte[] toBytes(final HllSketch sketch)
  {
    return sketch.toCompactByteArray();
  }

}
