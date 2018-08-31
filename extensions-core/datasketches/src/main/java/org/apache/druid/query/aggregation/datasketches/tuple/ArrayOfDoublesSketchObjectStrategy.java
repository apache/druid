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

package org.apache.druid.query.aggregation.datasketches.tuple;

import org.apache.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

public class ArrayOfDoublesSketchObjectStrategy implements ObjectStrategy<ArrayOfDoublesSketch>
{

  static final ArrayOfDoublesSketchObjectStrategy STRATEGY = new ArrayOfDoublesSketchObjectStrategy();

  @Override
  public int compare(final ArrayOfDoublesSketch s1, final ArrayOfDoublesSketch s2)
  {
    return ArrayOfDoublesSketchAggregatorFactory.COMPARATOR.compare(s1, s2);
  }

  @Override
  public Class<? extends ArrayOfDoublesSketch> getClazz()
  {
    return ArrayOfDoublesSketch.class;
  }

  @Override
  public ArrayOfDoublesSketch fromByteBuffer(final ByteBuffer buffer, final int numBytes)
  {
    return ArrayOfDoublesSketches.wrapSketch(Memory.wrap(buffer).region(buffer.position(), numBytes));
  }

  @Override
  @Nullable
  public byte[] toBytes(@Nullable final ArrayOfDoublesSketch sketch)
  {
    if (sketch == null) {
      return null;
    }
    return sketch.toByteArray();
  }

}
