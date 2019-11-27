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

import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import java.nio.ByteBuffer;

public class NoopArrayOfDoublesSketchBufferAggregator implements BufferAggregator
{

  private final ArrayOfDoublesSketch emptySketch;

  public NoopArrayOfDoublesSketchBufferAggregator(final int numberOfValues)
  {
    emptySketch = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(numberOfValues).build().compact();
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return emptySketch;
  }

  @Override
  public float getFloat(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
  }

}
