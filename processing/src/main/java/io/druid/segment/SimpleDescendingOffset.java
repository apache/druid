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

package io.druid.segment;

import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.data.Offset;
import io.druid.segment.data.ReadableOffset;

public class SimpleDescendingOffset extends Offset
{
  private final int rowCount;
  private final int initialOffset;
  private int currentOffset;

  SimpleDescendingOffset(int rowCount)
  {
    this(rowCount - 1, rowCount);
  }

  private SimpleDescendingOffset(int initialOffset, int rowCount)
  {
    this.rowCount = rowCount;
    this.initialOffset = initialOffset;
    this.currentOffset = initialOffset;
  }

  @Override
  public void increment()
  {
    currentOffset--;
  }

  @Override
  public boolean withinBounds()
  {
    return currentOffset >= 0;
  }

  @Override
  public void reset()
  {
    currentOffset = initialOffset;
  }

  @Override
  public ReadableOffset getBaseReadableOffset()
  {
    return this;
  }

  @Override
  public Offset clone()
  {
    return new SimpleDescendingOffset(currentOffset, rowCount);
  }

  @Override
  public int getOffset()
  {
    return currentOffset;
  }

  @Override
  public String toString()
  {
    return currentOffset + "/" + rowCount + "(DSC)";
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }
}
