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

package io.druid.segment.data;

import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;

/**
 */
public class ArrayBasedOffset extends Offset
{
  private final int[] ints;
  private int currIndex;

  public ArrayBasedOffset(
      int[] ints
  )
  {
    this(ints, 0);
  }

  public ArrayBasedOffset(
      int[] ints,
      int startIndex
  )
  {
    this.ints = ints;
    this.currIndex = startIndex;
  }

  @Override
  public int getOffset()
  {
    return ints[currIndex];
  }

  @Override
  public void increment()
  {
    ++currIndex;
  }

  @Override
  public boolean withinBounds()
  {
    return currIndex < ints.length;
  }

  @Override
  public Offset clone()
  {
    final ArrayBasedOffset retVal = new ArrayBasedOffset(ints);
    retVal.currIndex = currIndex;
    return retVal;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }
}
