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

package org.apache.druid.segment.vector;

public class NoFilterVectorOffset implements VectorOffset
{
  private final int maxVectorSize;
  private final int start;
  private final int end;
  private int theOffset;

  public NoFilterVectorOffset(final int maxVectorSize, final int start, final int end)
  {
    this.maxVectorSize = maxVectorSize;
    this.start = start;
    this.end = end;
    reset();
  }

  @Override
  public int getId()
  {
    return theOffset;
  }

  @Override
  public void advance()
  {
    theOffset += maxVectorSize;
  }

  @Override
  public boolean isDone()
  {
    return theOffset >= end;
  }

  @Override
  public boolean isContiguous()
  {
    return true;
  }

  @Override
  public int getMaxVectorSize()
  {
    return maxVectorSize;
  }

  @Override
  public int getCurrentVectorSize()
  {
    return Math.min(maxVectorSize, end - theOffset);
  }

  @Override
  public int getStartOffset()
  {
    return theOffset;
  }

  @Override
  public int[] getOffsets()
  {
    throw new UnsupportedOperationException("no filter");
  }

  @Override
  public void reset()
  {
    theOffset = start;
  }
}
