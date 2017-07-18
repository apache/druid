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
public class UnioningOffset extends Offset
{
  private final Offset[] offsets = new Offset[2];
  private final int[] offsetVals = new int[2];

  private int nextOffsetIndex;

  public UnioningOffset(Offset lhs, Offset rhs)
  {
    if (lhs.withinBounds()) {
      offsets[0] = lhs;
    }

    if (rhs.withinBounds()) {
      if (offsets[0] == null) {
        offsets[0] = rhs;
      } else {
        offsets[1] = rhs;
      }
    }

    if (offsets[0] != null) {
      offsetVals[0] = offsets[0].getOffset();
      if (offsets[1] != null) {
        offsetVals[1] = offsets[1].getOffset();
      }
    }
    figureOutNextValue();
  }

  private UnioningOffset(
      Offset[] offsets,
      int[] offsetVals,
      int nextOffsetIndex
  )
  {
    System.arraycopy(offsets, 0, this.offsets, 0, 2);
    System.arraycopy(offsetVals, 0, this.offsetVals, 0, 2);
    this.nextOffsetIndex = nextOffsetIndex;
  }

  private void figureOutNextValue()
  {
    if (offsets[0] != null) {
      if (offsets[1] != null) {
        int lhs = offsetVals[0];
        int rhs = offsetVals[1];

        if (lhs < rhs) {
          nextOffsetIndex = 0;
        } else if (lhs == rhs) {
          nextOffsetIndex = 0;
          rollIndexForward(1);
        } else {
          nextOffsetIndex = 1;
        }
      } else {
        nextOffsetIndex = 0;
      }
    }
  }

  private void rollIndexForward(int i)
  {
    offsets[i].increment();

    if (!offsets[i].withinBounds()) {
      offsets[i] = null;
      if (i == 0) {
        offsets[0] = offsets[1];
        offsetVals[0] = offsetVals[1];
      }
    } else {
      offsetVals[i] = offsets[i].getOffset();
    }
  }

  @Override
  public int getOffset()
  {
    return offsetVals[nextOffsetIndex];
  }

  @Override
  public void increment()
  {
    rollIndexForward(nextOffsetIndex);
    figureOutNextValue();
  }

  @Override
  public boolean withinBounds()
  {
    return offsets[0] != null;
  }

  @Override
  public Offset clone()
  {
    Offset[] newOffsets = new Offset[2];
    int[] newOffsetValues = new int[2];

    for(int i = 0; i < newOffsets.length; ++i) {
      newOffsets[i] = offsets[i] == null ? null : offsets[i].clone();
      newOffsetValues[i] = this.offsetVals[i];
    }

    return new UnioningOffset(newOffsets, newOffsetValues, nextOffsetIndex);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("lhs", offsets[0]);
    inspector.visit("rhs", offsets[1]);
  }
}
