/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

/**
 */
public class UnioningOffset implements Offset
{
  private final Offset[] offsets = new Offset[2];
  private final int[] offsetVals = new int[2];

  private int nextOffsetIndex;

  public UnioningOffset(
      Offset lhs,
      Offset rhs
  )
  {
    if (lhs.withinBounds()) {
      offsets[0] = lhs;
    }

    if (rhs.withinBounds()) {
      if (offsets[0] == null) {
        offsets[0] = rhs;
      }
      else {
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

  private void figureOutNextValue() {
    if (offsets[0] != null) {
      if (offsets[1] != null) {
        int lhs = offsetVals[0];
        int rhs = offsetVals[1];

        if (lhs < rhs) {
          nextOffsetIndex = 0;
        } else if (lhs == rhs) {
          nextOffsetIndex = 0;
          rollIndexForward(1);
        }
        else {
          nextOffsetIndex = 1;
        }
      }
      else {
        nextOffsetIndex = 0;
      }
    }
  }

  private void rollIndexForward(int i) {
    offsets[i].increment();

    if (! offsets[i].withinBounds()) {
      offsets[i] = null;
      if (i == 0) {
        offsets[0] = offsets[1];
        offsetVals[0] = offsetVals[1];
      }
    }
    else {
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
}
