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
public class IntersectingOffset implements Offset {
  private final Offset lhs;
  private final Offset rhs;

  public IntersectingOffset(
      Offset lhs,
      Offset rhs
  )
  {
    this.lhs = lhs;
    this.rhs = rhs;

    findIntersection();
  }

  @Override
  public int getOffset() {
    return lhs.getOffset();
  }

  @Override
  public void increment() {
    lhs.increment();
    rhs.increment();

    findIntersection();
  }

  private void findIntersection()
  {
    if (! (lhs.withinBounds() && rhs.withinBounds())) {
      return;
    }

    int lhsOffset = lhs.getOffset();
    int rhsOffset = rhs.getOffset();

    while (lhsOffset != rhsOffset) {
      while (lhsOffset < rhsOffset) {
        lhs.increment();
        if (! lhs.withinBounds()) {
          return;
        }

        lhsOffset = lhs.getOffset();
      }

      while (rhsOffset < lhsOffset) {
        rhs.increment();
        if (! rhs.withinBounds()) {
          return;
        }

        rhsOffset = rhs.getOffset();
      }
    }
  }

  @Override
  public boolean withinBounds() {
    return lhs.withinBounds() && rhs.withinBounds();
  }

  @Override
  public Offset clone()
  {
    final Offset lhsClone = lhs.clone();
    final Offset rhsClone = rhs.clone();
    return new IntersectingOffset(lhsClone, rhsClone);
  }
}
