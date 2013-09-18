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

package io.druid.segment;

import io.druid.segment.data.Offset;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import it.uniroma3.mat.extendedset.intset.IntSet;

/**
 */
public class ConciseOffset implements Offset
{
  private static final int INVALID_VALUE = -1;

  IntSet.IntIterator itr;
  private final ImmutableConciseSet invertedIndex;
  private volatile int val;

  public ConciseOffset(ImmutableConciseSet invertedIndex)
  {
    this.invertedIndex = invertedIndex;
    this.itr = invertedIndex.iterator();
    increment();
  }

  private ConciseOffset(ConciseOffset otherOffset)
  {
    this.invertedIndex = otherOffset.invertedIndex;
    this.itr = otherOffset.itr.clone();
    this.val = otherOffset.val;
  }

  @Override
  public void increment()
  {
    if (itr.hasNext()) {
      val = itr.next();
    } else {
      val = INVALID_VALUE;
    }
  }

  @Override
  public boolean withinBounds()
  {
    return val > INVALID_VALUE;
  }

  @Override
  public Offset clone()
  {
    if (invertedIndex == null || invertedIndex.size() == 0) {
      return new ConciseOffset(new ImmutableConciseSet());
    }

    return new ConciseOffset(this);
  }

  @Override
  public int getOffset()
  {
    return val;
  }
}
