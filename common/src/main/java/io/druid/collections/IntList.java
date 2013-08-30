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

package io.druid.collections;

import java.nio.IntBuffer;
import java.util.ArrayList;

/**
 */
public class IntList
{
  private final ArrayList<int[]> baseLists = new ArrayList<int[]>();

  private final int allocateSize;

  private int maxIndex;

  public IntList()
  {
    this(1000);
  }

  public IntList(final int allocateSize)
  {
    this.allocateSize = allocateSize;

    maxIndex = -1;
  }

  public int length()
  {
    return maxIndex + 1;
  }

  public boolean isEmpty()
  {
    return (length() == 0);
  }

  public void add(int value)
  {
    set(length(), value);
  }

  public void set(int index, int value)
  {
    int subListIndex = index / allocateSize;

    if (subListIndex >= baseLists.size()) {
      for (int i = baseLists.size(); i <= subListIndex; ++i) {
        baseLists.add(null);
      }
    }

    int[] baseList = baseLists.get(subListIndex);

    if (baseList == null) {
      baseList = new int[allocateSize];
      baseLists.set(subListIndex, baseList);
    }

    baseList[index % allocateSize] = value;

    if (index > maxIndex) {
      maxIndex = index;
    }
  }

  public int get(int index)
  {
    if (index > maxIndex) {
      throw new ArrayIndexOutOfBoundsException(index);
    }

    int subListIndex = index / allocateSize;
    int[] baseList = baseLists.get(subListIndex);

    if (baseList == null) {
      return 0;
    }

    return baseList[index % allocateSize];
  }

  public int baseListCount()
  {
    return baseLists.size();
  }

  public IntBuffer getBaseList(int index)
  {
    final int[] array = baseLists.get(index);
    if (array == null) {
      return null;
    }

    final IntBuffer retVal = IntBuffer.wrap(array);

    if (index + 1 == baseListCount()) {
      retVal.limit(maxIndex - (index * allocateSize));
    }

    return retVal.asReadOnlyBuffer();
  }

  public int[] toArray()
  {
    int[] retVal = new int[length()];
    int currIndex = 0;
    for (int[] arr : baseLists) {
      int min = Math.min(length() - currIndex, arr.length);
      System.arraycopy(arr, 0, retVal, currIndex, min);
      currIndex += min;
    }
    return retVal;
  }
}
