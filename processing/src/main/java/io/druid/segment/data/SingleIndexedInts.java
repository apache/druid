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

import com.google.common.collect.Iterators;

import java.util.Iterator;

/**
*/
public class SingleIndexedInts implements IndexedInts
{
  private final int value;

  public SingleIndexedInts(int value) {
    this.value = value;
  }

  @Override
  public int size()
  {
    return 1;
  }

  @Override
  public int get(int index)
  {
    return value;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return Iterators.singletonIterator(value);
  }
}
