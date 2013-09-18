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

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.metamx.common.guava.nary.BinaryFn;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 */
public class CombiningIterator<InType> implements Iterator<InType>
{
  public static <InType> CombiningIterator<InType> create(
      Iterator<InType> it,
      Comparator<InType> comparator,
      BinaryFn<InType, InType, InType> fn
  )
  {
    return new CombiningIterator<InType>(it, comparator, fn);
  }

  private final PeekingIterator<InType> it;
  private final Comparator<InType> comparator;
  private final BinaryFn<InType, InType, InType> fn;

  public CombiningIterator(
      Iterator<InType> it,
      Comparator<InType> comparator,
      BinaryFn<InType, InType, InType> fn
  )
  {
    this.it = Iterators.peekingIterator(it);
    this.comparator = comparator;
    this.fn = fn;
  }

  @Override
  public boolean hasNext()
  {
    return it.hasNext();
  }

  @Override
  public InType next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    InType res = null;

    while (hasNext()) {
      if (res == null) {
        res = fn.apply(it.next(), null);
        continue;
      }

      if (comparator.compare(res, it.peek()) == 0) {
        res = fn.apply(res, it.next());
      } else {
        break;
      }
    }

    return res;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
