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

import com.metamx.common.guava.MergeIterable;
import com.metamx.common.guava.nary.BinaryFn;

import java.util.Comparator;
import java.util.Iterator;

/**
 */
public class CombiningIterable<InType> implements Iterable<InType>
{
  /**
   * Creates a CombiningIterable around a MergeIterable such that equivalent elements are thrown away
   *
   * If there are multiple Iterables in parameter "in" with equivalent objects, there are no guarantees
   * around which object will win.  You will get *some* object from one of the Iterables, but which Iterable is
   * unknown.
   *
   * @param in An Iterable of Iterables to be merged
   * @param comparator the Comparator to determine sort and equality
   * @param <InType> Type of object
   * @return An Iterable that is the merge of all Iterables from in such that there is only one instance of
   * equivalent objects.
   */
  @SuppressWarnings("unchecked")
  public static <InType> CombiningIterable<InType> createSplatted(
      Iterable<? extends Iterable<InType>> in,
      Comparator<InType> comparator
  )
  {
    return create(
        new MergeIterable<InType>(comparator, (Iterable<Iterable<InType>>) in),
        comparator,
        new BinaryFn<InType, InType, InType>()
        {
          @Override
          public InType apply(InType arg1, InType arg2)
          {
            if (arg1 == null) {
              return arg2;
            }
            return arg1;
          }
        }
    );
  }

  public static <InType> CombiningIterable<InType> create(
    Iterable<InType> it,
    Comparator<InType> comparator,
    BinaryFn<InType, InType, InType> fn
  )
  {
    return new CombiningIterable<InType>(it, comparator, fn);
  }

  private final Iterable<InType> it;
  private final Comparator<InType> comparator;
  private final BinaryFn<InType, InType, InType> fn;

  public CombiningIterable(
      Iterable<InType> it,
      Comparator<InType> comparator,
      BinaryFn<InType, InType, InType> fn
  )
  {
    this.it = it;
    this.comparator = comparator;
    this.fn = fn;
  }

  @Override
  public Iterator<InType> iterator()
  {
    return CombiningIterator.create(it.iterator(), comparator, fn);
  }
}
