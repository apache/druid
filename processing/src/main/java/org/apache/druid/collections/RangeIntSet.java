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

package org.apache.druid.collections;

import it.unimi.dsi.fastutil.ints.AbstractIntSortedSet;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.ints.IntSortedSets;

import java.util.NoSuchElementException;

/**
 * Set from start (inclusive) to end (exclusive).
 */
public class RangeIntSet extends AbstractIntSortedSet
{
  private final int start;
  private final int end;

  public RangeIntSet(int start, int end)
  {
    this.start = start;
    this.end = end;
  }

  @Override
  public IntBidirectionalIterator iterator()
  {
    return IntIterators.fromTo(start, end);
  }

  @Override
  public IntBidirectionalIterator iterator(int fromElement)
  {
    if (fromElement < end) {
      return IntIterators.fromTo(Math.max(start, fromElement), end);
    } else {
      return IntIterators.EMPTY_ITERATOR;
    }
  }

  @Override
  public boolean contains(int k)
  {
    return k >= start && k < end;
  }

  @Override
  public IntSortedSet subSet(int fromElement, int toElement)
  {
    if (fromElement < end && toElement > start) {
      return new RangeIntSet(Math.max(fromElement, start), Math.min(toElement, end));
    } else {
      return IntSortedSets.EMPTY_SET;
    }
  }

  @Override
  public IntSortedSet headSet(int toElement)
  {
    if (toElement > start) {
      return new RangeIntSet(start, Math.min(toElement, end));
    } else {
      return IntSortedSets.EMPTY_SET;
    }
  }

  @Override
  public IntSortedSet tailSet(int fromElement)
  {
    if (fromElement < end) {
      return new RangeIntSet(Math.max(start, fromElement), end);
    } else {
      return IntSortedSets.EMPTY_SET;
    }
  }

  @Override
  public IntComparator comparator()
  {
    // Natural ordering.
    return null;
  }

  @Override
  public int firstInt()
  {
    if (start < end) {
      return start;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public int lastInt()
  {
    if (start < end) {
      return end - 1;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public int size()
  {
    return end > start ? end - start : 0;
  }

  @Override
  public boolean isEmpty()
  {
    return end <= start;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }

    if (o instanceof RangeIntSet) {
      final RangeIntSet other = (RangeIntSet) o;
      return (other.start == start && other.end == end) || (other.isEmpty() && isEmpty());
    } else {
      return super.equals(o);
    }
  }

  @Override
  public int hashCode()
  {
    return isEmpty() ? 0 : start + 31 * end;
  }
}
