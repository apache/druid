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

package io.druid.collections;

import io.druid.collections.bitmap.MutableBitmap;
import org.roaringbitmap.IntIterator;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;

/**
 *
 */
public class IntegerSet extends AbstractSet<Integer>
{
  private final MutableBitmap mutableBitmap;

  private IntegerSet(MutableBitmap mutableBitmap)
  {
    this.mutableBitmap = mutableBitmap;
  }

  public static IntegerSet wrap(MutableBitmap mutableBitmap)
  {
    return new IntegerSet(mutableBitmap);
  }

  @Override
  public int size()
  {
    return this.mutableBitmap.size();
  }

  @Override
  public boolean isEmpty()
  {
    return this.mutableBitmap.isEmpty();
  }

  @Override
  public boolean contains(Object o)
  {
    if (o instanceof Integer) {
      return mutableBitmap.get((Integer) o);
    } else if (o instanceof Long) {
      return this.contains(((Long) o).intValue());
    }
    return false;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return new BitSetIterator(mutableBitmap);
  }

  @Override
  public Object[] toArray()
  {
    Integer[] retval = new Integer[mutableBitmap.size()];
    int pos = 0;
    for (Integer i : this) {
      retval[pos++] = i;
    }
    return retval;
  }

  @Override
  public boolean add(Integer integer)
  {
    if (null == integer) {
      throw new NullPointerException("BitSet cannot contain null values");
    }
    if (integer < 0) {
      throw new IllegalArgumentException("Only positive integers or zero can be added");
    }
    boolean isSet = mutableBitmap.get(integer);
    mutableBitmap.add(integer.intValue());
    return !isSet;
  }

  @Override
  public boolean remove(Object o)
  {
    if (o == null) {
      throw new NullPointerException("BitSet cannot contain null values");
    }
    if (o instanceof Integer) {
      Integer integer = (Integer) o;
      boolean isSet = mutableBitmap.get(integer);
      mutableBitmap.remove(integer);
      return isSet;
    } else {
      throw new ClassCastException("Cannot remove non Integer from integer BitSet");
    }
  }

  @Override
  public boolean containsAll(Collection<?> c)
  {
    Iterator<?> it = c.iterator();
    while (it.hasNext()) {
      if (!this.contains(it.next())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends Integer> c)
  {
    boolean setChanged = false;
    for (Integer i : c) {
      if (!this.contains(i)) {
        setChanged = true;
        this.add(i);
      }
    }
    return setChanged;
  }

  @Override
  public boolean retainAll(Collection<?> c)
  {
    // Stub
    throw new UnsupportedOperationException("Cannot retainAll ona an IntegerSet");
  }

  @Override
  public boolean removeAll(Collection<?> c)
  {
    Iterator<?> it = c.iterator();
    boolean changed = false;
    while (it.hasNext()) {
      Integer val = (Integer) it.next();
      changed = remove(val) || changed;
    }
    return changed;
  }

  @Override
  public void clear()
  {
    mutableBitmap.clear();
  }

  public static class BitSetIterator implements Iterator<Integer>
  {
    private final IntIterator intIt;
    private final MutableBitmap bitSet;
    private Integer prior = null;

    public BitSetIterator(MutableBitmap bitSet)
    {
      this.intIt = bitSet.iterator();
      this.bitSet = bitSet;
    }

    @Override
    public boolean hasNext()
    {
      return intIt.hasNext();
    }

    @Override
    public Integer next()
    {
      prior = intIt.next();
      return prior;
    }

    @Override
    public void remove()
    {
      bitSet.remove(prior);
    }
  }
}
