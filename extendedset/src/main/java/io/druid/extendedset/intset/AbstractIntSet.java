/* 
 * (c) 2010 Alessandro Colantonio
 * <mailto:colanton@mat.uniroma3.it>
 * <http://ricerca.mat.uniroma3.it/users/colanton>
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.extendedset.intset;


import java.util.Collection;
import java.util.NoSuchElementException;

/**
 * This class provides a skeletal implementation of the {@link IntSet}
 * interface to minimize the effort required to implement this interface.
 *
 * @author Alessandro Colantonio
 * @version $Id: AbstractIntSet.java 156 2011-09-01 00:13:57Z cocciasik $
 */
public abstract class AbstractIntSet implements IntSet
{
  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet union(IntSet other)
  {
    IntSet res = clone();
    res.addAll(other);
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet difference(IntSet other)
  {
    IntSet res = clone();
    res.removeAll(other);
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet intersection(IntSet other)
  {
    IntSet res = clone();
    res.retainAll(other);
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void complement()
  {
    if (isEmpty()) {
      return;
    }
    for (int e = last(); e >= 0; e--) {
      flip(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract IntSet empty();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract IntSet clone();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract double bitmapCompressionRatio();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract double collectionCompressionRatio();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract IntIterator iterator();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract IntIterator descendingIterator();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract String debugInfo();

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear()
  {
    IntIterator itr = iterator();
    while (itr.hasNext()) {
      itr.next();
      itr.remove();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear(int from, int to)
  {
    if (from > to) {
      throw new IndexOutOfBoundsException("from: " + from + " > to: " + to);
    }
    for (int e = from; e <= to; e++) {
      remove(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void fill(int from, int to)
  {
    if (from > to) {
      throw new IndexOutOfBoundsException("from: " + from + " > to: " + to);
    }
    for (int e = from; e <= to; e++) {
      add(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flip(int e)
  {
    if (!add(e)) {
      remove(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract int get(int i);

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract int indexOf(int e);

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract IntSet convert(int... a);

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract IntSet convert(Collection<Integer> c);

  /**
   * {@inheritDoc}
   */
  @Override
  public int first()
  {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    return iterator().next();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract int last();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract int size();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract boolean isEmpty();

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract boolean contains(int i);

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract boolean add(int i);

  /**
   * {@inheritDoc}
   */
  @Override
  public abstract boolean remove(int i);

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addAll(IntSet c)
  {
    if (c == null || c.isEmpty()) {
      return false;
    }
    IntIterator itr = c.iterator();
    boolean res = false;
    while (itr.hasNext()) {
      res |= add(itr.next());
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean removeAll(IntSet c)
  {
    if (c == null || c.isEmpty()) {
      return false;
    }
    IntIterator itr = c.iterator();
    boolean res = false;
    while (itr.hasNext()) {
      res |= remove(itr.next());
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean retainAll(IntSet c)
  {
    if (c == null || c.isEmpty()) {
      return false;
    }
    IntIterator itr = iterator();
    boolean res = false;
    while (itr.hasNext()) {
      int e = itr.next();
      if (!c.contains(e)) {
        res = true;
        itr.remove();
      }
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int[] toArray()
  {
    if (isEmpty()) {
      return null;
    }
    return toArray(new int[size()]);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int[] toArray(int[] a)
  {
    if (a.length < size()) {
      a = new int[size()];
    }
    IntIterator itr = iterator();
    int i = 0;
    while (itr.hasNext()) {
      a[i++] = itr.next();
    }
    for (; i < a.length; i++) {
      a[i] = 0;
    }
    return a;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString()
  {
    IntIterator itr = iterator();
    if (!itr.hasNext()) {
      return "[]";
    }

    StringBuilder sb = new StringBuilder();
    sb.append('[');
    while (true) {
      int e = itr.next();
      sb.append(e);
      if (!itr.hasNext()) {
        return sb.append(']').toString();
      }
      sb.append(", ");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(IntSet o)
  {
    IntIterator thisIterator = this.descendingIterator();
    IntIterator otherIterator = o.descendingIterator();
    while (thisIterator.hasNext() && otherIterator.hasNext()) {
      int thisItem = thisIterator.next();
      int otherItem = otherIterator.next();
      if (thisItem < otherItem) {
        return -1;
      }
      if (thisItem > otherItem) {
        return 1;
      }
    }
    return thisIterator.hasNext() ? 1 : (otherIterator.hasNext() ? -1 : 0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object obj)
  {
    // special cases
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof IntSet)) {
      return false;
    }
    if (size() != ((IntSet) obj).size()) {
      return false;
    }

    // compare all the integrals, according to their natural order
    IntIterator itr1 = iterator();
    IntIterator itr2 = ((IntSet) obj).iterator();
    while (itr1.hasNext()) {
      if (itr1.next() != itr2.next()) {
        return false;
      }
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    if (isEmpty()) {
      return 0;
    }
    int h = 1;
    IntIterator itr = iterator();
    if (!itr.hasNext()) {
      h = (h << 5) - h + itr.next();
    }
    return h;
  }
}
