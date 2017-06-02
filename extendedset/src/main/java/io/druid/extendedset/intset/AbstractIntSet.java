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


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
  public IntSet symmetricDifference(IntSet c)
  {
    IntSet res = clone();
    IntIterator itr = c.iterator();
    while (itr.hasNext()) {
      res.flip(itr.next());
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet complemented()
  {
    IntSet res = clone();
    res.complement();
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
  public boolean containsAll(IntSet c)
  {
    IntIterator itr = c.iterator();
    boolean res = true;
    while (res && itr.hasNext()) {
      res = contains(itr.next());
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAny(IntSet c)
  {
    IntIterator itr = c.iterator();
    while (itr.hasNext()) {
      if (contains(itr.next())) {
        return true;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAtLeast(IntSet c, int minElements)
  {
    IntIterator itr = c.iterator();
    while (minElements > 0 && itr.hasNext()) {
      if (contains(itr.next())) {
        minElements--;
      }
    }
    return minElements == 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int intersectionSize(IntSet c)
  {
    int res = 0;
    IntIterator itr = c.iterator();
    while (itr.hasNext()) {
      if (contains(itr.next())) {
        res++;
      }
    }
    return res;

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int unionSize(IntSet other)
  {
    return other == null ? size() : size() + other.size() - intersectionSize(other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int symmetricDifferenceSize(IntSet other)
  {
    return other == null ? size() : size() + other.size() - 2 * intersectionSize(other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int differenceSize(IntSet other)
  {
    return other == null ? size() : size() - intersectionSize(other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int complementSize()
  {
    if (isEmpty()) {
      return 0;
    }
    return last() - size() + 1;
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
    for (; ; ) {
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
  public List<? extends IntSet> powerSet()
  {
    return powerSet(1, Integer.MAX_VALUE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<? extends IntSet> powerSet(int min, int max)
  {
    if (min < 1 || max < min) {
      throw new IllegalArgumentException();
    }

    // special cases
    List<IntSet> res = new ArrayList<IntSet>();
    if (size() < min) {
      return res;
    }
    if (size() == min) {
      res.add(clone());
      return res;
    }
    if (size() == min + 1) {
      IntIterator itr = descendingIterator();
      while (itr.hasNext()) {
        IntSet set = clone();
        set.remove(itr.next());
        res.add(set);
      }
      if (max > min) {
        res.add(clone());
      }
      return res;
    }

    // the first level contains only one prefix made up of all 1-subsets
    List<List<IntSet>> level = new ArrayList<List<IntSet>>();
    level.add(new ArrayList<IntSet>());
    IntIterator itr = iterator();
    while (itr.hasNext()) {
      IntSet single = empty();
      single.add(itr.next());
      level.get(0).add(single);
    }
    if (min == 1) {
      res.addAll(level.get(0));
    }

    // all combinations
    int lvl = 2;
    while (!level.isEmpty() && lvl <= max) {
      List<List<IntSet>> newLevel = new ArrayList<List<IntSet>>();
      for (List<IntSet> prefix : level) {
        for (int i = 0; i < prefix.size() - 1; i++) {
          List<IntSet> newPrefix = new ArrayList<IntSet>();
          for (int j = i + 1; j < prefix.size(); j++) {
            IntSet x = prefix.get(i).clone();
            x.add(prefix.get(j).last());
            newPrefix.add(x);
            if (lvl >= min) {
              res.add(x);
            }
          }
          if (newPrefix.size() > 1) {
            newLevel.add(newPrefix);
          }
        }
      }
      level = newLevel;
      lvl++;
    }

    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int powerSetSize()
  {
    return isEmpty() ? 0 : (int) Math.pow(2, size()) - 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double jaccardSimilarity(IntSet other)
  {
    if (isEmpty() && other.isEmpty()) {
      return 1D;
    }
    int inters = intersectionSize(other);
    return (double) inters / (size() + other.size() - inters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double jaccardDistance(IntSet other)
  {
    return 1D - jaccardSimilarity(other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double weightedJaccardSimilarity(IntSet other)
  {
    if (isEmpty() && other.isEmpty()) {
      return 1D;
    }
    IntIterator itr = intersection(other).iterator();
    double intersectionSum = 0D;
    while (itr.hasNext()) {
      intersectionSum += itr.next();
    }

    itr = symmetricDifference(other).iterator();
    double symmetricDifferenceSum = 0D;
    while (itr.hasNext()) {
      symmetricDifferenceSum += itr.next();
    }

    return intersectionSum / (intersectionSum + symmetricDifferenceSum);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double weightedJaccardDistance(IntSet other)
  {
    return 1D - weightedJaccardSimilarity(other);
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
