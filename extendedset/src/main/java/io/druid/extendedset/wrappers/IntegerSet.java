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

package io.druid.extendedset.wrappers;

import io.druid.extendedset.AbstractExtendedSet;
import io.druid.extendedset.ExtendedSet;
import io.druid.extendedset.intset.IntSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * This class provides a "wrapper" for any  {@link IntSet}  instance in order to be used as an  {@link ExtendedSet}  instance.
 *
 * @author Alessandro Colantonio
 * @version $Id: IntegerSet.java 153 2011-05-30 16:39:57Z cocciasik $
 */
public class IntegerSet extends AbstractExtendedSet<Integer>
{
  /**
   * the collection of <code>int</code> numbers
   *
   * @uml.property name="items"
   * @uml.associationEnd
   */
  private final IntSet items;

  /**
   * Wraps an instance of {@link IntSet}
   *
   * @param items the {@link IntSet} to wrap
   */
  public IntegerSet(IntSet items)
  {
    this.items = items;
  }

  /**
   * @return the internal integer representation
   */
  public IntSet intSet()
  {
    return items;
  }

  /**
   * Converts a generic collection of {@link Integer} instances to a
   * {@link IntSet} instance. If the given collection is an
   * {@link IntegerSet} instance, it returns the contained
   * {@link #items} object.
   *
   * @param c the generic collection of {@link Integer} instances
   *
   * @return the resulting {@link IntSet} instance
   */
  private IntSet toIntSet(Collection<?> c)
  {
    // nothing to convert
    if (c == null) {
      return null;
    }
    if (c instanceof IntegerSet) {
      return ((IntegerSet) c).items;
    }

    // extract integers from the given collection
    IntSet res = items.empty();
    List<Integer> sorted = new ArrayList<Integer>(c.size());
    for (Object i : c) {
      try {
        sorted.add((Integer) i);
      }
      catch (ClassCastException e) {
        // do nothing
      }
    }
    Collections.sort(sorted);
    for (Integer i : sorted) {
      res.add(i.intValue());
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addAll(Collection<? extends Integer> c)
  {
    return items.addAll(toIntSet(c));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double bitmapCompressionRatio()
  {
    return items.bitmapCompressionRatio();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear(Integer from, Integer to)
  {
    items.clear(from.intValue(), to.intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntegerSet clone()
  {
    // NOTE: do not use super.clone() since it is 10 times slower!
    return new IntegerSet(items.clone());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double collectionCompressionRatio()
  {
    return items.collectionCompressionRatio();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(ExtendedSet<Integer> o)
  {
    return items.compareTo(toIntSet(o));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntegerSet complemented()
  {
    return new IntegerSet(items.complemented());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int complementSize()
  {
    return items.complementSize();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAny(Collection<? extends Integer> other)
  {
    return items.containsAny(toIntSet(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAtLeast(Collection<? extends Integer> other, int minElements)
  {
    return items.containsAtLeast(toIntSet(other), minElements);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntegerSet convert(Collection<?> c)
  {
    return new IntegerSet(toIntSet(c));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntegerSet convert(Object... e)
  {
    return convert(Arrays.asList(e));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String debugInfo()
  {
    return getClass().getSimpleName() + "\n" + items.debugInfo();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExtendedIterator<Integer> descendingIterator()
  {
    return new ExtendedIterator<Integer>()
    {
      final IntSet.IntIterator itr = items.descendingIterator();

      @Override
      public void remove() {itr.remove();}

      @Override
      public Integer next() {return Integer.valueOf(itr.next());}

      @Override
      public boolean hasNext() {return itr.hasNext();}

      @Override
      public void skipAllBefore(Integer element) {itr.skipAllBefore(element.intValue());}
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntegerSet difference(Collection<? extends Integer> other)
  {
    return new IntegerSet(items.difference(toIntSet(other)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int differenceSize(Collection<? extends Integer> other)
  {
    return items.differenceSize(toIntSet(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntegerSet empty()
  {
    return new IntegerSet(items.empty());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IntegerSet)) {
      return false;
    }
    return items.equals(((IntegerSet) o).items);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void fill(Integer from, Integer to)
  {
    items.fill(from.intValue(), to.intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Integer first()
  {
    return Integer.valueOf(items.first());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flip(Integer e)
  {
    items.flip(e.intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Integer get(int i)
  {
    return Integer.valueOf(items.get(i));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int indexOf(Integer e)
  {
    return items.indexOf(e.intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntegerSet intersection(Collection<? extends Integer> other)
  {
    return new IntegerSet(items.intersection(toIntSet(other)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int intersectionSize(Collection<? extends Integer> other)
  {
    return items.intersectionSize(toIntSet(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExtendedIterator<Integer> iterator()
  {
    return new ExtendedIterator<Integer>()
    {
      final IntSet.IntIterator itr = items.iterator();

      @Override
      public void remove() {itr.remove();}

      @Override
      public Integer next() {return Integer.valueOf(itr.next());}

      @Override
      public boolean hasNext() {return itr.hasNext();}

      @Override
      public void skipAllBefore(Integer element) {itr.skipAllBefore(element.intValue());}
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Integer last()
  {
    return Integer.valueOf(items.last());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<? extends IntegerSet> powerSet()
  {
    return powerSet(1, Integer.MAX_VALUE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<? extends IntegerSet> powerSet(int min, int max)
  {
    List<? extends IntSet> ps = items.powerSet(min, max);
    List<IntegerSet> res = new ArrayList<IntegerSet>(ps.size());
    for (IntSet s : ps) {
      res.add(new IntegerSet(s));
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean removeAll(Collection<?> c)
  {
    return items.removeAll(toIntSet(c));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean retainAll(Collection<?> c)
  {
    return items.retainAll(toIntSet(c));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntegerSet symmetricDifference(Collection<? extends Integer> other)
  {
    return new IntegerSet(items.symmetricDifference(toIntSet(other)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int symmetricDifferenceSize(Collection<? extends Integer> other)
  {
    return items.symmetricDifferenceSize(toIntSet(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntegerSet union(Collection<? extends Integer> other)
  {
    return new IntegerSet(items.union(toIntSet(other)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int unionSize(Collection<? extends Integer> other)
  {
    return items.unionSize(toIntSet(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    return items.hashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void complement()
  {
    items.complement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Comparator<? super Integer> comparator()
  {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean add(Integer e)
  {
    return items.add(e.intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear()
  {
    items.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean contains(Object o)
  {
    return o instanceof Integer && items.contains(((Integer) o).intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAll(Collection<?> c)
  {
    return items.containsAll(toIntSet(c));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty()
  {
    return items.isEmpty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(Object o)
  {
    return o instanceof Integer && items.remove(((Integer) o).intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size()
  {
    return items.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString()
  {
    // NOTE: by not calling super.toString(), we avoid to iterate over new
    // Integer instances, thus avoiding to waste time and memory with garbage
    // collection
    return items.toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double jaccardSimilarity(ExtendedSet<Integer> other)
  {
    return items.jaccardSimilarity(toIntSet(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double weightedJaccardSimilarity(ExtendedSet<Integer> other)
  {
    return items.weightedJaccardSimilarity(toIntSet(other));
  }
}
