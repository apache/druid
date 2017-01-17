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

import io.druid.extendedset.ExtendedSet;
import io.druid.extendedset.intset.ConciseSetUtils;
import io.druid.extendedset.intset.IntSet;
import io.druid.extendedset.intset.IntSet.IntIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * Very similar to  {@link ExtendedSet}  but for the primitive <code>long</code> type.
 *
 * @author Alessandro Colantonio
 * @version $Id: LongSet.java 154 2011-05-30 22:19:24Z cocciasik $
 */
public class LongSet implements Cloneable, Comparable<LongSet>, java.io.Serializable, Iterable<Long>
{
  /**
   * generated ID
   */
  private static final long serialVersionUID = -6165350530254304256L;

  /**
   * maximum cardinality of each subset
   */
  private static int SUBSET_SIZE = ConciseSetUtils.MAX_ALLOWED_INTEGER + 1;

  /**
   * transaction-item pair indices (from 0 to   {@link #SUBSET_SIZE}   - 1)
   *
   * @uml.property name="firstIndices"
   * @uml.associationEnd
   */
  private final IntSet firstIndices;

  /**
   * transaction-item pair indices (from {@link #SUBSET_SIZE})
   */
  private final NavigableMap<Long, IntSet> otherIndices;

  /**
   * Creates an empty set
   *
   * @param block {@link IntSet} instance internally used to represent
   *              {@link Long} values. It can be non-empty.
   */
  public LongSet(IntSet block)
  {
    firstIndices = block.empty();
    otherIndices = new TreeMap<Long, IntSet>();
  }

  /**
   * Shallow-copy constructor
   */
  private LongSet(IntSet firstIndices, NavigableMap<Long, IntSet> otherIndices)
  {
    this.firstIndices = firstIndices;
    this.otherIndices = otherIndices;
  }

  /**
   * @return an empty {@link IntSet} instance of the same type of that of
   * internally used to represent integers
   */
  public IntSet emptyBlock()
  {
    return firstIndices.empty();
  }

  /**
   * Retains only the elements in this set that are contained in the specified
   * collection. In other words, removes from this set all of its elements
   * that are not contained in the specified collection.
   *
   * @param other collection containing elements to be retained in this set
   *
   * @return <tt>true</tt> if this set changed as a result of the call
   *
   * @throws NullPointerException if this set contains a null element and the specified
   *                              collection does not permit null elements (optional), or if
   *                              the specified collection is null
   * @see #remove(long)
   */
  @SuppressWarnings("null")
  public boolean retainAll(LongSet other)
  {
    if (isEmpty() || this == other) {
      return false;
    }
    if (other == null || other.isEmpty()) {
      clear();
      return true;
    }

    boolean res = firstIndices.retainAll(other.firstIndices);
    if (otherIndices.isEmpty()) {
      return res;
    }
    if (other.otherIndices.isEmpty()) {
      otherIndices.clear();
      return true;
    }
    Iterator<Entry<Long, IntSet>> itr1 = otherIndices.entrySet().iterator();
    Iterator<Entry<Long, IntSet>> itr2 = other.otherIndices.entrySet().iterator();
    Entry<Long, IntSet> e1 = null;
    Entry<Long, IntSet> e2 = null;
    int c = 0;
    while (true) {
      if (c <= 0) {
        if (itr1.hasNext()) {
          e1 = itr1.next();
        } else {
          return res;
        }
      }
      if (c >= 0) {
        if (itr2.hasNext()) {
          e2 = itr2.next();
        } else {
          itr1.remove();
          while (itr1.hasNext()) {
            itr1.next();
            itr1.remove();
          }
          return true;
        }
      }

      c = e1.getKey().compareTo(e2.getKey());
      if (c < 0) {
        itr1.remove();
        res = true;
      } else if (c == 0) {
        res |= e1.getValue().retainAll(e2.getValue());
        if (e1.getValue().isEmpty()) {
          itr1.remove();
        }
      }
    }
  }

  /**
   * Generates the intersection set
   *
   * @param other {@link LongSet} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #retainAll(LongSet)
   */
  @SuppressWarnings("null")
  public LongSet intersection(LongSet other)
  {
    if (isEmpty() || other == null || other.isEmpty()) {
      return empty();
    }
    if (this == other) {
      return clone();
    }

    LongSet res = new LongSet(firstIndices.intersection(other.firstIndices), new TreeMap<Long, IntSet>());
    if (otherIndices.isEmpty() || other.otherIndices.isEmpty()) {
      return res;
    }
    Iterator<Entry<Long, IntSet>> itr1 = otherIndices.entrySet().iterator();
    Iterator<Entry<Long, IntSet>> itr2 = other.otherIndices.entrySet().iterator();
    Entry<Long, IntSet> e1 = null;
    Entry<Long, IntSet> e2 = null;
    int c = 0;
    while (true) {
      if (c <= 0) {
        if (itr1.hasNext()) {
          e1 = itr1.next();
        } else {
          return res;
        }
      }
      if (c >= 0) {
        if (itr2.hasNext()) {
          e2 = itr2.next();
        } else {
          return res;
        }
      }

      c = e1.getKey().compareTo(e2.getKey());
      if (c == 0) {
        IntSet s = e1.getValue().intersection(e2.getValue());
        if (!s.isEmpty()) {
          res.otherIndices.put(e1.getKey(), s);
        }
      }
    }
  }

  /**
   * Adds all of the elements in the specified collection to this set if
   * they're not already present.
   *
   * @param other collection containing elements to be added to this set
   *
   * @return <tt>true</tt> if this set changed as a result of the call
   *
   * @throws NullPointerException     if the specified collection contains one or more null
   *                                  elements and this set does not permit null elements, or if
   *                                  the specified collection is null
   * @throws IllegalArgumentException if some property of an element of the specified collection
   *                                  prevents it from being added to this set
   * @see #add(long)
   */
  @SuppressWarnings("null")
  public boolean addAll(LongSet other)
  {
    if (other == null || other.isEmpty() || this == other) {
      return false;
    }

    boolean res = firstIndices.addAll(other.firstIndices);
    if (other.otherIndices.isEmpty()) {
      return res;
    }
    if (otherIndices.isEmpty()) {
      for (Entry<Long, IntSet> e : other.otherIndices.entrySet()) {
        otherIndices.put(e.getKey(), e.getValue().clone());
      }
      return true;
    }
    Iterator<Entry<Long, IntSet>> itr1 = new ArrayList<Entry<Long, IntSet>>(otherIndices.entrySet()).iterator();
    Iterator<Entry<Long, IntSet>> itr2 = other.otherIndices.entrySet().iterator();
    Entry<Long, IntSet> e1 = null;
    Entry<Long, IntSet> e2 = null;
    int c = 0;
    while (true) {
      if (c >= 0) {
        if (itr2.hasNext()) {
          e2 = itr2.next();
        } else {
          return res;
        }
      }
      if (c <= 0) {
        if (itr1.hasNext()) {
          e1 = itr1.next();
        } else {
          otherIndices.put(e2.getKey(), e2.getValue().clone());
          while (itr2.hasNext()) {
            e2 = itr2.next();
            otherIndices.put(e2.getKey(), e2.getValue().clone());
          }
          return true;
        }
      }

      c = e1.getKey().compareTo(e2.getKey());
      if (c > 0) {
        otherIndices.put(e2.getKey(), e2.getValue().clone());
        res = true;
      } else if (c == 0) {
        res |= e1.getValue().addAll(e2.getValue());
      }
    }
  }

  /**
   * Generates the union set
   *
   * @param other {@link LongSet} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #addAll(LongSet)
   */
  @SuppressWarnings("null")
  public LongSet union(LongSet other)
  {
    if (other == null || other.isEmpty() || this == other) {
      return clone();
    }
    if (isEmpty()) {
      return other.clone();
    }

    LongSet res = new LongSet(firstIndices.union(other.firstIndices), new TreeMap<Long, IntSet>());
    if (other.otherIndices.isEmpty()) {
      for (Entry<Long, IntSet> e : otherIndices.entrySet()) {
        res.otherIndices.put(e.getKey(), e.getValue().clone());
      }
      return res;
    }
    if (otherIndices.isEmpty()) {
      for (Entry<Long, IntSet> e : other.otherIndices.entrySet()) {
        res.otherIndices.put(e.getKey(), e.getValue().clone());
      }
      return res;
    }
    Iterator<Entry<Long, IntSet>> itr1 = otherIndices.entrySet().iterator();
    Iterator<Entry<Long, IntSet>> itr2 = other.otherIndices.entrySet().iterator();
    Entry<Long, IntSet> e1 = null;
    Entry<Long, IntSet> e2 = null;
    int c = 0;
    while (true) {
      if (c <= 0) {
        if (itr1.hasNext()) {
          e1 = itr1.next();
        } else {
          if (c != 0) {
            res.otherIndices.put(e2.getKey(), e2.getValue().clone());
          }
          while (itr2.hasNext()) {
            e2 = itr2.next();
            res.otherIndices.put(e2.getKey(), e2.getValue().clone());
          }
          return res;
        }
      }
      if (c >= 0) {
        if (itr2.hasNext()) {
          e2 = itr2.next();
        } else {
          res.otherIndices.put(e1.getKey(), e1.getValue().clone());
          while (itr1.hasNext()) {
            e1 = itr1.next();
            res.otherIndices.put(e1.getKey(), e1.getValue().clone());
          }
          return res;
        }
      }

      c = e1.getKey().compareTo(e2.getKey());
      if (c < 0) {
        res.otherIndices.put(e1.getKey(), e1.getValue().clone());
      } else if (c > 0) {
        res.otherIndices.put(e2.getKey(), e2.getValue().clone());
      } else {
        res.otherIndices.put(e1.getKey(), e1.getValue().union(e2.getValue()));
      }
    }
  }

  /**
   * Removes from this set all of its elements that are contained in the
   * specified collection.
   *
   * @param other collection containing elements to be removed from this set
   *
   * @return <tt>true</tt> if this set changed as a result of the call
   *
   * @throws NullPointerException if this set contains a null element and the specified
   *                              collection does not permit null elements (optional), or if
   *                              the specified collection is null
   * @see #remove(long)
   * @see #contains(long)
   */
  @SuppressWarnings("null")
  public boolean removeAll(LongSet other)
  {
    if (isEmpty() || other == null || other.isEmpty()) {
      return false;
    }
    if (this == other) {
      clear();
      return true;
    }

    boolean res = firstIndices.removeAll(other.firstIndices);
    if (otherIndices.isEmpty() || other.otherIndices.isEmpty()) {
      return res;
    }
    Iterator<Entry<Long, IntSet>> itr1 = otherIndices.entrySet().iterator();
    Iterator<Entry<Long, IntSet>> itr2 = other.otherIndices.entrySet().iterator();
    Entry<Long, IntSet> e1 = null;
    Entry<Long, IntSet> e2 = null;
    int c = 0;
    while (true) {
      if (c <= 0) {
        if (itr1.hasNext()) {
          e1 = itr1.next();
        } else {
          return res;
        }
      }
      if (c >= 0) {
        if (itr2.hasNext()) {
          e2 = itr2.next();
        } else {
          return res;
        }
      }

      c = e1.getKey().compareTo(e2.getKey());
      if (c == 0) {
        res |= e1.getValue().removeAll(e2.getValue());
        if (e1.getValue().isEmpty()) {
          itr1.remove();
        }
      }
    }
  }

  /**
   * Generates the difference set
   *
   * @param other {@link LongSet} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #removeAll(LongSet)
   */
  @SuppressWarnings("null")
  public LongSet difference(LongSet other)
  {
    if (other == null || other.isEmpty()) {
      return clone();
    }
    if (isEmpty() || this == other) {
      return empty();
    }

    LongSet res = new LongSet(firstIndices.difference(other.firstIndices), new TreeMap<Long, IntSet>());
    if (otherIndices.isEmpty()) {
      return res;
    }
    if (other.otherIndices.isEmpty()) {
      for (Entry<Long, IntSet> e : otherIndices.entrySet()) {
        res.otherIndices.put(e.getKey(), e.getValue().clone());
      }
      return res;
    }
    Iterator<Entry<Long, IntSet>> itr1 = otherIndices.entrySet().iterator();
    Iterator<Entry<Long, IntSet>> itr2 = other.otherIndices.entrySet().iterator();
    Entry<Long, IntSet> e1 = null;
    Entry<Long, IntSet> e2 = null;
    int c = 0;
    while (true) {
      if (c <= 0) {
        if (itr1.hasNext()) {
          e1 = itr1.next();
        } else {
          return res;
        }
      }
      if (c >= 0) {
        if (itr2.hasNext()) {
          e2 = itr2.next();
        } else {
          res.otherIndices.put(e1.getKey(), e1.getValue().clone());
          while (itr1.hasNext()) {
            e1 = itr1.next();
            res.otherIndices.put(e1.getKey(), e1.getValue().clone());
          }
          return res;
        }
      }

      c = e1.getKey().compareTo(e2.getKey());
      if (c < 0) {
        res.otherIndices.put(e1.getKey(), e1.getValue().clone());
      } else if (c == 0) {
        IntSet s = e1.getValue().difference(e2.getValue());
        if (!s.isEmpty()) {
          res.otherIndices.put(e1.getKey(), s);
        }
      }
    }
  }

  /**
   * Generates the symmetric difference set
   *
   * @param other {@link LongSet} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #flip(long)
   */
  @SuppressWarnings("null")
  public LongSet symmetricDifference(LongSet other)
  {
    if (other == null || other.isEmpty() || this == other) {
      return clone();
    }
    if (isEmpty()) {
      return other.clone();
    }

    LongSet res = new LongSet(firstIndices.symmetricDifference(other.firstIndices), new TreeMap<Long, IntSet>());
    if (other.otherIndices.isEmpty()) {
      for (Entry<Long, IntSet> e : otherIndices.entrySet()) {
        res.otherIndices.put(e.getKey(), e.getValue().clone());
      }
      return res;
    }
    if (otherIndices.isEmpty()) {
      for (Entry<Long, IntSet> e : other.otherIndices.entrySet()) {
        res.otherIndices.put(e.getKey(), e.getValue().clone());
      }
      return res;
    }
    Iterator<Entry<Long, IntSet>> itr1 = otherIndices.entrySet().iterator();
    Iterator<Entry<Long, IntSet>> itr2 = other.otherIndices.entrySet().iterator();
    Entry<Long, IntSet> e1 = null;
    Entry<Long, IntSet> e2 = null;
    int c = 0;
    while (true) {
      if (c <= 0) {
        if (itr1.hasNext()) {
          e1 = itr1.next();
        } else {
          if (c != 0) {
            res.otherIndices.put(e2.getKey(), e2.getValue().clone());
          }
          while (itr2.hasNext()) {
            e2 = itr2.next();
            res.otherIndices.put(e2.getKey(), e2.getValue().clone());
          }
          return res;
        }
      }
      if (c >= 0) {
        if (itr2.hasNext()) {
          e2 = itr2.next();
        } else {
          res.otherIndices.put(e1.getKey(), e1.getValue().clone());
          while (itr1.hasNext()) {
            e1 = itr1.next();
            res.otherIndices.put(e1.getKey(), e1.getValue().clone());
          }
          return res;
        }
      }

      c = e1.getKey().compareTo(e2.getKey());
      if (c < 0) {
        res.otherIndices.put(e1.getKey(), e1.getValue().clone());
      } else if (c > 0) {
        res.otherIndices.put(e2.getKey(), e2.getValue().clone());
      } else {
        res.otherIndices.put(e1.getKey(), e1.getValue().symmetricDifference(e2.getValue()));
      }
    }
  }

  /**
   * Generates the complement set. The returned set is represented by all the
   * elements strictly less than {@link #last()} that do not exist in the
   * current set.
   *
   * @return the complement set
   *
   * @see LongSet#complement()
   */
  public LongSet complemented()
  {
    LongSet cloned = clone();
    cloned.complement();
    return cloned;
  }

  /**
   * Complements the current set. The modified set is represented by all the
   * elements strictly less than {@link #last()} that do not exist in the
   * current set.
   *
   * @see LongSet#complemented()
   */
  public void complement()
  {
    if (otherIndices.isEmpty()) {
      firstIndices.complement();
      return;
    }

    // complement the last block
    Iterator<Entry<Long, IntSet>> itr = otherIndices.descendingMap().entrySet().iterator();
    Entry<Long, IntSet> e = itr.next();
    e.getValue().complement();
    if (e.getValue().isEmpty()) {
      itr.remove();
    }

    // complement other blocks
    NavigableMap<Long, IntSet> toAdd = new TreeMap<Long, IntSet>(); // avoid concurrent modification
    for (long i = e.getKey().longValue() - SUBSET_SIZE; i > 0L; i -= SUBSET_SIZE) {
      while (e != null && e.getKey().longValue() > i) {
        e = itr.hasNext() ? itr.next() : null;
      }

      if (e != null && e.getKey().longValue() == i) {
        if (e.getValue().add(SUBSET_SIZE - 1)) {
          e.getValue().complement();
          e.getValue().add(SUBSET_SIZE - 1);
        } else {
          e.getValue().complement();
        }
        if (e.getValue().isEmpty()) {
          itr.remove();
        }
      } else {
        IntSet s = firstIndices.empty();
        s.fill(0, SUBSET_SIZE - 1);
        toAdd.put(Long.valueOf(i), s);
      }
    }
    otherIndices.putAll(toAdd);
    if (firstIndices.add(SUBSET_SIZE - 1)) {
      firstIndices.complement();
      firstIndices.add(SUBSET_SIZE - 1);
    } else {
      firstIndices.complement();
    }
  }

  /**
   * Computes the intersection set size.
   * <p>
   * This is faster than calling {@link #intersection(LongSet)} and
   * then {@link #size()}
   *
   * @param other {@link LongSet} instance that represents the right
   *              operand
   *
   * @return the size
   */
  @SuppressWarnings("null")
  public long intersectionSize(LongSet other)
  {
    if (isEmpty() || other == null || other.isEmpty()) {
      return 0L;
    }
    if (this == other) {
      return size();
    }

    long res = firstIndices.intersectionSize(other.firstIndices);
    if (otherIndices.isEmpty() || other.otherIndices.isEmpty()) {
      return res;
    }
    Iterator<Entry<Long, IntSet>> itr1 = otherIndices.entrySet().iterator();
    Iterator<Entry<Long, IntSet>> itr2 = other.otherIndices.entrySet().iterator();
    Entry<Long, IntSet> e1 = null;
    Entry<Long, IntSet> e2 = null;
    int c = 0;
    while (true) {
      if (c <= 0) {
        if (itr1.hasNext()) {
          e1 = itr1.next();
        } else {
          return res;
        }
      }
      if (c >= 0) {
        if (itr2.hasNext()) {
          e2 = itr2.next();
        } else {
          return res;
        }
      }

      c = e1.getKey().compareTo(e2.getKey());
      if (c == 0) {
        res += e1.getValue().intersectionSize(e2.getValue());
      }
    }
  }

  /**
   * Computes the union set size.
   * <p>
   * This is faster than calling {@link #union(LongSet)} and then
   * {@link #size()}
   *
   * @param other {@link LongSet} instance that represents the right
   *              operand
   *
   * @return the size
   */
  public long unionSize(LongSet other)
  {
    return other == null ? size() : size() + other.size() - intersectionSize(other);
  }

  /**
   * Computes the symmetric difference set size.
   * <p>
   * This is faster than calling {@link #symmetricDifference(LongSet)}
   * and then {@link #size()}
   *
   * @param other {@link LongSet} instance that represents the right
   *              operand
   *
   * @return the size
   */
  public long symmetricDifferenceSize(LongSet other)
  {
    return other == null ? size() : size() + other.size() - 2 * intersectionSize(other);
  }

  /**
   * Computes the difference set size.
   * <p>
   * This is faster than calling {@link #difference(LongSet)} and then
   * {@link #size()}
   *
   * @param other {@link LongSet} instance that represents the right
   *              operand
   *
   * @return the size
   */
  public long differenceSize(LongSet other)
  {
    return other == null ? size() : size() - intersectionSize(other);
  }

  /**
   * Computes the complement set size.
   * <p>
   * This is faster than calling {@link #complemented()} and then
   * {@link #size()}
   *
   * @return the size
   */
  public long complementSize()
  {
    if (isEmpty()) {
      return 0L;
    }
    return last() - size() + 1L;
  }

  /**
   * Generates an empty set
   *
   * @return the empty set
   */
  public LongSet empty()
  {
    return new LongSet(firstIndices.empty(), new TreeMap<Long, IntSet>());
  }

  /**
   * See the <code>clone()</code> of {@link Object}
   *
   * @return cloned object
   */
  @Override
  public LongSet clone()
  {
    // NOTE: do not use super.clone() since it is 10 times slower!
    NavigableMap<Long, IntSet> otherIndicesClone = new TreeMap<Long, IntSet>();
    for (Entry<Long, IntSet> e : otherIndices.entrySet()) {
      otherIndicesClone.put(e.getKey(), e.getValue().clone());
    }
    return new LongSet(firstIndices.clone(), otherIndicesClone);
  }

  /**
   * Computes the compression factor of the equivalent bitmap representation
   * (1 means not compressed, namely a memory footprint similar to
   * {@link BitSet}, 2 means twice the size of {@link BitSet}, etc.)
   *
   * @return the compression factor
   */
  public double bitmapCompressionRatio()
  {
    //TODO
    throw new RuntimeException("TODO");
  }

  /**
   * Computes the compression factor of the equivalent integer collection (1
   * means not compressed, namely a memory footprint similar to
   * {@link ArrayList}, 2 means twice the size of {@link ArrayList}, etc.)
   *
   * @return the compression factor
   */
  public double collectionCompressionRatio()
  {
    //TODO
    throw new RuntimeException("TODO");
  }

  /**
   * @return a {@link ExtendedLongIterator} instance to iterate over the set
   */
  public ExtendedLongIterator longIterator()
  {
    return new ExtendedLongIterator();
  }

  /**
   * @return a {@link ExtendedLongIterator} instance to iterate over the set in
   * descending order
   */
  public ExtendedLongIterator descendingLongIterator()
  {
    return new ReverseLongIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<Long> iterator()
  {
    return new Iterator<Long>()
    {
      final ExtendedLongIterator itr = longIterator();

      @Override
      public boolean hasNext() {return itr.hasNext();}

      @Override
      public Long next() {return Long.valueOf(itr.next());}

      @Override
      public void remove() {itr.remove();}
    };
  }

  /**
   * Prints debug info about the given {@link LongSet} implementation
   *
   * @return a string that describes the internal representation of the
   * instance
   */
  public String debugInfo()
  {
    StringBuilder s = new StringBuilder();

    s.append("elements: ");
    s.append(toString());
    s.append("\nfirstIndices: " + firstIndices);
    s.append('\n');
    s.append("otherIndices: " + otherIndices.size());
    s.append('\n');
    for (Entry<Long, IntSet> e : otherIndices.entrySet()) {
      s.append('\t');
      s.append(e.getKey());
      s.append(", ");
      s.append(e.getValue());
      s.append('\n');
    }

    return s.toString();
  }

  /**
   * Adds to the set all the elements between <code>first</code> and
   * <code>last</code>, both included.
   *
   * @param from first element
   * @param to   last element
   */
  public void fill(long from, long to)
  {
    if (from > to) {
      throw new IndexOutOfBoundsException("from: " + from + " > to: " + to);
    }
    if (from == to) {
      add(from);
      return;
    }

    final long firstBlockIndex = (from / SUBSET_SIZE) * SUBSET_SIZE;
    final long lastBlockIndex = (to / SUBSET_SIZE) * SUBSET_SIZE;
    if (firstBlockIndex == lastBlockIndex) {
      // Case 1: One block
      if (firstBlockIndex == 0L) {
        firstIndices.fill((int) from, (int) to);
      } else {
        IntSet s = otherIndices.get(firstBlockIndex);
        if (s == null) {
          otherIndices.put(firstBlockIndex, s = firstIndices.empty());
        }
        s.fill((int) (from - firstBlockIndex), (int) (to - firstBlockIndex));
      }
    } else {
      // Case 2: Multiple blocks
      // Handle first block
      if (firstBlockIndex == 0L) {
        firstIndices.fill((int) from, SUBSET_SIZE - 1);
      } else {
        IntSet s = otherIndices.get(firstBlockIndex);
        if (s == null) {
          otherIndices.put(firstBlockIndex, s = firstIndices.empty());
        }
        s.fill((int) (from - firstBlockIndex), SUBSET_SIZE - 1);
      }

      // Handle intermediate words, if any
      for (long i = firstBlockIndex + SUBSET_SIZE; i < lastBlockIndex; i += SUBSET_SIZE) {
        IntSet s = firstIndices.empty();
        s.fill(0, SUBSET_SIZE - 1);
        otherIndices.put(Long.valueOf(i), s);
      }

      // Handle last word
      IntSet s = otherIndices.get(lastBlockIndex);
      if (s == null) {
        otherIndices.put(lastBlockIndex, s = firstIndices.empty());
      }
      s.fill(0, (int) (to - lastBlockIndex));
    }
  }

  /**
   * Removes from the set all the elements between <code>first</code> and
   * <code>last</code>, both included.
   *
   * @param from first element
   * @param to   last element
   */
  public void clear(long from, long to)
  {
    if (from > to) {
      throw new IndexOutOfBoundsException("from: " + from + " > to: " + to);
    }
    if (from == to) {
      remove(from);
      return;
    }

    final long firstBlockIndex = (from / SUBSET_SIZE) * SUBSET_SIZE;
    final long lastBlockIndex = (to / SUBSET_SIZE) * SUBSET_SIZE;
    if (firstBlockIndex == lastBlockIndex) {
      // Case 1: One block
      if (firstBlockIndex == 0L) {
        firstIndices.clear((int) from, (int) to);
      } else {
        IntSet s = otherIndices.get(firstBlockIndex);
        if (s != null) {
          s.clear((int) (from - firstBlockIndex), (int) (to - firstBlockIndex));
          if (s.isEmpty()) {
            otherIndices.remove(firstBlockIndex);
          }
        }
      }
    } else {
      // Case 2: Multiple blocks
      // Handle first block
      if (firstBlockIndex == 0L) {
        firstIndices.clear((int) from, SUBSET_SIZE - 1);
      } else {
        IntSet s = otherIndices.get(firstBlockIndex);
        if (s != null) {
          s.clear((int) (from - firstBlockIndex), SUBSET_SIZE - 1);
          if (s.isEmpty()) {
            otherIndices.remove(firstBlockIndex);
          }
        }
      }

      // Handle intermediate words, if any
      for (long i = firstBlockIndex + SUBSET_SIZE; i < lastBlockIndex; i += SUBSET_SIZE) {
        otherIndices.remove(Long.valueOf(i));
      }

      // Handle last word
      IntSet s = otherIndices.get(lastBlockIndex);
      if (s != null) {
        s.clear(0, (int) (to - lastBlockIndex));
        if (s.isEmpty()) {
          otherIndices.remove(lastBlockIndex);
        }
      }
    }
  }

  /**
   * Adds the element if it not existing, or removes it if existing
   *
   * @param e element to flip
   *
   * @see #symmetricDifference(LongSet)
   */
  public void flip(long e)
  {
    if (e < SUBSET_SIZE) {
      firstIndices.flip((int) e);
      return;
    }

    final long block = (e / SUBSET_SIZE) * SUBSET_SIZE;
    IntSet s = otherIndices.get(block);
    if (s == null) {
      otherIndices.put(block, s = firstIndices.empty());
    }
    s.flip((int) (e - block));
    if (s.isEmpty()) {
      otherIndices.remove(block);
    }
  }

  /**
   * Gets the <code>i</code><sup>th</sup> element of the set
   *
   * @param index position of the element in the sorted set
   *
   * @return the <code>i</code><sup>th</sup> element of the set
   *
   * @throws IndexOutOfBoundsException if <code>i</code> is less than zero, or greater or equal to
   *                                   {@link #size()}
   */
  public long get(long index)
  {
    if (index < firstIndices.size()) {
      return firstIndices.get((int) index);
    }

    index -= firstIndices.size();
    for (Entry<Long, IntSet> e : otherIndices.entrySet()) {
      if (index < e.getValue().size()) {
        return e.getKey().longValue() + e.getValue().get((int) index);
      }
      index -= e.getValue().size();
    }
    throw new IndexOutOfBoundsException(Long.toString(index));
  }

  /**
   * Provides position of element within the set.
   * <p>
   * It returns -1 if the element does not exist within the set.
   *
   * @param i element of the set
   *
   * @return the element position
   */
  public long indexOf(long i)
  {
    if (i < SUBSET_SIZE) {
      return firstIndices.indexOf((int) i);
    }
    long prev = firstIndices.size();
    for (Entry<Long, IntSet> e : otherIndices.entrySet()) {
      if (i < e.getKey().longValue() + SUBSET_SIZE) {
        return prev + e.getValue().indexOf((int) (i - e.getKey().longValue()));
      }
      prev += e.getValue().size();
    }
    return -1L;
  }

  /**
   * Converts a given array into an instance of the current class.
   *
   * @param a array to use to generate the new instance
   *
   * @return the converted collection
   */
  public LongSet convert(long... a)
  {
    LongSet res = empty();
    if (a != null) {
      a = Arrays.copyOf(a, a.length);
      Arrays.sort(a);
      for (long i : a) {
        res.add(i);
      }
    }
    return res;
  }

  /**
   * Converts a given array into an instance of the current class.
   *
   * @param a array to use to generate the new instance
   *
   * @return the converted collection
   */
  public LongSet convert(Collection<Long> a)
  {
    LongSet res = empty();
    Collection<Long> sorted;
    if (a != null) {
      if (a instanceof SortedSet<?> && ((SortedSet<?>) a).comparator() == null) {
        sorted = a;
      } else {
        sorted = new ArrayList<Long>(a);
        Collections.sort((List<Long>) sorted);
      }
      for (long i : sorted) {
        res.add(i);
      }
    }
    return res;
  }

  /**
   * Returns the first (lowest) element currently in this set.
   *
   * @return the first (lowest) element currently in this set
   *
   * @throws NoSuchElementException if this set is empty
   */
  public long first()
  {
    if (!firstIndices.isEmpty()) {
      return firstIndices.first();
    }
    if (otherIndices.isEmpty()) {
      throw new NoSuchElementException();
    }
    Entry<Long, IntSet> e = otherIndices.firstEntry();
    return e.getKey().longValue() + e.getValue().first();
  }

  /**
   * Returns the last (highest) element currently in this set.
   *
   * @return the last (highest) element currently in this set
   *
   * @throws NoSuchElementException if this set is empty
   */
  public long last()
  {
    if (otherIndices.isEmpty() && firstIndices.isEmpty()) {
      throw new NoSuchElementException();
    }
    if (!otherIndices.isEmpty()) {
      Entry<Long, IntSet> e = otherIndices.lastEntry();
      return e.getKey().longValue() + e.getValue().last();
    }
    return firstIndices.last();
  }

  /**
   * @return the number of elements in this set (its cardinality)
   */
  public long size()
  {
    long res = firstIndices.size();
    for (Entry<Long, IntSet> e : otherIndices.entrySet()) {
      res += e.getValue().size();
    }
    return res;
  }

  /**
   * @return <tt>true</tt> if this set contains no elements
   */
  public boolean isEmpty()
  {
    return firstIndices.isEmpty() && otherIndices.isEmpty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    return 31 * firstIndices.hashCode() + otherIndices.hashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof LongSet)) {
      return false;
    }
    final LongSet other = (LongSet) obj;
    return firstIndices.equals(other.firstIndices)
           && otherIndices.equals(other.otherIndices);
  }

  /**
   * Returns <tt>true</tt> if this set contains the specified element.
   *
   * @param i element whose presence in this set is to be tested
   *
   * @return <tt>true</tt> if this set contains the specified element
   */
  public boolean contains(long i)
  {
    if (i < SUBSET_SIZE) {
      return firstIndices.contains((int) i);
    }
    long first = (i / SUBSET_SIZE) * SUBSET_SIZE;
    IntSet s = otherIndices.get(first);
    if (s == null) {
      return false;
    }
    return s.contains((int) (i - first));
  }

  /**
   * Adds the specified element to this set if it is not already present. It
   * ensures that sets never contain duplicate elements.
   *
   * @param i element to be added to this set
   *
   * @return <tt>true</tt> if this set did not already contain the specified
   * element
   *
   * @throws IllegalArgumentException if some property of the specified element prevents it from
   *                                  being added to this set
   */
  public boolean add(long i)
  {
    if (i < SUBSET_SIZE) {
      return firstIndices.add((int) i);
    }
    long first = (i / SUBSET_SIZE) * SUBSET_SIZE;
    IntSet s = otherIndices.get(first);
    if (s == null) {
      otherIndices.put(first, s = firstIndices.empty());
    }
    return s.add((int) (i - first));
  }

  /**
   * Removes the specified element from this set if it is present.
   *
   * @param i object to be removed from this set, if present
   *
   * @return <tt>true</tt> if this set contained the specified element
   *
   * @throws UnsupportedOperationException if the <tt>remove</tt> operation is not supported by this set
   */
  public boolean remove(long i)
  {
    if (i < SUBSET_SIZE) {
      return firstIndices.remove((int) i);
    }
    long first = (i / SUBSET_SIZE) * SUBSET_SIZE;
    IntSet s = otherIndices.get(first);
    if (s == null) {
      return false;
    }
    boolean res = s.remove((int) (i - first));
    if (res && s.isEmpty()) {
      otherIndices.remove(first);
    }
    return res;
  }

  /**
   * Returns <tt>true</tt> if this set contains all of the elements of the
   * specified collection.
   *
   * @param other collection to be checked for containment in this set
   *
   * @return <tt>true</tt> if this set contains all of the elements of the
   * specified collection
   *
   * @throws NullPointerException if the specified collection contains one or more null
   *                              elements and this set does not permit null elements
   *                              (optional), or if the specified collection is null
   * @see #contains(long)
   */
  @SuppressWarnings("null")
  public boolean containsAll(LongSet other)
  {
    if (other == null || other.isEmpty() || other == this) {
      return true;
    }
    if (isEmpty()) {
      return false;
    }

    if (!firstIndices.containsAll(other.firstIndices)) {
      return false;
    }
    if (other.otherIndices.isEmpty()) {
      return true;
    }
    if (otherIndices.isEmpty()) {
      return false;
    }
    Iterator<Entry<Long, IntSet>> itr1 = otherIndices.entrySet().iterator();
    Iterator<Entry<Long, IntSet>> itr2 = other.otherIndices.entrySet().iterator();
    Entry<Long, IntSet> e1 = null;
    Entry<Long, IntSet> e2 = null;
    int c = 0;
    while (true) {
      if (c <= 0) {
        if (itr1.hasNext()) {
          e1 = itr1.next();
        } else {
          return c == 0 && !itr2.hasNext();
        }
      }
      if (c >= 0) {
        if (itr2.hasNext()) {
          e2 = itr2.next();
        } else {
          return true;
        }
      }

      c = e1.getKey().compareTo(e2.getKey());
      if (c > 0) {
        return false;
      } else if (c == 0) {
        if (!e1.getValue().containsAll(e2.getValue())) {
          return false;
        }
      }
    }
  }

  /**
   * Returns <code>true</code> if the specified {@link LongSet}
   * instance contains any elements that are also contained within this
   * {@link LongSet} instance
   *
   * @param other {@link LongSet} to intersect with
   *
   * @return a boolean indicating whether this {@link LongSet}
   * intersects the specified {@link LongSet}.
   */
  @SuppressWarnings("null")
  public boolean containsAny(LongSet other)
  {
    if (other == null || other.isEmpty() || other == this) {
      return true;
    }
    if (isEmpty()) {
      return false;
    }

    if (firstIndices.containsAny(other.firstIndices) && !other.firstIndices.isEmpty()) {
      return true;
    }
    if (other.otherIndices.isEmpty() || otherIndices.isEmpty()) {
      return false;
    }
    Iterator<Entry<Long, IntSet>> itr1 = otherIndices.entrySet().iterator();
    Iterator<Entry<Long, IntSet>> itr2 = other.otherIndices.entrySet().iterator();
    Entry<Long, IntSet> e1 = null;
    Entry<Long, IntSet> e2 = null;
    int c = 0;
    while (true) {
      if (c <= 0) {
        if (itr1.hasNext()) {
          e1 = itr1.next();
        } else {
          return false;
        }
      }
      if (c >= 0) {
        if (itr2.hasNext()) {
          e2 = itr2.next();
        } else {
          return false;
        }
      }

      c = e1.getKey().compareTo(e2.getKey());
      if (c == 0 && e1.getValue().containsAny(e2.getValue())) {
        return true;
      }
    }
  }

  /**
   * Returns <code>true</code> if the specified {@link LongSet}
   * instance contains at least <code>minElements</code> elements that are
   * also contained within this {@link LongSet} instance
   *
   * @param other       {@link LongSet} instance to intersect with
   * @param minElements minimum number of elements to be contained within this
   *                    {@link LongSet} instance
   *
   * @return a boolean indicating whether this {@link LongSet}
   * intersects the specified {@link LongSet}.
   *
   * @throws IllegalArgumentException if <code>minElements &lt; 1</code>
   */
  @SuppressWarnings("null")
  public boolean containsAtLeast(LongSet other, long minElements)
  {
    if (minElements < 1) {
      throw new IllegalArgumentException();
    }
    if (this == other) {
      return size() >= minElements;
    }
    if (other == null || other.isEmpty() || isEmpty() || size() < minElements) {
      return false;
    }

    long res = firstIndices.intersectionSize(other.firstIndices);
    if (res >= minElements) {
      return true;
    }
    if (otherIndices.isEmpty() || other.otherIndices.isEmpty()) {
      return false;
    }
    Iterator<Entry<Long, IntSet>> itr1 = otherIndices.entrySet().iterator();
    Iterator<Entry<Long, IntSet>> itr2 = other.otherIndices.entrySet().iterator();
    Entry<Long, IntSet> e1 = null;
    Entry<Long, IntSet> e2 = null;
    int c = 0;
    while (true) {
      if (c <= 0) {
        if (itr1.hasNext()) {
          e1 = itr1.next();
        } else {
          return false;
        }
      }
      if (c >= 0) {
        if (itr2.hasNext()) {
          e2 = itr2.next();
        } else {
          return false;
        }
      }

      c = e1.getKey().compareTo(e2.getKey());
      if (c == 0) {
        res += e1.getValue().intersectionSize(e2.getValue());
        if (res >= minElements) {
          return true;
        }
      }
    }
  }

  /**
   * Removes all of the elements from this set. The set will be empty after
   * this call returns.
   */
  public void clear()
  {
    firstIndices.clear();
    otherIndices.clear();
  }

  /**
   * @return an array containing all the elements in this set, in the same
   * order.
   */
  public long[] toArray()
  {
    if (isEmpty()) {
      return null;
    }
    return toArray(new long[(int) size()]);
  }

  /**
   * Returns an array containing all of the elements in this set.
   * <p>
   * If this set fits in the specified array with room to spare (i.e., the
   * array has more elements than this set), the element in the array
   * immediately following the end of the set are left unchanged.
   *
   * @param a the array into which the elements of this set are to be
   *          stored.
   *
   * @return the array containing all the elements in this set
   *
   * @throws NullPointerException     if the specified array is null
   * @throws IllegalArgumentException if this set does not fit in the specified array
   */
  public long[] toArray(long[] a)
  {
    if (a.length < size()) {
      throw new IllegalArgumentException();
    }
    if (isEmpty()) {
      return a;
    }
    ExtendedLongIterator itr = longIterator();
    int i = 0;
    while (itr.hasNext()) {
      a[i++] = itr.next();
    }
    return a;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString()
  {
    ExtendedLongIterator itr = longIterator();
    if (!itr.hasNext()) {
      return "[]";
    }

    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (; ; ) {
      long e = itr.next();
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
  public int compareTo(LongSet o)
  {
    //TODO
    throw new RuntimeException("TODO");
  }

  /**
   * A  {@link Iterator} -like interface that allows to "skip" some elements of the set
   */
  public class ExtendedLongIterator
  {
    /**
     * @uml.property name="itr"
     * @uml.associationEnd
     */
    protected IntIterator itr;
    protected Iterator<Entry<Long, IntSet>> otherItrs;
    protected long first = 0;
    /**
     * @uml.property name="current"
     * @uml.associationEnd
     */
    protected IntSet current = null;

    private ExtendedLongIterator()
    {
      itr = firstIndices.iterator();
      otherItrs = otherIndices.entrySet().iterator();
      first = 0;
    }

    protected void nextItr()
    {
      Entry<Long, IntSet> e = otherItrs.next();
      current = e.getValue();
      itr = e.getValue().iterator();
      first = e.getKey().longValue();
    }

    /**
     * @return <tt>true</tt> if the iterator has more elements.
     */
    public boolean hasNext()
    {
      return otherItrs.hasNext() || itr.hasNext();
    }

    /**
     * @return the next element in the iteration.
     *
     * @throws NoSuchElementException iteration has no more elements.
     */
    public long next()
    {
      if (!itr.hasNext()) {
        nextItr();
      }
      return first + itr.next();
    }

    /**
     * Removes from the underlying collection the last element returned by
     * the iterator (optional operation). This method can be called only
     * once per call to <tt>next</tt>. The behavior of an iterator is
     * unspecified if the underlying collection is modified while the
     * iteration is in progress in any way other than by calling this
     * method.
     *
     * @throws UnsupportedOperationException if the <tt>remove</tt> operation is not supported by
     *                                       this Iterator.
     * @throws IllegalStateException         if the <tt>next</tt> method has not yet been called,
     *                                       or the <tt>remove</tt> method has already been called
     *                                       after the last call to the <tt>next</tt> method.
     */
    public void remove()
    {
      itr.remove();
      if (current != null && current.isEmpty()) {
        otherItrs.remove();
      }
    }

    /**
     * Skips all the elements before the the specified element, so that
     * {@link #next()} gives the given element or, if it does not exist, the
     * element immediately after according to the sorting provided by this
     * set.
     * <p>
     * If <code>element</code> is less than the next element, it does
     * nothing
     *
     * @param element first element to not skip
     */
    public void skipAllBefore(long element)
    {
      while (element >= first + SUBSET_SIZE) {
        if (otherItrs.hasNext()) {
          nextItr();
        } else {
          itr.skipAllBefore(SUBSET_SIZE - 1); // no next
          assert !itr.hasNext();
          return;
        }
      }
      if (element < first) {
        return;
      }
      itr.skipAllBefore((int) (element - first));
    }
  }

  /**
   * Iteration over the union of all indices, reverse order
   */
  private class ReverseLongIterator extends ExtendedLongIterator
  {
    private ReverseLongIterator()
    {
      super();
      otherItrs = otherIndices.descendingMap().entrySet().iterator();
      nextItr();
    }

    @Override
    protected void nextItr()
    {
      if (otherItrs.hasNext()) {
        Entry<Long, IntSet> e = otherItrs.next();
        current = e.getValue();
        itr = e.getValue().descendingIterator();
        first = e.getKey().longValue();
      } else {
        itr = firstIndices.descendingIterator();
        current = null;
        first = 0;
      }
    }

    @Override
    public void skipAllBefore(long element)
    {
      while (element <= first) {
        nextItr();
      }
      if (element > first + SUBSET_SIZE) {
        return;
      }
      itr.skipAllBefore((int) (element - first));
    }
  }
}
