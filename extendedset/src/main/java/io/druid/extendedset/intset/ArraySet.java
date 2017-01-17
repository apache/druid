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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;

/**
 * {@link IntSet}-based class internally managed by a sorted array of
 * <code>int</code>s.
 *
 * @author Alessandro Colantonio
 * @version $Id: ArraySet.java 156 2011-09-01 00:13:57Z cocciasik $
 */
public class ArraySet extends AbstractIntSet
{
  /**
   * elements of the set
   */
  private int[] elements;

  /**
   * set cardinality
   */
  private int size;

  /**
   * Empty-set constructor
   */
  public ArraySet()
  {
    size = 0;
    elements = null;
  }

  /**
   * Replace the content of the current instance with the content of another
   * instance
   *
   * @param other
   */
  private void replaceWith(ArraySet other)
  {
    size = other.size;
    elements = other.elements;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double bitmapCompressionRatio()
  {
    if (isEmpty()) {
      return 0D;
    }
    return size() / Math.ceil(elements[size - 1] / 32D);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double collectionCompressionRatio()
  {
    return isEmpty() ? 0D : 1D;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ArraySet empty()
  {
    return new ArraySet();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntIterator iterator()
  {
    return new IntIterator()
    {
      int next = 0;

      @Override
      public void skipAllBefore(int e)
      {
        if (e <= elements[next]) {
          return;
        }
        next = Arrays.binarySearch(elements, next + 1, size, e);
        if (next < 0) {
          next = -(next + 1);
        }
      }

      @Override
      public boolean hasNext()
      {
        return next < size;
      }

      @Override
      public int next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return elements[next++];
      }

      @Override
      public void remove()
      {
        next--;
        size--;
        System.arraycopy(elements, next + 1, elements, next, size - next);
        compact();
      }

      @Override
      public IntIterator clone()
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntIterator descendingIterator()
  {
    return new IntIterator()
    {
      int next = size - 1;

      @Override
      public void skipAllBefore(int e)
      {
        if (e >= elements[next]) {
          return;
        }
        next = Arrays.binarySearch(elements, 0, next, e);
        if (next < 0) {
          next = -(next + 1) - 1;
        }
      }

      @Override
      public boolean hasNext()
      {
        return next >= 0;
      }

      @Override
      public int next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return elements[next--];
      }

      @Override
      public void remove()
      {
        next++;
        size--;
        System.arraycopy(elements, next + 1, elements, next, size - next);
        compact();
      }

      @Override
      public IntIterator clone()
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ArraySet clone()
  {
    // NOTE: do not use super.clone() since it is 10 times slower!
    ArraySet c = empty();
    if (!isEmpty()) {
      c.elements = Arrays.copyOf(elements, elements.length);
      c.size = size;
    }
    return c;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String debugInfo()
  {
    return toString();
  }

  /**
   * Assures that the size of {@link #elements} is sufficient to contain
   * {@link #size} elements.
   */
  private void ensureCapacity()
  {
    int capacity = elements == null ? 0 : elements.length;
    if (capacity >= size) {
      return;
    }
    capacity = Math.max(capacity << 1, size);

    if (elements == null) {
      // nothing to copy
      elements = new int[capacity];
      return;
    }
    elements = Arrays.copyOf(elements, capacity);
  }

  /**
   * Removes unused allocated words at the end of {@link #words} only when they
   * are more than twice of the needed space
   */
  private void compact()
  {
    if (size == 0) {
      elements = null;
      return;
    }
    if (elements != null && (size << 1) < elements.length) {
      elements = Arrays.copyOf(elements, size);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean add(int element)
  {
    // append
    if (isEmpty() || elements[size - 1] < element) {
      size++;
      ensureCapacity();
      elements[size - 1] = element;
      return true;
    }

    // insert
    int pos = Arrays.binarySearch(elements, 0, size, element);
    if (pos >= 0) {
      return false;
    }

    size++;
    ensureCapacity();
    pos = -(pos + 1);
    System.arraycopy(elements, pos, elements, pos + 1, size - pos - 1);
    elements[pos] = element;
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(int element)
  {
    if (element < 0) {
      return false;
    }

    int pos = Arrays.binarySearch(elements, 0, size, element);
    if (pos < 0) {
      return false;
    }

    size--;
    System.arraycopy(elements, pos + 1, elements, pos, size - pos);
    compact();
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flip(int element)
  {
    // first
    if (isEmpty()) {
      size++;
      ensureCapacity();
      elements[size - 1] = element;
      return;
    }

    int pos = Arrays.binarySearch(elements, 0, size, element);

    // add
    if (pos < 0) {
      size++;
      ensureCapacity();
      pos = -(pos + 1);
      System.arraycopy(elements, pos, elements, pos + 1, size - pos - 1);
      elements[pos] = element;
      return;
    }

    // remove
    size--;
    System.arraycopy(elements, pos + 1, elements, pos, size - pos);
    compact();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean contains(int element)
  {
    if (isEmpty()) {
      return false;
    }
    return Arrays.binarySearch(elements, 0, size, element) >= 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAll(IntSet c)
  {
    if (c == null || c.isEmpty() || c == this) {
      return true;
    }
    if (isEmpty()) {
      return false;
    }

    final ArraySet o = convert(c);
    final int[] thisElements = elements;    // faster
    final int[] otherElements = o.elements;    // faster
    int otherSize = o.size;
    int thisIndex = -1;
    int otherIndex = -1;
    while (thisIndex < (size - 1) && otherIndex < (otherSize - 1)) {
      thisIndex++;
      otherIndex++;
      while (thisElements[thisIndex] < otherElements[otherIndex]) {
        if (thisIndex == size - 1) {
          return false;
        }
        thisIndex++;
      }
      if (thisElements[thisIndex] > otherElements[otherIndex]) {
        return false;
      }
    }
    return otherIndex == otherSize - 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAny(IntSet other)
  {
    if (other == null || other.isEmpty() || other == this) {
      return true;
    }
    if (isEmpty()) {
      return false;
    }

    final ArraySet o = convert(other);
    final int[] thisElements = elements;    // faster
    final int[] otherElements = o.elements;    // faster
    int otherSize = o.size;
    int thisIndex = -1;
    int otherIndex = -1;
    while (thisIndex < (size - 1) && otherIndex < (otherSize - 1)) {
      thisIndex++;
      otherIndex++;
      while (thisElements[thisIndex] != otherElements[otherIndex]) {
        while (thisElements[thisIndex] > otherElements[otherIndex]) {
          if (otherIndex == otherSize - 1) {
            return false;
          }
          otherIndex++;
        }
        if (thisElements[thisIndex] == otherElements[otherIndex]) {
          break;
        }
        while (thisElements[thisIndex] < otherElements[otherIndex]) {
          if (thisIndex == size - 1) {
            return false;
          }
          thisIndex++;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAtLeast(IntSet other, int minElements)
  {
    if (minElements < 1) {
      throw new IllegalArgumentException();
    }
    if ((size >= 0 && size < minElements) || other == null || other.isEmpty() || isEmpty()) {
      return false;
    }
    if (this == other) {
      return size() >= minElements;
    }

    final ArraySet o = convert(other);
    final int[] thisElements = elements;    // faster
    final int[] otherElements = o.elements;    // faster
    int otherSize = o.size;
    int thisIndex = -1;
    int otherIndex = -1;
    int res = 0;
    while (thisIndex < (size - 1) && otherIndex < (otherSize - 1)) {
      thisIndex++;
      otherIndex++;
      while (thisElements[thisIndex] != otherElements[otherIndex]) {
        while (thisElements[thisIndex] > otherElements[otherIndex]) {
          if (otherIndex == otherSize - 1) {
            return false;
          }
          otherIndex++;
        }
        if (thisElements[thisIndex] == otherElements[otherIndex]) {
          break;
        }
        while (thisElements[thisIndex] < otherElements[otherIndex]) {
          if (thisIndex == size - 1) {
            return false;
          }
          thisIndex++;
        }
      }
      res++;
      if (res >= minElements) {
        return true;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addAll(IntSet c)
  {
    ArraySet res = union(c);
    boolean r = !equals(res);
    replaceWith(res);
    return r;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean retainAll(IntSet c)
  {
    ArraySet res = intersection(c);
    boolean r = !equals(res);
    replaceWith(res);
    return r;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean removeAll(IntSet c)
  {
    ArraySet res = difference(c);
    boolean r = !equals(res);
    replaceWith(res);
    return r;
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
    final int[] thisElements = elements;    // faster
    int h = 1;
    for (int i = 0; i < size; i++) {
      h = (h << 5) - h + thisElements[i];
    }
    return h;
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
    if (!(obj instanceof ArraySet)) {
      return super.equals(obj);
    }
    final ArraySet other = (ArraySet) obj;
    if (size != other.size) {
      return false;
    }
    final int[] thisElements = elements;    // faster
    final int[] otherElements = other.elements;  // faster
    for (int i = 0; i < size; i++) {
      if (thisElements[i] != otherElements[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size()
  {
    return size;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty()
  {
    return size == 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear()
  {
    elements = null;
    size = 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int first()
  {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    return elements[0];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int last()
  {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    return elements[size - 1];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int intersectionSize(IntSet other)
  {
    if (isEmpty() || other == null || other.isEmpty()) {
      return 0;
    }
    if (this == other) {
      return size();
    }

    final ArraySet o = convert(other);
    final int[] thisElements = elements;    // faster
    final int[] otherElements = o.elements;    // faster
    int otherSize = o.size;
    int thisIndex = -1;
    int otherIndex = -1;
    int res = 0;
    while (thisIndex < (size - 1) && otherIndex < (otherSize - 1)) {
      thisIndex++;
      otherIndex++;
      while (thisElements[thisIndex] != otherElements[otherIndex]) {
        while (thisElements[thisIndex] > otherElements[otherIndex]) {
          if (otherIndex == otherSize - 1) {
            return res;
          }
          otherIndex++;
        }
        if (thisElements[thisIndex] == otherElements[otherIndex]) {
          break;
        }
        while (thisElements[thisIndex] < otherElements[otherIndex]) {
          if (thisIndex == size - 1) {
            return res;
          }
          thisIndex++;
        }
      }
      res++;
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ArraySet intersection(IntSet other)
  {
    if (isEmpty() || other == null || other.isEmpty()) {
      return empty();
    }
    if (this == other) {
      return clone();
    }

    final ArraySet o = convert(other);
    int otherSize = o.size;
    int thisIndex = -1;
    int otherIndex = -1;
    int resSize = 0;
    final int[] thisElements = elements;    // faster
    final int[] otherElements = o.elements;    // faster
    final int[] resElements = new int[Math.min(size, otherSize)];
    while (thisIndex < (size - 1) && otherIndex < (otherSize - 1)) {
      thisIndex++;
      otherIndex++;
      while (thisElements[thisIndex] != otherElements[otherIndex]) {
        while (thisElements[thisIndex] > otherElements[otherIndex]) {
          if (otherIndex == otherSize - 1) {
            ArraySet res = empty();
            res.elements = resElements;
            res.size = resSize;
            res.compact();
            return res;
          }
          otherIndex++;
        }
        if (thisElements[thisIndex] == otherElements[otherIndex]) {
          break;
        }
        while (thisElements[thisIndex] < otherElements[otherIndex]) {
          if (thisIndex == size - 1) {
            ArraySet res = empty();
            res.elements = resElements;
            res.size = resSize;
            res.compact();
            return res;
          }
          thisIndex++;
        }
      }
      resElements[resSize++] = thisElements[thisIndex];
    }

    ArraySet res = empty();
    res.elements = resElements;
    res.size = resSize;
    res.compact();
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ArraySet union(IntSet other)
  {
    if (this == other || other == null || other.isEmpty()) {
      return clone();
    }
    if (isEmpty()) {
      ArraySet cloned = convert(other);
      if (cloned == other) {
        cloned = cloned.clone();
      }
      return cloned;
    }

    final ArraySet o = convert(other);
    int otherSize = o.size;
    int thisIndex = -1;
    int otherIndex = -1;
    int resSize = 0;
    final int[] thisElements = elements;    // faster
    final int[] otherElements = o.elements;    // faster
    final int[] resElements = new int[size + otherSize];
mainLoop:
    while (thisIndex < (size - 1) && otherIndex < (otherSize - 1)) {
      thisIndex++;
      otherIndex++;
      while (thisElements[thisIndex] != otherElements[otherIndex]) {
        while (thisElements[thisIndex] > otherElements[otherIndex]) {
          resElements[resSize++] = otherElements[otherIndex];
          if (otherIndex == otherSize - 1) {
            resElements[resSize++] = thisElements[thisIndex];
            break mainLoop;
          }
          otherIndex++;
        }
        if (thisElements[thisIndex] == otherElements[otherIndex]) {
          break;
        }
        while (thisElements[thisIndex] < otherElements[otherIndex]) {
          resElements[resSize++] = thisElements[thisIndex];
          if (thisIndex == size - 1) {
            resElements[resSize++] = otherElements[otherIndex];
            break mainLoop;
          }
          thisIndex++;
        }
      }
      resElements[resSize++] = thisElements[thisIndex];
    }
    while (thisIndex < size - 1) {
      resElements[resSize++] = thisElements[++thisIndex];
    }
    while (otherIndex < otherSize - 1) {
      resElements[resSize++] = otherElements[++otherIndex];
    }

    ArraySet res = empty();
    res.elements = resElements;
    res.size = resSize;
    res.compact();
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ArraySet difference(IntSet other)
  {
    if (isEmpty() || this == other) {
      return empty();
    }
    if (other == null || other.isEmpty()) {
      return clone();
    }

    final ArraySet o = convert(other);
    int otherSize = o.size;
    int thisIndex = -1;
    int otherIndex = -1;
    int resSize = 0;
    final int[] thisElements = elements;    // faster
    final int[] otherElements = o.elements;    // faster
    final int[] resElements = new int[size];
mainLoop:
    while (thisIndex < (size - 1) && otherIndex < (otherSize - 1)) {
      thisIndex++;
      otherIndex++;
      while (thisElements[thisIndex] != otherElements[otherIndex]) {
        while (thisElements[thisIndex] > otherElements[otherIndex]) {
          if (otherIndex == otherSize - 1) {
            resElements[resSize++] = thisElements[thisIndex];
            break mainLoop;
          }
          otherIndex++;
        }
        if (thisElements[thisIndex] == otherElements[otherIndex]) {
          break;
        }
        while (thisElements[thisIndex] < otherElements[otherIndex]) {
          resElements[resSize++] = thisElements[thisIndex];
          if (thisIndex == size - 1) {
            break mainLoop;
          }
          thisIndex++;
        }
      }
    }
    while (thisIndex < size - 1) {
      resElements[resSize++] = thisElements[++thisIndex];
    }

    ArraySet res = empty();
    res.elements = resElements;
    res.size = resSize;
    res.compact();
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ArraySet symmetricDifference(IntSet other)
  {
    if (this == other || other == null || other.isEmpty()) {
      return clone();
    }
    if (isEmpty()) {
      return convert(other).clone();
    }

    final ArraySet o = convert(other);
    int otherSize = o.size;
    int thisIndex = -1;
    int otherIndex = -1;
    int resSize = 0;
    final int[] thisElements = elements;    // faster
    final int[] otherElements = o.elements;    // faster
    final int[] resElements = new int[size + otherSize];
mainLoop:
    while (thisIndex < (size - 1) && otherIndex < (otherSize - 1)) {
      thisIndex++;
      otherIndex++;
      while (thisElements[thisIndex] != otherElements[otherIndex]) {
        while (thisElements[thisIndex] > otherElements[otherIndex]) {
          resElements[resSize++] = otherElements[otherIndex];
          if (otherIndex == otherSize - 1) {
            resElements[resSize++] = thisElements[thisIndex];
            break mainLoop;
          }
          otherIndex++;
        }
        if (thisElements[thisIndex] == otherElements[otherIndex]) {
          break;
        }
        while (thisElements[thisIndex] < otherElements[otherIndex]) {
          resElements[resSize++] = thisElements[thisIndex];
          if (thisIndex == size - 1) {
            resElements[resSize++] = otherElements[otherIndex];
            break mainLoop;
          }
          thisIndex++;
        }
      }
    }
    while (thisIndex < size - 1) {
      resElements[resSize++] = thisElements[++thisIndex];
    }
    while (otherIndex < otherSize - 1) {
      resElements[resSize++] = otherElements[++otherIndex];
    }

    ArraySet res = empty();
    res.elements = resElements;
    res.size = resSize;
    res.compact();
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

    IntIterator thisItr = clone().iterator(); // avoid concurrency
    elements = new int[complementSize()];
    final int[] thisElements = elements;    // faster
    size = 0;
    int u = -1;
    while (thisItr.hasNext()) {
      int c = thisItr.next();
      while (++u < c) {
        thisElements[size++] = u;
      }
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
    if (from == to) {
      add(from);
      return;
    }

    int[] thisElements = elements;    // faster

    if (isEmpty()) {
      size = to - from + 1;
      ensureCapacity();
      thisElements = elements;
      for (int i = 0; i < size; i++) {
        thisElements[i] = from++;
      }
      return;
    }

    // increase capacity, if necessary
    int posFrom = Arrays.binarySearch(thisElements, 0, size, from);
    boolean fromMissing = posFrom < 0;
    if (fromMissing) {
      posFrom = -posFrom - 1;
    }

    int posTo = Arrays.binarySearch(thisElements, posFrom, size, to);
    boolean toMissing = posTo < 0;
    if (toMissing) {
      posTo = -posTo - 1;
    }

    int delta = 0;
    if (toMissing || (fromMissing && (posFrom == posTo + 1))) {
      delta = 1;
    }

    int gap = to - from;
    delta += gap - (posTo - posFrom);
    if (delta > 0) {
      size += delta;
      ensureCapacity();
      thisElements = elements;
      System.arraycopy(thisElements, posTo, thisElements, posTo + delta, size - delta - posTo);
      posTo = posFrom + gap;

      // set values
      for (int i = posFrom; i <= posTo; i++) {
        thisElements[i] = from++;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear(int from, int to)
  {
    if (isEmpty()) {
      return;
    }
    if (from > to) {
      throw new IndexOutOfBoundsException("from: " + from + " > to: " + to);
    }
    if (from == to) {
      remove(from);
      return;
    }

    int posFrom = Arrays.binarySearch(elements, 0, size, from);
    if (posFrom < 0) {
      posFrom = -posFrom - 1;
    }
    if (posFrom >= size) {
      return;
    }
    int posTo = Arrays.binarySearch(elements, posFrom, size, to);
    if (posTo >= 0) {
      posTo++;
    } else {
      posTo = -posTo - 1;
    }
    if (posFrom == posTo) {
      return;
    }
    System.arraycopy(elements, posTo, elements, posFrom, size - posTo);
    size -= posTo - posFrom;
  }

  /**
   * Convert a generic {@link IntSet} instance to an {@link ArraySet} instance
   *
   * @param c
   *
   * @return
   */
  private ArraySet convert(IntSet c)
  {
    if (c instanceof ArraySet) {
      return (ArraySet) c;
    }

    int[] resElements = new int[c.size()];
    int resSize = 0;
    IntIterator itr = c.iterator();
    while (itr.hasNext()) {
      resElements[resSize++] = itr.next();
    }

    ArraySet res = empty();
    res.elements = resElements;
    res.size = resSize;
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ArraySet convert(int... a)
  {
    int[] resElements = null;
    int resSize = 0;
    int last = -1;
    if (a != null) {
      resElements = new int[a.length];
      a = Arrays.copyOf(a, a.length);
      Arrays.sort(a);
      if (a[0] < 0) {
        throw new ArrayIndexOutOfBoundsException(Integer.toString(a[0]));
      }
      for (int i : a) {
        if (last != i) {
          resElements[resSize++] = last = i;
        }
      }
    }

    ArraySet res = empty();
    res.elements = resElements;
    res.size = resSize;
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ArraySet convert(Collection<Integer> c)
  {
    Collection<Integer> sorted;
    int[] resElements = null;
    int resSize = 0;
    int last = -1;
    if (c != null) {
      resElements = new int[c.size()];
      if (c instanceof SortedSet<?> && ((SortedSet<?>) c).comparator() == null) {
        sorted = c;
      } else {
        sorted = new ArrayList<Integer>(c);
        Collections.sort((List<Integer>) sorted);
        int first = ((ArrayList<Integer>) sorted).get(0).intValue();
        if (first < 0) {
          throw new ArrayIndexOutOfBoundsException(Integer.toString(first));
        }
      }
      for (int i : sorted) {
        if (last != i) {
          resElements[resSize++] = last = i;
        }
      }
    }

    ArraySet res = empty();
    res.elements = resElements;
    res.size = resSize;
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ArraySet complemented()
  {
    ArraySet res = clone();
    res.complement();
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int get(int i)
  {
    if (i < 0 || i >= size) {
      throw new IndexOutOfBoundsException(Integer.toString(i));
    }
    return elements[i];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int indexOf(int e)
  {
    if (e < 0) {
      throw new IllegalArgumentException("positive integer expected: " + Integer.toString(e));
    }
    int pos = Arrays.binarySearch(elements, 0, size, e);
    if (pos < 0) {
      return -1;
    }
    return pos;
  }
}
