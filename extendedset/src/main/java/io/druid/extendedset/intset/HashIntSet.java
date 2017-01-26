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

// update CompactIdentityHashSet.java, UniqueSet.java and
// SoftHashMapIndex.java accordingly.

import io.druid.extendedset.utilities.IntHashCode;

import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;

/**
 * Implements a fast hash-set.
 * <p/>
 * Inspired by <a href=
 * "http://code.google.com/p/ontopia/source/browse/trunk/ontopia/src/java/net/ontopia/utils/CompactHashSet.java"
 * >http://code.google.com/p/ontopia/source/browse/trunk/ontopia/src/java/net/
 * ontopia/utils/CompactHashSet.java</a>
 *
 * @author Alessandro Colantonio
 * @version $Id: HashIntSet.java 156 2011-09-01 00:13:57Z cocciasik $
 */
public class HashIntSet extends AbstractIntSet
{
  protected final static int INITIAL_SIZE = 3;
  protected final static double LOAD_FACTOR = 0.75D;

  /**
   * empty cell
   */
  protected final static int EMPTY = -1;

  /**
   * When an object is deleted this object is put into the hashtable in its
   * place, so that other objects with the same key (collisions) further down
   * the hashtable are not lost after we delete an object in the collision
   * chain.
   */
  protected final static int REMOVED = -2;

  /**
   * number of elements
   */
  protected int size;

  /**
   * This is the number of empty cells. It's not necessarily the same as
   * objects.length - elements, because some cells may contain REMOVED.
   */
  protected int freecells;

  /**
   * cells
   */
  protected int[] cells;

  /**
   * concurrent modification during iteration
   */
  protected int modCount;

  /**
   * Constructs a new, empty set.
   */
  public HashIntSet()
  {
    this(INITIAL_SIZE);
  }

  /**
   * Constructs a new, empty set.
   *
   * @param initialSize
   */
  public HashIntSet(int initialSize)
  {
    if (initialSize <= 0) {
      throw new IllegalArgumentException();
    }
    cells = new int[initialSize];
    modCount = 0;
    clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntIterator iterator()
  {
    return new SortedIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntIterator descendingIterator()
  {
    return new DescendingSortedIterator();
  }

  /**
   * Similar to {@link #iterator()}, but with no particular order
   *
   * @return iterator with no sorting
   */
  public IntIterator unsortedIterator()
  {
    return new UnsortedIterator();
  }

  /**
   * Returns the number of elements in this set (its cardinality).
   */
  @Override
  public int size()
  {
    return size;
  }

  /**
   * Returns <tt>true</tt> if this set contains no elements.
   */
  @Override
  public boolean isEmpty()
  {
    return size == 0;
  }

  /**
   * Compute the index of the element
   *
   * @param o element to search
   *
   * @return index of the element in {@link #cells}
   */
  private final int toIndex(int o)
  {
    return (o & 0x7FFFFFFF) % cells.length;
  }

  /**
   * Find position of the integer in {@link #cells}. If not found, returns the
   * first empty cell.
   *
   * @param element element to search
   *
   * @return if <code>returned value >=0</code>, it returns the index of the
   * element; if <code>returned value <0</code>, the index of the
   * first empty cell is <code>-(returned value - 1)</code>
   */
  private int findElementOrEmpty(int element)
  {
    assert element >= 0;
    int index = toIndex(IntHashCode.hashCode(element));
    int offset = 1;

    while (cells[index] != EMPTY) {
      // element found!
      if (cells[index] == element) {
        return index;
      }

      // compute the next index to check
      index = toIndex(index + offset);
      offset <<= 1;
      offset++;
      if (offset < 0) {
        offset = 2;
      }
    }

    // element not found!
    return -(index + 1);
  }

  /**
   * Find position of the integer in {@link #cells}. If not found, returns the
   * first removed cell.
   *
   * @param element element to search
   *
   * @return if <code>returned value >=0</code>, it returns the index of the
   * element; if <code>returned value <0</code>, the index of the
   * first empty cell is <code>-(returned value - 1)</code>
   */
  private int findElementOrRemoved(int element)
  {
    assert element >= 0;
    int index = toIndex(IntHashCode.hashCode(element));
    int offset = 1;
    int removed = -1;

    while (cells[index] != EMPTY) {
      // element found!
      if (cells[index] == element) {
        return index;
      }

      // remember the last removed cell if we don't find the element
      if (cells[index] == REMOVED) {
        removed = index;
      }

      index = toIndex(index + offset);
      offset <<= 1;
      offset++;
      if (offset < 0) {
        offset = 2;
      }
    }
    if (removed >= 0) {
      return -(removed + 1);
    }
    return index;
  }

  /**
   * Returns <tt>true</tt> if this set contains the specified element.
   *
   * @param element element whose presence in this set is to be tested.
   *
   * @return <tt>true</tt> if this set contains the specified element.
   */
  @Override
  public boolean contains(int element)
  {
    if (element < 0) {
      throw new IndexOutOfBoundsException("element < 0: " + element);
    }
    if (isEmpty()) {
      return false;
    }
    return findElementOrEmpty(element) >= 0;
  }

  /**
   * Adds the specified element to this set if it is not already present.
   *
   * @param element element to be added to this set.
   *
   * @return <tt>true</tt> if the set did not already contain the specified
   * element.
   */
  @Override
  public boolean add(int element)
  {
    if (element < 0) {
      throw new IndexOutOfBoundsException("element < 0: " + element);
    }
    int index = findElementOrRemoved(element);
    if (index >= 0) {
      if (cells[index] == element) {
        return false;
      }
      freecells--;
    } else {
      index = -(index + 1);
    }

    modCount++;
    size++;

    // set the integer
    cells[index] = element;

    // do we need to rehash?
    if (1 - ((double) freecells / cells.length) > LOAD_FACTOR) {
      rehash();
    }
    return true;
  }

  /**
   * Removes the specified element from the set.
   */
  @Override
  public boolean remove(int element)
  {
    if (element < 0) {
      throw new IndexOutOfBoundsException("element < 0: " + element);
    }
    int index = findElementOrEmpty(element);
    if (index < 0) {
      return false;
    }

    cells[index] = REMOVED;
    modCount++;
    size--;
    return true;
  }

  /**
   * Removes all of the elements from this set.
   */
  @Override
  public void clear()
  {
    size = 0;
    Arrays.fill(cells, EMPTY);
    freecells = cells.length;
    modCount++;
  }

  /**
   * Figures out correct size for rehashed set, then does the rehash.
   */
  protected void rehash()
  {
    // do we need to increase capacity, or are there so many
    // deleted objects hanging around that rehashing to the same
    // size is sufficient? if 5% (arbitrarily chosen number) of
    // cells can be freed up by a rehash, we do it.

    int gargagecells = cells.length - (size + freecells);
    if ((double) gargagecells / cells.length > 0.05D)
    // rehash with same size
    {
      rehash(cells.length);
    } else
    // rehash with increased capacity
    {
      rehash((cells.length << 1) + 1);
    }
  }

  /**
   * Rehashes to a bigger size.
   */
  protected void rehash(int newCapacity)
  {
    HashIntSet rehashed = new HashIntSet(newCapacity);
    @SuppressWarnings("hiding")
    int[] cells = rehashed.cells;
    for (int element : this.cells) {
      if (element < 0)
      // removed or empty
      {
        continue;
      }

      // add the element
      cells[-(rehashed.findElementOrEmpty(element) + 1)] = element;
    }
    this.cells = cells;
    freecells = newCapacity - size;
    modCount++;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addAll(IntSet c)
  {
    if (c == null || c.isEmpty()) {
      return false;
    }
    IntIterator itr;
    if (c instanceof HashIntSet) {
      itr = ((HashIntSet) c).unsortedIterator();
    } else {
      itr = c.iterator();
    }
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
    IntIterator itr;
    if (c instanceof HashIntSet) {
      itr = ((HashIntSet) c).unsortedIterator();
    } else {
      itr = c.iterator();
    }
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
    boolean res = false;
    for (int i = 0; i < cells.length; i++) {
      if (cells[i] >= 0 && !c.contains(cells[i])) {
        cells[i] = REMOVED;
        res = true;
        size--;
      }
    }
    if (res) {
      modCount++;
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HashIntSet clone()
  {
    HashIntSet cloned = new HashIntSet(cells.length);
    System.arraycopy(cells, 0, cloned.cells, 0, cells.length);
    cloned.freecells = freecells;
    cloned.size = size;
    cloned.modCount = 0;
    return cloned;
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
    return cells.length / Math.ceil(last() / 32D);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double collectionCompressionRatio()
  {
    return isEmpty() ? 0D : (double) cells.length / size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HashIntSet complemented()
  {
    return (HashIntSet) super.complemented();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAll(IntSet c)
  {
    IntIterator itr;
    if (c instanceof HashIntSet) {
      itr = ((HashIntSet) c).unsortedIterator();
    } else {
      itr = c.iterator();
    }
    boolean res = true;
    while (res && itr.hasNext()) {
      res &= contains(itr.next());
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAny(IntSet c)
  {
    IntIterator itr;
    if (c instanceof HashIntSet) {
      itr = ((HashIntSet) c).unsortedIterator();
    } else {
      itr = c.iterator();
    }
    boolean res = true;
    while (res && itr.hasNext()) {
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
    IntIterator itr;
    if (c instanceof HashIntSet) {
      itr = ((HashIntSet) c).unsortedIterator();
    } else {
      itr = c.iterator();
    }
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
  public HashIntSet convert(int... a)
  {
    HashIntSet res = new HashIntSet((int) (a.length / LOAD_FACTOR) + 1);
    for (int e : a) {
      res.add(e);
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HashIntSet convert(Collection<Integer> c)
  {
    HashIntSet res = new HashIntSet((int) (c.size() / LOAD_FACTOR) + 1);
    for (int e : c) {
      res.add(e);
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String debugInfo()
  {
    return "size: " + size + ", freecells: " + freecells + ", "
           + Arrays.toString(cells);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HashIntSet symmetricDifference(IntSet c)
  {
    HashIntSet res = clone();
    IntIterator itr;
    if (c instanceof HashIntSet) {
      itr = ((HashIntSet) c).unsortedIterator();
    } else {
      itr = c.iterator();
    }
    while (itr.hasNext()) {
      res.flip(itr.next());
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HashIntSet union(IntSet other)
  {
    return (HashIntSet) super.union(other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HashIntSet difference(IntSet other)
  {
    return (HashIntSet) super.difference(other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HashIntSet intersection(IntSet other)
  {
    return (HashIntSet) super.intersection(other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HashIntSet empty()
  {
    return new HashIntSet();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flip(int element)
  {
    if (element < 0) {
      throw new IndexOutOfBoundsException("element < 0: " + element);
    }
    modCount++;
    int index = findElementOrRemoved(element);
    if (index >= 0) {
      // REMOVE
      if (cells[index] == element) {
        cells[index] = REMOVED;
        size--;
        return;
      }
      freecells--;
    } else {
      index = -(index + 1);
    }

    // ADD
    cells[index] = element;
    size++;

    // do we need to rehash?
    if (1 - ((double) freecells / cells.length) > LOAD_FACTOR) {
      rehash();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int get(int i)
  {
    return toArray()[i];
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
    return Arrays.binarySearch(toArray(), e);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int intersectionSize(IntSet c)
  {
    int res = 0;
    IntIterator itr;
    if (c instanceof HashIntSet) {
      itr = ((HashIntSet) c).unsortedIterator();
    } else {
      itr = c.iterator();
    }
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
  public int last()
  {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    int max = 0;
    for (int element : cells) {
      if (max < element) {
        max = element;
      }
    }
    return max;
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
    int min = Integer.MAX_VALUE;
    for (int element : cells) {
      if (element >= 0 && min > element) {
        min = element;
      }
    }
    return min;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int[] toArray(int[] a)
  {
    if (a.length < size) {
      throw new IllegalArgumentException();
    }
    if (isEmpty()) {
      return a;
    }
    int i = 0;
    for (int element : this.cells) {
      if (element < 0)
      // removed or empty
      {
        continue;
      }

      // copy the element
      a[i++] = element;
    }
    Arrays.sort(a, 0, size);
    return a;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString()
  {
    return Arrays.toString(toArray());
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
    for (int e : cells) {
      if (e >= 0) {
        h ^= IntHashCode.hashCode(e);
      }
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
    if (!(obj instanceof HashIntSet)) {
      return super.equals(obj);
    }
    final HashIntSet other = (HashIntSet) obj;
    if (size != other.size) {
      return false;
    }
    for (int e : other.cells) {
      if (e >= 0 && !contains(e)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Iterates over the hashset, with no sorting
   */
  private class UnsortedIterator implements IntIterator
  {
    private int nextIndex = 0;
    private int current = -1;
    private int expectedModCount = modCount;

    public UnsortedIterator()
    {
      nextIndex = 0;
      skipEmpty();
      expectedModCount = modCount;
    }

    void skipEmpty()
    {
      while (nextIndex < cells.length
             && (cells[nextIndex] == EMPTY || cells[nextIndex] == REMOVED)) {
        nextIndex++;
      }
    }

    @Override
    public boolean hasNext()
    {
      return nextIndex < cells.length;
    }

    @Override
    public int next()
    {
      if (modCount != expectedModCount) {
        throw new ConcurrentModificationException();
      }
      if (nextIndex >= cells.length) {
        throw new NoSuchElementException();
      }

      current = nextIndex;
      nextIndex++;
      skipEmpty();
      return cells[current];
    }

    @Override
    public void remove()
    {
      if (modCount != expectedModCount) {
        throw new ConcurrentModificationException();
      }
      if (current < 0) {
        throw new IllegalStateException();
      }
      // delete object
      cells[current] = REMOVED;
      size--;
      modCount++;
      expectedModCount = modCount; // this is expected!
      current = -1;
    }

    @Override
    public void skipAllBefore(int element)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public IntIterator clone()
    {
      UnsortedIterator retVal = new UnsortedIterator();
      retVal.nextIndex = nextIndex;
      retVal.current = current;
      retVal.expectedModCount = expectedModCount;
      return retVal;
    }
  }

  /**
   * Iterates over the hashset, with no sorting
   */
  private class SortedIterator implements IntIterator
  {
    int[] elements = toArray();
    int next = 0;

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
      if (elements[next - 1] == REMOVED) {
        throw new IllegalStateException();
      }
      HashIntSet.this.remove(elements[next - 1]);
      elements[next - 1] = REMOVED;
    }

    @Override
    public void skipAllBefore(int element)
    {
      if (element <= elements[next]) {
        return;
      }
      next = Arrays.binarySearch(elements, next + 1, size, element);
      if (next < 0) {
        next = -(next + 1);
      }
    }

    @Override
    public IntIterator clone()
    {
      SortedIterator retVal = new SortedIterator();
      retVal.next = next;
      retVal.elements = elements.clone();
      return retVal;
    }
  }

  /**
   * Iterates over the hashset, with no sorting
   */
  private class DescendingSortedIterator implements IntIterator
  {
    int[] elements = toArray();
    int next = size - 1;

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
      if (elements[next + 1] == REMOVED) {
        throw new IllegalStateException();
      }
      HashIntSet.this.remove(elements[next + 1]);
      elements[next + 1] = REMOVED;
    }

    @Override
    public void skipAllBefore(int element)
    {
      if (element >= elements[next]) {
        return;
      }
      next = Arrays.binarySearch(elements, 0, next, element);
      if (next < 0) {
        next = -(next + 1) - 1;
      }
    }

    @Override
    public IntIterator clone()
    {
      DescendingSortedIterator retVal = new DescendingSortedIterator();
      retVal.elements = elements.clone();
      retVal.next = next;
      return retVal;
    }
  }
}
