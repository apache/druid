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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An  {@link ExtendedSet}  implementation that maps each element of the universe (i.e., the collection of all possible elements) to an integer referred to as its "index".
 *
 * @param < T  >  the type of elements maintained by this set
 *
 * @author Alessandro Colantonio
 * @version $Id: IndexedSet.java 154 2011-05-30 22:19:24Z cocciasik $
 * @see ExtendedSet
 * @see AbstractExtendedSet
 */
public class IndexedSet<T> extends AbstractExtendedSet<T> implements java.io.Serializable
{
  /**
   * generated serial ID
   */
  private static final long serialVersionUID = -2386771695765773453L;

  // indices
  /**
   * @uml.property name="indices"
   * @uml.associationEnd
   */
  private final IntSet indices;

  // mapping to translate items to indices and vice-versa
  private final Map<T, Integer> itemToIndex;
  private final T[] indexToItem;

  /**
   * Creates an empty {@link IndexedSet} based on a given collection that
   * represents the set of <i>all</i> possible items that can be added to the
   * {@link IndexedSet} instance.
   * <p>
   * <b>VERY IMPORTANT!</b> to correctly work and effectively reduce the
   * memory allocation, new instances of {@link IndexedSet} <i>must</i> be
   * created through the {@link #clone()} or {@link #empty()} methods and
   * <i>not</i> by calling many times this constructor with the same
   * collection for <code>universe</code>!
   *
   * @param indices  {@link IntSet} instance used for internal representation
   * @param universe collection of <i>all</i> possible items. Order will be
   *                 preserved.
   */
  @SuppressWarnings("unchecked")
  public IndexedSet(IntSet indices, final Collection<T> universe)
  {
    // NOTE: this procedure removes duplicates while keeping the order
    indexToItem = universe instanceof Set ? (T[]) universe.toArray() : (T[]) (new LinkedHashSet<T>(universe)).toArray();
    itemToIndex = new HashMap<T, Integer>(Math.max((int) (indexToItem.length / .75f) + 1, 16));
    for (int i = 0; i < indexToItem.length; i++) {
      itemToIndex.put(indexToItem[i], Integer.valueOf(i));
    }
    this.indices = indices;
  }

  /**
   * Creates a {@link IndexedSet} instance from a given universe
   * mapping
   *
   * @param itemToIndex universe item-to-index mapping
   * @param indexToItem universe index-to-item mapping
   * @param indices     initial item set
   */
  private IndexedSet(Map<T, Integer> itemToIndex, T[] indexToItem, IntSet indices)
  {
    this.itemToIndex = itemToIndex;
    this.indexToItem = indexToItem;
    this.indices = indices;
  }

  /**
   * A shortcut for <code>new IndexedSet&lt;T&gt;(itemToIndex, indexToItem, indices)</code>
   */
  private IndexedSet<T> createFromIndices(IntSet indx)
  {
    return new IndexedSet<T>(itemToIndex, indexToItem, indx);
  }

  /**
   * Checks if the given collection is a instance of {@link IndexedSet} with
   * the same index mappings
   *
   * @param c collection to check
   *
   * @return <code>true</code> if the given collection is a instance of
   * {@link IndexedSet} with the same index mappings
   */
  private boolean hasSameIndices(Collection<?> c)
  {
    // since indices are always re-created through constructor and
    // referenced through clone(), it is sufficient to check just only one
    // mapping table
    return (c instanceof IndexedSet) && (indexToItem == ((IndexedSet) c).indexToItem);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IndexedSet<T> clone()
  {
    return createFromIndices(indices.clone());
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
    if (obj == null || !(obj instanceof Collection<?>)) {
      return false;
    }
    IndexedSet<?> other = convert((Collection<?>) obj);
    return this.indexToItem == other.indexToItem
           && this.itemToIndex == other.itemToIndex
           && this.indices.equals(other.indices);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    return indices.hashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(ExtendedSet<T> o)
  {
    return indices.compareTo(convert(o).indices);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Comparator<? super T> comparator()
  {
    return new Comparator<T>()
    {
      @Override
      public int compare(T o1, T o2)
      {
        // compare elements according to the universe ordering
        return itemToIndex.get(o1).compareTo(itemToIndex.get(o2));
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T first()
  {
    return indexToItem[indices.first()];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T last()
  {
    return indexToItem[indices.last()];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean add(T e)
  {
    Integer index = itemToIndex.get(e);
    if (index == null) {
      throw new IllegalArgumentException("element not in the current universe");
    }
    return indices.add(index.intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addAll(Collection<? extends T> c)
  {
    return c != null && !c.isEmpty() && indices.addAll(convert(c).indices);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear()
  {
    indices.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flip(T e)
  {
    indices.flip(itemToIndex.get(e).intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean contains(Object o)
  {
    if (o == null) {
      return false;
    }
    Integer index = itemToIndex.get(o);
    return index != null && indices.contains(index.intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAll(Collection<?> c)
  {
    return c == null || indices.containsAll(convert(c).indices);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAny(Collection<? extends T> other)
  {
    return other == null || indices.containsAny(convert(other).indices);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAtLeast(Collection<? extends T> other, int minElements)
  {
    return other != null && !other.isEmpty() && indices.containsAtLeast(convert(other).indices, minElements);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty()
  {
    return indices.isEmpty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExtendedIterator<T> iterator()
  {
    return new ExtendedIterator<T>()
    {
      final IntSet.IntIterator itr = indices.iterator();

      @Override
      public boolean hasNext() {return itr.hasNext();}

      @Override
      public T next() {return indexToItem[itr.next()];}

      @Override
      public void skipAllBefore(T element) {itr.skipAllBefore(itemToIndex.get(element).intValue());}

      @Override
      public void remove() {itr.remove();}
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExtendedIterator<T> descendingIterator()
  {
    return new ExtendedIterator<T>()
    {
      final IntSet.IntIterator itr = indices.descendingIterator();

      @Override
      public boolean hasNext() {return itr.hasNext();}

      @Override
      public T next() {return indexToItem[itr.next()];}

      @Override
      public void skipAllBefore(T element) {itr.skipAllBefore(itemToIndex.get(element).intValue());}

      @Override
      public void remove() {itr.remove();}
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(Object o)
  {
    if (o == null) {
      return false;
    }
    Integer index = itemToIndex.get(o);
    return index != null && indices.remove(index.intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean removeAll(Collection<?> c)
  {
    return c != null && !c.isEmpty() && indices.removeAll(convert(c).indices);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean retainAll(Collection<?> c)
  {
    if (isEmpty()) {
      return false;
    }
    if (c == null || c.isEmpty()) {
      indices.clear();
      return true;
    }
    return indices.retainAll(convert(c).indices);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size()
  {
    return indices.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IndexedSet<T> intersection(Collection<? extends T> other)
  {
    if (other == null) {
      return empty();
    }
    return createFromIndices(indices.intersection(convert(other).indices));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IndexedSet<T> union(Collection<? extends T> other)
  {
    if (other == null) {
      return clone();
    }
    return createFromIndices(indices.union(convert(other).indices));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IndexedSet<T> difference(Collection<? extends T> other)
  {
    if (other == null) {
      return clone();
    }
    return createFromIndices(indices.difference(convert(other).indices));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IndexedSet<T> symmetricDifference(Collection<? extends T> other)
  {
    if (other == null) {
      return clone();
    }
    return createFromIndices(indices.symmetricDifference(convert(other).indices));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IndexedSet<T> complemented()
  {
    return createFromIndices(indices.complemented());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void complement()
  {
    indices.complement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int intersectionSize(Collection<? extends T> other)
  {
    if (other == null) {
      return 0;
    }
    return indices.intersectionSize(convert(other).indices);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int unionSize(Collection<? extends T> other)
  {
    if (other == null) {
      return size();
    }
    return indices.unionSize(convert(other).indices);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int symmetricDifferenceSize(Collection<? extends T> other)
  {
    if (other == null) {
      return size();
    }
    return indices.symmetricDifferenceSize(convert(other).indices);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int differenceSize(Collection<? extends T> other)
  {
    if (other == null) {
      return size();
    }
    return indices.differenceSize(convert(other).indices);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int complementSize()
  {
    return indices.complementSize();
  }

  /**
   * Returns the collection of all possible elements
   *
   * @return the collection of all possible elements
   */
  public IndexedSet<T> universe()
  {
    IntSet allItems = indices.empty();
    allItems.fill(0, indexToItem.length - 1);
    return createFromIndices(allItems);
  }

  /**
   * Returns the index of the given item
   *
   * @param item
   *
   * @return the index of the given item
   */
  public Integer absoluteIndexOf(T item)
  {
    return itemToIndex.get(item);
  }

  /**
   * Returns the item corresponding to the given index
   *
   * @param i index
   *
   * @return the item
   */
  public T absoluteGet(int i)
  {
    return indexToItem[i];
  }

  /**
   * Returns the set of indices. Modifications to this set are reflected to
   * this {@link IndexedSet} instance. Trying to perform operation on
   * out-of-bound indices will throw an {@link IllegalArgumentException}
   * exception.
   *
   * @return the index set
   *
   * @see #absoluteGet(int)
   * @see #absoluteIndexOf(Object)
   */
  public IntSet indices()
  {
    return indices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IndexedSet<T> empty()
  {
    return createFromIndices(indices.empty());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double bitmapCompressionRatio()
  {
    return indices.bitmapCompressionRatio();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double collectionCompressionRatio()
  {
    return indices.collectionCompressionRatio();
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public IndexedSet<T> convert(Collection<?> c)
  {
    if (c == null) {
      return empty();
    }

    // useless to convert...
    if (hasSameIndices(c)) {
      return (IndexedSet<T>) c;
    }

    // NOTE: cannot call super.convert(c) because of loop
    IndexedSet<T> res = empty();
    for (T t : (Collection<T>) c) {
      res.add(t);
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IndexedSet<T> convert(Object... e)
  {
    return (IndexedSet<T>) super.convert(e);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<? extends IndexedSet<T>> powerSet()
  {
    return powerSet(1, Integer.MAX_VALUE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<? extends IndexedSet<T>> powerSet(int min, int max)
  {
    List<? extends IntSet> ps = indices.powerSet(min, max);
    List<IndexedSet<T>> res = new ArrayList<IndexedSet<T>>(ps.size());
    for (IntSet s : ps) {
      res.add(createFromIndices(s));
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String debugInfo()
  {
    return String.format("items = %s\nitemToIndex = %s\nindexToItem = %s\n",
                         indices.debugInfo(), itemToIndex.toString(), Arrays.toString(indexToItem)
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double jaccardSimilarity(ExtendedSet<T> other)
  {
    return indices.jaccardSimilarity(convert(other).indices);
  }

  //TODO
//	/**
//	 * {@inheritDoc}
//	 */
//	@Override
//	public IndexedSet<T> unmodifiable() {
//		return createFromIndices(indices.unmodifiable());
//	}
//	
//	/**
//	 * {@inheritDoc}
//	 */
//	@Override
//	public IndexedSet<T> subSet(T fromElement, T toElement) {
//		return createFromIndices(indices.subSet(itemToIndex.get(fromElement), itemToIndex.get(toElement)));
//	}
//	
//	/**
//	 * {@inheritDoc}
//	 */
//	@Override
//	public IndexedSet<T> headSet(T toElement) {
//		return createFromIndices(indices.headSet(itemToIndex.get(toElement)));
//	}
//	
//	/**
//	 * {@inheritDoc}
//	 */
//	@Override
//	public IndexedSet<T> tailSet(T fromElement) {
//		return createFromIndices(indices.tailSet(itemToIndex.get(fromElement)));
//	}

  /**
   * {@inheritDoc}
   */
  @Override
  public T get(int i)
  {
    return indexToItem[indices.get(i)];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int indexOf(T e)
  {
    return indices.indexOf(itemToIndex.get(e).intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear(T from, T to)
  {
    indices.clear(itemToIndex.get(from).intValue(), itemToIndex.get(to).intValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void fill(T from, T to)
  {
    indices.fill(itemToIndex.get(from).intValue(), itemToIndex.get(to).intValue());
  }
}
