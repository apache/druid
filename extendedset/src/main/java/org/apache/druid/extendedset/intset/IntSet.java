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

package org.apache.druid.extendedset.intset;


import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @version $Id: IntSet.java 135 2011-01-04 15:54:48Z cocciasik $
 */
public interface IntSet extends Cloneable, Comparable<IntSet>
{

  /**
   * See the <code>clone()</code> of {@link Object}
   *
   * @return cloned object
   */
  IntSet clone();

  /**
   * @return a {@link IntIterator} instance to iterate over the set
   */
  IntIterator iterator();

  /**
   * @return a {@link IntIterator} instance to iterate over the set in
   * descending order
   */
  IntIterator descendingIterator();

  /**
   * @return the number of elements in this set (its cardinality)
   */
  int size();

  /**
   * @return <tt>true</tt> if this set contains no elements
   */
  boolean isEmpty();

  /**
   * An {@link Iterator}-like interface that allows to "skip" some elements of
   * the set
   */
  interface IntIterator extends org.roaringbitmap.IntIterator
  {
    /**
     * @return <tt>true</tt> if the iterator has more elements.
     */
    @Override
    boolean hasNext();

    /**
     * @return the next element in the iteration.
     *
     * @throws NoSuchElementException iteration has no more elements.
     */
    @Override
    int next();

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
    void skipAllBefore(int element);

    /**
     * Clone the iterator
     *
     * @return a clone of the IntIterator
     */
    @Override
    IntIterator clone();
  }
}
