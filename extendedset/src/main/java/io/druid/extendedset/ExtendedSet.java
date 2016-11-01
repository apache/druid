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


package io.druid.extendedset;


import io.druid.extendedset.intset.ArraySet;
import io.druid.extendedset.intset.IntSet;
import io.druid.extendedset.wrappers.GenericExtendedSet;
import io.druid.extendedset.wrappers.IndexedSet;
import io.druid.extendedset.wrappers.IntegerSet;
import io.druid.extendedset.wrappers.LongSet;
import io.druid.extendedset.wrappers.matrix.PairSet;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

/**
 * An interface which extends {@link SortedSet} by adding
 * intersection/union/difference and other set operations.
 *
 * @param <T> the type of elements maintained by this set
 *
 * @author Alessandro Colantonio
 * @version $Id: ExtendedSet.java 140 2011-02-07 21:30:29Z cocciasik $
 * @see AbstractExtendedSet
 * @see IndexedSet
 * @see GenericExtendedSet
 * @see ArraySet
 * @see IntegerSet
 * @see LongSet
 * @see PairSet
 */
public interface ExtendedSet<T> extends SortedSet<T>, Cloneable, Comparable<ExtendedSet<T>>
{
  /**
   * Generates the intersection set
   *
   * @param other {@link ExtendedSet} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #retainAll(java.util.Collection)
   */
  public ExtendedSet<T> intersection(Collection<? extends T> other);

  /**
   * Generates the union set
   *
   * @param other {@link ExtendedSet} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #addAll(java.util.Collection)
   */
  public ExtendedSet<T> union(Collection<? extends T> other);

  /**
   * Generates the difference set
   *
   * @param other {@link ExtendedSet} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #removeAll(java.util.Collection)
   */
  public ExtendedSet<T> difference(Collection<? extends T> other);

  /**
   * Generates the symmetric difference set
   *
   * @param other {@link ExtendedSet} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #flip(Object)
   */
  public ExtendedSet<T> symmetricDifference(Collection<? extends T> other);

  /**
   * Generates the complement set. The returned set is represented by all the
   * elements strictly less than {@link #last()} that do not exist in the
   * current set.
   *
   * @return the complement set
   *
   * @see ExtendedSet#complement()
   */
  public ExtendedSet<T> complemented();

  /**
   * Complements the current set. The modified set is represented by all the
   * elements strictly less than {@link #last()} that do not exist in the
   * current set.
   *
   * @see ExtendedSet#complemented()
   */
  public void complement();

  /**
   * Returns <code>true</code> if the specified {@link Collection} instance
   * contains any elements that are also contained within this
   * {@link ExtendedSet} instance
   *
   * @param other {@link ExtendedSet} to intersect with
   *
   * @return a boolean indicating whether this {@link ExtendedSet} intersects
   * the specified {@link ExtendedSet}.
   */
  public boolean containsAny(Collection<? extends T> other);

  /**
   * Returns <code>true</code> if the specified {@link Collection} instance
   * contains at least <code>minElements</code> elements that are also
   * contained within this {@link ExtendedSet} instance
   *
   * @param other       {@link Collection} instance to intersect with
   * @param minElements minimum number of elements to be contained within this
   *                    {@link ExtendedSet} instance
   *
   * @return a boolean indicating whether this {@link ExtendedSet} intersects
   * the specified {@link Collection}.
   *
   * @throws IllegalArgumentException if <code>minElements &lt; 1</code>
   */
  public boolean containsAtLeast(Collection<? extends T> other, int minElements);

  /**
   * Computes the intersection set size.
   * <p>
   * This is faster than calling {@link #intersection(Collection)} and
   * then {@link #size()}
   *
   * @param other {@link Collection} instance that represents the right
   *              operand
   *
   * @return the size
   */
  public int intersectionSize(Collection<? extends T> other);

  /**
   * Computes the union set size.
   * <p>
   * This is faster than calling {@link #union(Collection)} and then
   * {@link #size()}
   *
   * @param other {@link Collection} instance that represents the right
   *              operand
   *
   * @return the size
   */
  public int unionSize(Collection<? extends T> other);

  /**
   * Computes the symmetric difference set size.
   * <p>
   * This is faster than calling
   * {@link #symmetricDifference(Collection)} and then {@link #size()}
   *
   * @param other {@link Collection} instance that represents the right
   *              operand
   *
   * @return the size
   */
  public int symmetricDifferenceSize(Collection<? extends T> other);

  /**
   * Computes the difference set size.
   * <p>
   * This is faster than calling {@link #difference(Collection)} and
   * then {@link #size()}
   *
   * @param other {@link Collection} instance that represents the right
   *              operand
   *
   * @return the size
   */
  public int differenceSize(Collection<? extends T> other);

  /**
   * Computes the complement set size.
   * <p>
   * This is faster than calling {@link #complemented()} and then
   * {@link #size()}
   *
   * @return the size
   */
  public int complementSize();

  /**
   * Generates an empty set
   *
   * @return the empty set
   */
  public ExtendedSet<T> empty();

  /**
   * See the <code>clone()</code> of {@link Object}
   *
   * @return cloned object
   */
  public ExtendedSet<T> clone();

  /**
   * Computes the compression factor of the equivalent bitmap representation
   * (1 means not compressed, namely a memory footprint similar to
   * {@link BitSet}, 2 means twice the size of {@link BitSet}, etc.)
   *
   * @return the compression factor
   */
  public double bitmapCompressionRatio();

  /**
   * Computes the compression factor of the equivalent integer collection (1
   * means not compressed, namely a memory footprint similar to
   * {@link ArrayList}, 2 means twice the size of {@link ArrayList}, etc.)
   *
   * @return the compression factor
   */
  public double collectionCompressionRatio();

  /**
   * {@inheritDoc}
   */
  @Override
  public ExtendedIterator<T> iterator();

  /**
   * Gets the descending order iterator over the elements of type
   * <code>T</code>
   *
   * @return descending iterator
   */
  public ExtendedIterator<T> descendingIterator();

  /**
   * Allows to use the Java "for-each" statement in descending order
   *
   * @return {@link Iterable} instance to iterate items in descending
   * order
   */
  public Iterable<T> descending();

  /**
   * Computes the power-set of the current set.
   * <p>
   * It is a particular implementation of the algorithm <i>Apriori</i> (see:
   * Rakesh Agrawal, Ramakrishnan Srikant, <i>Fast Algorithms for Mining
   * Association Rules in Large Databases</i>, in Proceedings of the
   * 20<sup>th</sup> International Conference on Very Large Data Bases,
   * p.487-499, 1994). The returned power-set does <i>not</i> contain the
   * empty set.
   * <p>
   * The subsets composing the powerset are returned in a list that is sorted
   * according to the lexicographical order provided by the sorted set.
   *
   * @return the power-set
   *
   * @see #powerSet(int, int)
   * @see #powerSetSize()
   */
  public List<? extends ExtendedSet<T>> powerSet();

  /**
   * Computes a subset of the power-set of the current set, composed by those
   * subsets that have cardinality between <code>min</code> and
   * <code>max</code>.
   * <p>
   * It is a particular implementation of the algorithm <i>Apriori</i> (see:
   * Rakesh Agrawal, Ramakrishnan Srikant, <i>Fast Algorithms for Mining
   * Association Rules in Large Databases</i>, in Proceedings of the
   * 20<sup>th</sup> International Conference on Very Large Data Bases,
   * p.487-499, 1994). The power-set does <i>not</i> contains the empty set.
   * <p>
   * The subsets composing the powerset are returned in a list that is sorted
   * according to the lexicographical order provided by the sorted set.
   *
   * @param min minimum subset size (greater than zero)
   * @param max maximum subset size
   *
   * @return the power-set
   *
   * @see #powerSet()
   * @see #powerSetSize(int, int)
   */
  public List<? extends ExtendedSet<T>> powerSet(int min, int max);

  /**
   * Computes the power-set size of the current set.
   * <p>
   * The power-set does <i>not</i> contains the empty set.
   *
   * @return the power-set size
   *
   * @see #powerSet()
   */
  public int powerSetSize();

  /**
   * Computes the power-set size of the current set, composed by those subsets
   * that have cardinality between <code>min</code> and <code>max</code>.
   * <p>
   * The returned power-set does <i>not</i> contain the empty set.
   *
   * @param min minimum subset size (greater than zero)
   * @param max maximum subset size
   *
   * @return the power-set size
   *
   * @see #powerSet(int, int)
   */
  public int powerSetSize(int min, int max);

  /**
   * Prints debug info about the given {@link ExtendedSet} implementation
   *
   * @return a string that describes the internal representation of the
   * instance
   */
  public String debugInfo();

  /**
   * Adds to the set all the elements between <code>first</code> and
   * <code>last</code>, both included. It supposes that there is an ordering
   * of the elements of type <code>T</code> and that the universe of all
   * possible elements is known.
   *
   * @param from first element
   * @param to   last element
   */
  public void fill(T from, T to);

  /**
   * Removes from the set all the elements between <code>first</code> and
   * <code>last</code>, both included. It supposes that there is an ordering
   * of the elements of type <code>T</code> and that the universe of all
   * possible elements is known.
   *
   * @param from first element
   * @param to   last element
   */
  public void clear(T from, T to);

  /**
   * Adds the element if it not existing, or removes it if existing
   *
   * @param e element to flip
   *
   * @see #symmetricDifference(Collection)
   */
  public void flip(T e);

  /**
   * Gets the read-only version of the current set
   *
   * @return the read-only version of the current set
   */
  public ExtendedSet<T> unmodifiable();

  /**
   * Gets the <code>i</code><sup>th</sup> element of the set
   *
   * @param i position of the element in the sorted set
   *
   * @return the <code>i</code><sup>th</sup> element of the set
   *
   * @throws IndexOutOfBoundsException if <code>i</code> is less than zero, or greater or equal to
   *                                   {@link #size()}
   */
  public T get(int i);

  /**
   * Provides position of element within the set.
   * <p>
   * It returns -1 if the element does not exist within the set.
   *
   * @param e element of the set
   *
   * @return the element position
   */
  public int indexOf(T e);

  /**
   * {@inheritDoc}
   */
  @Override
  public ExtendedSet<T> tailSet(T fromElement);

  /**
   * {@inheritDoc}
   */
  @Override
  public ExtendedSet<T> headSet(T toElement);

  /**
   * {@inheritDoc}
   */
  @Override
  public ExtendedSet<T> subSet(T fromElement, T toElement);

  /**
   * Converts a given {@link Collection} instance into an instance of the
   * current class. <b>NOTE:</b> when the collection is already an instance of
   * the current class, the method returns the collection itself.
   *
   * @param c collection to use to generate the new instance
   *
   * @return the converted collection
   *
   * @see #convert(Object...)
   */
  public ExtendedSet<T> convert(Collection<?> c);

  /**
   * Converts a given integer array into an instance of the current class
   *
   * @param e objects to use to generate the new instance
   *
   * @return the converted collection
   *
   * @see #convert(Collection)
   */
  public ExtendedSet<T> convert(Object... e);

  /**
   * Computes the Jaccard similarity coefficient between this set and the
   * given set.
   * <p>
   * The coefficient is defined as
   * <code>|A intersection B| / |A union B|</code>.
   *
   * @param other the other set
   *
   * @return the Jaccard similarity coefficient
   *
   * @see #jaccardDistance(ExtendedSet)
   */
  public double jaccardSimilarity(ExtendedSet<T> other);

  /**
   * Computes the Jaccard distance between this set and the given set.
   * <p>
   * The coefficient is defined as
   * <code>1 - </code> {@link #jaccardSimilarity(ExtendedSet)}.
   *
   * @param other the other set
   *
   * @return the Jaccard distance
   *
   * @see #jaccardSimilarity(ExtendedSet)
   */
  public double jaccardDistance(ExtendedSet<T> other);

  /**
   * Computes the weighted version of the Jaccard similarity coefficient
   * between this set and the given set.
   * <p>
   * The coefficient is defined as
   * <code>sum of min(A_i, B_i) / sum of max(A_i, B_i)</code>.
   * <p>
   * <b>NOTE:</b> <code>T</code> must be a number, namely one of
   * {@link Integer}, {@link Double}, {@link Float}, {@link Byte},
   * {@link Long}, {@link Short}.
   *
   * @param other the other set
   *
   * @return the weighted Jaccard similarity coefficient
   *
   * @throws IllegalArgumentException if <code>T</code> is not a number
   * @see #weightedJaccardDistance(ExtendedSet)
   */
  public double weightedJaccardSimilarity(ExtendedSet<T> other);

  /**
   * Computes the weighted version of the Jaccard distance between this set
   * and the given set.
   * <p>
   * The coefficient is defined as <code>1 - </code>
   * {@link #weightedJaccardSimilarity(ExtendedSet)}.
   * <p>
   * <b>NOTE:</b> <code>T</code> must be a number, namely one of
   * {@link Integer}, {@link Double}, {@link Float}, {@link Byte},
   * {@link Long}, {@link Short}.
   *
   * @param other the other set
   *
   * @return the weighted Jaccard distance
   *
   * @throws IllegalArgumentException if <code>T</code> is not a number
   * @see #weightedJaccardSimilarity(ExtendedSet)
   */
  public double weightedJaccardDistance(ExtendedSet<T> other);

  /**
   * Compares this object with the specified object for order. Returns a
   * negative integer, zero, or a positive integer as this object is less
   * than, equal to, or greater than the specified object. An {@link IntSet}
   * instance <code>A</code> is less than another {@link IntSet} instance
   * <code>B</code> if <code>B-A</code> (that is, the elements in
   * <code>B</code> that are not contained in <code>A</code>) contains at
   * least one element that is greater than all the elements in
   * <code>A-B</code>.
   * <p>
   * <p>
   * The implementor must ensure <tt>sgn(x.compareTo(y)) ==
   * -sgn(y.compareTo(x))</tt> for all <tt>x</tt> and <tt>y</tt>. (This
   * implies that <tt>x.compareTo(y)</tt> must throw an exception iff
   * <tt>y.compareTo(x)</tt> throws an exception.)
   * <p>
   * <p>
   * The implementor must also ensure that the relation is transitive:
   * <tt>(x.compareTo(y)&gt;0 &amp;&amp; y.compareTo(z)&gt;0)</tt> implies
   * <tt>x.compareTo(z)&gt;0</tt>.
   * <p>
   * <p>
   * Finally, the implementor must ensure that <tt>x.compareTo(y)==0</tt>
   * implies that <tt>sgn(x.compareTo(z)) == sgn(y.compareTo(z))</tt>, for all
   * <tt>z</tt>.
   * <p>
   * <p>
   * It is strongly recommended, but <i>not</i> strictly required that
   * <tt>(x.compareTo(y)==0) == (x.equals(y))</tt>. Generally speaking, any
   * class that implements the <tt>Comparable</tt> interface and violates this
   * condition should clearly indicate this fact. The recommended language is
   * "Note: this class has a natural ordering that is inconsistent with
   * equals."
   * <p>
   * <p>
   * In the foregoing description, the notation <tt>sgn(</tt><i>expression</i>
   * <tt>)</tt> designates the mathematical <i>signum</i> function, which is
   * defined to return one of <tt>-1</tt>, <tt>0</tt>, or <tt>1</tt> according
   * to whether the value of <i>expression</i> is negative, zero or positive.
   *
   * @param o the object to be compared.
   *
   * @return a negative integer, zero, or a positive integer as this object is
   * less than, equal to, or greater than the specified object.
   *
   * @throws ClassCastException if the specified object's type prevents it from being
   *                            compared to this object.
   */
  @Override
  public int compareTo(ExtendedSet<T> o);

  /**
   * Extended version of the {@link Iterator} interface that allows to "skip"
   * some elements of the set
   *
   * @param <X> the type of elements maintained by this set
   */
  public interface ExtendedIterator<X> extends Iterator<X>
  {
    /**
     * Skips all the elements before the the specified element, so that
     * {@link Iterator#next()} gives the given element or, if it does not
     * exist, the element immediately after according to the sorting
     * provided by this {@link SortedSet} instance.
     * <p>
     * If <code>element</code> is less than the next element, it does
     * nothing
     *
     * @param element first element to not skip
     */
    public void skipAllBefore(X element);
  }
}	


