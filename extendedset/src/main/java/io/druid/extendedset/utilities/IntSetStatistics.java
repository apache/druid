package io.druid.extendedset.utilities;

import io.druid.extendedset.intset.IntSet;

import java.util.Collection;
import java.util.Formatter;
import java.util.List;

/**
 * A wrapper class for classes that implement the  {@link IntSet}  interface to count method calls
 *
 * @author Alessandro Colantonio
 * @version $Id: IntSetStatistics.java 153 2011-05-30 16:39:57Z cocciasik $
 */
public class IntSetStatistics implements IntSet
{
  /**
   * @uml.property name="unionCount"
   */
  private static long unionCount = 0;

	
	/*
   * Monitored characteristics
	 */
  /**
   * @uml.property name="intersectionCount"
   */
  private static long intersectionCount = 0;
  /**
   * @uml.property name="differenceCount"
   */
  private static long differenceCount = 0;
  /**
   * @uml.property name="symmetricDifferenceCount"
   */
  private static long symmetricDifferenceCount = 0;
  /**
   * @uml.property name="complementCount"
   */
  private static long complementCount = 0;
  /**
   * @uml.property name="unionSizeCount"
   */
  private static long unionSizeCount = 0;
  /**
   * @uml.property name="intersectionSizeCount"
   */
  private static long intersectionSizeCount = 0;
  /**
   * @uml.property name="differenceSizeCount"
   */
  private static long differenceSizeCount = 0;
  /**
   * @uml.property name="symmetricDifferenceSizeCount"
   */
  private static long symmetricDifferenceSizeCount = 0;
  /**
   * @uml.property name="complementSizeCount"
   */
  private static long complementSizeCount = 0;
  /**
   * @uml.property name="equalsCount"
   */
  private static long equalsCount = 0;
  /**
   * @uml.property name="hashCodeCount"
   */
  private static long hashCodeCount = 0;
  /**
   * @uml.property name="containsAllCount"
   */
  private static long containsAllCount = 0;
  /**
   * @uml.property name="containsAnyCount"
   */
  private static long containsAnyCount = 0;
  /**
   * @uml.property name="containsAtLeastCount"
   */
  private static long containsAtLeastCount = 0;
  /**
   * instance to monitor
   *
   * @uml.property name="container"
   * @uml.associationEnd
   */
  private final IntSet container;
	
	
	/*
	 * Statistics getters
	 */

  /**
   * Wraps an {@link IntSet} instance with an {@link IntSetStatistics}
   * instance
   *
   * @param container {@link IntSet} to wrap
   */
  public IntSetStatistics(IntSet container)
  {
    this.container = extractContainer(container);
  }

  /**
   * @return number of union operations (i.e.,  {@link #addAll(IntSet)}  ,  {@link #union(IntSet)}  )
   *
   * @uml.property name="unionCount"
   */
  public static long getUnionCount() {return unionCount;}

  /**
   * @return number of intersection operations (i.e.,  {@link #retainAll(IntSet)}  ,  {@link #intersection(IntSet)}  )
   *
   * @uml.property name="intersectionCount"
   */
  public static long getIntersectionCount() {return intersectionCount;}

  /**
   * @return number of difference operations (i.e.,  {@link #removeAll(IntSet)}  ,  {@link #difference(IntSet)}  )
   *
   * @uml.property name="differenceCount"
   */
  public static long getDifferenceCount() {return differenceCount;}

  /**
   * @return number of symmetric difference operations (i.e.,  {@link #symmetricDifference(IntSet)}  )
   *
   * @uml.property name="symmetricDifferenceCount"
   */
  public static long getSymmetricDifferenceCount() {return symmetricDifferenceCount;}

  /**
   * @return number of complement operations (i.e.,  {@link #complement()}  ,  {@link #complemented()}  )
   *
   * @uml.property name="complementCount"
   */
  public static long getComplementCount() {return complementCount;}

  /**
   * @return cardinality of union operations (i.e.,  {@link #addAll(IntSet)}  ,  {@link #union(IntSet)}  )
   *
   * @uml.property name="unionSizeCount"
   */
  public static long getUnionSizeCount() {return unionSizeCount;}

  /**
   * @return cardinality of intersection operations (i.e.,  {@link #retainAll(IntSet)}  ,  {@link #intersection(IntSet)}  )
   *
   * @uml.property name="intersectionSizeCount"
   */
  public static long getIntersectionSizeCount() {return intersectionSizeCount;}

  /**
   * @return cardinality of difference operations (i.e.,  {@link #removeAll(IntSet)}  ,  {@link #difference(IntSet)}  )
   *
   * @uml.property name="differenceSizeCount"
   */
  public static long getDifferenceSizeCount() {return differenceSizeCount;}

  /**
   * @return cardinality of symmetric difference operations (i.e.,  {@link #symmetricDifference(IntSet)}  )
   *
   * @uml.property name="symmetricDifferenceSizeCount"
   */
  public static long getSymmetricDifferenceSizeCount() {return symmetricDifferenceSizeCount;}

  /**
   * @return cardinality of complement operations (i.e.,  {@link #complement()}  ,  {@link #complemented()}  )
   *
   * @uml.property name="complementSizeCount"
   */
  public static long getComplementSizeCount() {return complementSizeCount;}

  /**
   * @return number of equality check operations (i.e.,  {@link #equals(Object)}  )
   *
   * @uml.property name="equalsCount"
   */
  public static long getEqualsCount() {return equalsCount;}

  /**
   * @return number of hash code computations (i.e.,  {@link #hashCode()}  )
   *
   * @uml.property name="hashCodeCount"
   */
  public static long getHashCodeCount() {return hashCodeCount;}

  /**
   * @return number of  {@link #containsAll(IntSet)}  calls
   *
   * @uml.property name="containsAllCount"
   */
  public static long getContainsAllCount() {return containsAllCount;}

  /**
   * @return number of  {@link #containsAny(IntSet)}  calls
   *
   * @uml.property name="containsAnyCount"
   */
  public static long getContainsAnyCount() {return containsAnyCount;}

  /**
   * @return number of  {@link #containsAtLeast(IntSet, int)}  calls
   *
   * @uml.property name="containsAtLeastCount"
   */
  public static long getContainsAtLeastCount() {return containsAtLeastCount;}
	
	
	/*
	 * Other statistical methods
	 */

  /**
   * @return the sum of the cardinality of set operations
   */
  public static long getSizeCheckCount()
  {
    return getIntersectionSizeCount()
           +
           getUnionSizeCount()
           + getDifferenceSizeCount()
           + getSymmetricDifferenceSizeCount()
           + getComplementSizeCount();
  }

  /**
   * Resets all counters
   */
  public static void resetCounters()
  {
    unionCount = intersectionCount = differenceCount = symmetricDifferenceCount = complementCount =
    unionSizeCount = intersectionSizeCount = differenceSizeCount = symmetricDifferenceSizeCount = complementSizeCount =
    equalsCount = hashCodeCount = containsAllCount = containsAnyCount = containsAtLeastCount = 0;
  }

  /**
   * @return the summary information string
   */
  public static String summary()
  {
    final StringBuilder s = new StringBuilder();
    final Formatter f = new Formatter(s);

    f.format("unionCount: %d\n", Long.valueOf(unionCount));
    f.format("intersectionCount: %d\n", Long.valueOf(intersectionCount));
    f.format("differenceCount: %d\n", Long.valueOf(differenceCount));
    f.format("symmetricDifferenceCount: %d\n", Long.valueOf(symmetricDifferenceCount));
    f.format("complementCount: %d\n", Long.valueOf(complementCount));
    f.format("unionSizeCount: %d\n", Long.valueOf(unionSizeCount));
    f.format("intersectionSizeCount: %d\n", Long.valueOf(intersectionSizeCount));
    f.format("differenceSizeCount: %d\n", Long.valueOf(differenceSizeCount));
    f.format("symmetricDifferenceSizeCount: %d\n", Long.valueOf(symmetricDifferenceSizeCount));
    f.format("complementSizeCount: %d\n", Long.valueOf(complementSizeCount));
    f.format("equalsCount: %d\n", Long.valueOf(equalsCount));
    f.format("hashCodeCount: %d\n", Long.valueOf(hashCodeCount));
    f.format("containsAllCount: %d\n", Long.valueOf(containsAllCount));
    f.format("containsAnyCount: %d\n", Long.valueOf(containsAnyCount));
    f.format("containsAtLeastCount: %d\n", Long.valueOf(containsAtLeastCount));

    return s.toString();
  }

  /**
   * Removes the {@link IntSetStatistics} wrapper
   *
   * @param c
   *
   * @return the contained {@link IntSet} instance
   */
  public static IntSet extractContainer(IntSet c)
  {
    if (c instanceof IntSetStatistics) {
      return extractContainer(((IntSetStatistics) c).container);
    }
    return c;
  }
	
	/*
	 * MONITORED METHODS
	 */

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addAll(IntSet c)
  {
    unionCount++;
    return container.addAll(extractContainer(c));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet union(IntSet other)
  {
    unionCount++;
    return new IntSetStatistics(container.union(extractContainer(other)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean retainAll(IntSet c)
  {
    intersectionCount++;
    return container.retainAll(extractContainer(c));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet intersection(IntSet other)
  {
    intersectionCount++;
    return new IntSetStatistics(container.intersection(extractContainer(other)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean removeAll(IntSet c)
  {
    differenceCount++;
    return container.removeAll(extractContainer(c));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet difference(IntSet other)
  {
    differenceCount++;
    return new IntSetStatistics(container.difference(extractContainer(other)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet symmetricDifference(IntSet other)
  {
    symmetricDifferenceCount++;
    return container.symmetricDifference(extractContainer(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void complement()
  {
    complementCount++;
    container.complement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet complemented()
  {
    complementCount++;
    return new IntSetStatistics(container.complemented());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int unionSize(IntSet other)
  {
    unionSizeCount++;
    return container.unionSize(extractContainer(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int intersectionSize(IntSet other)
  {
    intersectionSizeCount++;
    return container.intersectionSize(extractContainer(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int differenceSize(IntSet other)
  {
    differenceSizeCount++;
    return container.differenceSize(extractContainer(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int symmetricDifferenceSize(IntSet other)
  {
    symmetricDifferenceSizeCount++;
    return container.symmetricDifferenceSize(extractContainer(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int complementSize()
  {
    complementSizeCount++;
    return container.complementSize();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAll(IntSet c)
  {
    containsAllCount++;
    return container.containsAll(extractContainer(c));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAny(IntSet other)
  {
    containsAnyCount++;
    return container.containsAny(extractContainer(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAtLeast(IntSet other, int minElements)
  {
    containsAtLeastCount++;
    return container.containsAtLeast(extractContainer(other), minElements);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    hashCodeCount++;
    return container.hashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object obj)
  {
    equalsCount++;
    return obj != null
           && ((obj instanceof IntSetStatistics)
               ? container.equals(extractContainer((IntSetStatistics) obj))
               : container.equals(obj));
  }

	/*
	 * SIMPLE REDIRECTION
	 */

  /**
   * {@inheritDoc}
   */
  @Override
  public double bitmapCompressionRatio() {return container.bitmapCompressionRatio();}

  /**
   * {@inheritDoc}
   */
  @Override
  public double collectionCompressionRatio() {return container.collectionCompressionRatio();}

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear(int from, int to) {container.clear(from, to);}

  /**
   * {@inheritDoc}
   */
  @Override
  public void fill(int from, int to) {container.fill(from, to);}

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear() {container.clear();}

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean add(int i) {return container.add(i);}

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(int i) {return container.remove(i);}

  /**
   * {@inheritDoc}
   */
  @Override
  public void flip(int e) {container.flip(e);}

  /**
   * {@inheritDoc}
   */
  @Override
  public int get(int i) {return container.get(i);}

  /**
   * {@inheritDoc}
   */
  @Override
  public int indexOf(int e) {return container.indexOf(e);}

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean contains(int i) {return container.contains(i);}

  /**
   * {@inheritDoc}
   */
  @Override
  public int first() {return container.first();}

  /**
   * {@inheritDoc}
   */
  @Override
  public int last() {return container.last();}

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty() {return container.isEmpty();}

  /**
   * {@inheritDoc}
   */
  @Override
  public int size() {return container.size();}

  /**
   * {@inheritDoc}
   */
  @Override
  public IntIterator iterator() {return container.iterator();}

  /**
   * {@inheritDoc}
   */
  @Override
  public IntIterator descendingIterator() {return container.descendingIterator();}

  /**
   * {@inheritDoc}
   */
  @Override
  public int[] toArray() {return container.toArray();}

  /**
   * {@inheritDoc}
   */
  @Override
  public int[] toArray(int[] a) {return container.toArray(a);}

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(IntSet o) {return container.compareTo(o);}

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {return container.toString();}

  /**
   * {@inheritDoc}
   */
  @Override
  public List<? extends IntSet> powerSet() {return container.powerSet();}

  /**
   * {@inheritDoc}
   */
  @Override
  public List<? extends IntSet> powerSet(int min, int max) {return container.powerSet(min, max);}

  /**
   * {@inheritDoc}
   */
  @Override
  public int powerSetSize() {return container.powerSetSize();}

  /**
   * {@inheritDoc}
   */
  @Override
  public int powerSetSize(int min, int max) {return container.powerSetSize(min, max);}

  /**
   * {@inheritDoc}
   */
  @Override
  public double jaccardSimilarity(IntSet other) {return container.jaccardSimilarity(other);}

  /**
   * {@inheritDoc}
   */
  @Override
  public double jaccardDistance(IntSet other) {return container.jaccardDistance(other);}

  /**
   * {@inheritDoc}
   */
  @Override
  public double weightedJaccardSimilarity(IntSet other) {return container.weightedJaccardSimilarity(other);}

  /**
   * {@inheritDoc}
   */
  @Override
  public double weightedJaccardDistance(IntSet other) {return container.weightedJaccardDistance(other);}
	
	/*
	 * OTHERS
	 */

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet empty() {return new IntSetStatistics(container.empty());}

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet clone() {return new IntSetStatistics(container.clone());}

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet convert(int... a) {return new IntSetStatistics(container.convert(a));}

  /**
   * {@inheritDoc}
   */
  @Override
  public IntSet convert(Collection<Integer> c) {return new IntSetStatistics(container.convert(c));}

  /**
   * {@inheritDoc}
   */
  @Override
  public String debugInfo() {return "Analyzed IntSet:\n" + container.debugInfo();}
}
