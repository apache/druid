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

import io.druid.extendedset.ExtendedSet.ExtendedIterator;
import io.druid.extendedset.intset.AbstractIntSet;
import io.druid.extendedset.intset.ArraySet;
import io.druid.extendedset.intset.ConciseSet;
import io.druid.extendedset.intset.FastSet;
import io.druid.extendedset.intset.HashIntSet;
import io.druid.extendedset.intset.IntSet;
import io.druid.extendedset.utilities.IntSetStatistics;
import io.druid.extendedset.utilities.random.MersenneTwister;
import io.druid.extendedset.wrappers.GenericExtendedSet;
import io.druid.extendedset.wrappers.IndexedSet;
import io.druid.extendedset.wrappers.IntegerSet;
import io.druid.extendedset.wrappers.matrix.BinaryMatrix;
import io.druid.extendedset.wrappers.matrix.BinaryMatrix.CellIterator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

//import it.uniroma3.mat.extendedset.intset.Concise2Set;


/**
 * Test class for {@link ConciseSet}, {@link FastSet}, and {@link IndexedSet}.
 *
 * @author Alessandro Colantonio
 * @version $Id: Debug.java 155 2011-05-30 22:27:00Z cocciasik $
 */
public class Debug
{
  /**
   * Checks if a {@link ExtendedSet} instance and a {@link TreeSet} instance
   * contains the same elements. {@link TreeSet} is used because it is the
   * most similar class to {@link ExtendedSet}.
   *
   * @param <T>   type of elements within the set
   * @param bits  bit-set to check
   * @param items {@link TreeSet} instance that must contain the same elements
   *              of the bit-set
   *
   * @return <code>true</code> if the given {@link ConciseSet} and
   * {@link TreeSet} are equals in terms of contained elements
   */
  private static <T> boolean checkContent(ExtendedSet<T> bits, SortedSet<T> items)
  {
    if (bits.size() != items.size()) {
      return false;
    }
    if (bits.isEmpty() && items.isEmpty()) {
      return true;
    }
    for (T i : bits) {
      if (!items.contains(i)) {
        return false;
      }
    }
    for (T i : items) {
      if (!bits.contains(i)) {
        return false;
      }
    }
    if (!bits.last().equals(items.last())) {
      return false;
    }
    if (!bits.first().equals(items.first())) {
      return false;
    }
    return true;
  }

  /**
   * Generates an empty set of the specified class
   *
   * @param c the given class
   *
   * @return the empty set
   */
  private static <X extends Collection<Integer>> X empty(Class<X> c)
  {
    try {
      return c.newInstance();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Stress test for {@link ConciseSet#add(Integer)}
   * <p>
   * It starts from a very sparse set (most of the words will be 0's
   * sequences) and progressively become very dense (words first
   * become 0's sequences with 1 set bit and there will be almost one
   * word per item, then words become literals, and finally they
   * become 1's sequences and drastically reduce in number)
   */
  private static void testForAdditionStress(Class<? extends ExtendedSet<Integer>> c)
  {
    ExtendedSet<Integer> previousBits = empty(c);
    ExtendedSet<Integer> currentBits = empty(c);
    TreeSet<Integer> currentItems = new TreeSet<Integer>();

    Random rnd = new MersenneTwister();

    // add 100000 random numbers
    for (int i = 0; i < 100000; i++) {
      // random number to add
      int item = rnd.nextInt(10000 + 1);

      // keep the previous results
      previousBits = currentBits;
      currentBits = currentBits.clone();

      // add the element
      System.out.format("Adding %d...\n", item);
      boolean itemExistsBefore = currentItems.contains(item);
      boolean itemAdded = currentItems.add(item);
      boolean itemExistsAfter = currentItems.contains(item);
      boolean bitExistsBefore = currentBits.contains(item);
      boolean bitAdded = currentBits.add(item);
      boolean bitExistsAfter = currentBits.contains(item);
      if (itemAdded ^ bitAdded) {
        System.out.println("wrong add() result");
        return;
      }
      if (itemExistsBefore ^ bitExistsBefore) {
        System.out.println("wrong contains() before");
        return;
      }
      if (itemExistsAfter ^ bitExistsAfter) {
        System.out.println("wrong contains() after");
        return;
      }

      // check the list of elements
      if (!checkContent(currentBits, currentItems)) {
        System.out.println("add() error");
        System.out.println("Same elements: " + (currentItems.toString().equals(currentBits.toString())));
        System.out.println("\tcorrect: " + currentItems.toString());
        System.out.println("\twrong:   " + currentBits.toString());
        System.out.println("Original: " + currentItems);
        System.out.println(currentBits.debugInfo());
        System.out.println(previousBits.debugInfo());
        return;
      }

      // check the representation
      ExtendedSet<Integer> otherBits = previousBits.convert(currentItems);
      if (otherBits.hashCode() != currentBits.hashCode()) {
        System.out.println("Representation error");
        System.out.println(currentBits.debugInfo());
        System.out.println(otherBits.debugInfo());
        System.out.println(previousBits.debugInfo());
        return;
      }

      // check the union size
      ExtendedSet<Integer> singleBitSet = empty(c);
      singleBitSet.add(item);
      if (currentItems.size() != currentBits.unionSize(singleBitSet)) {
        System.out.println("Size error");
        System.out.println("Original: " + currentItems);
        System.out.println(currentBits.debugInfo());
        System.out.println(previousBits.debugInfo());
        return;
      }
    }

    System.out.println("Final");
    System.out.println(currentBits.debugInfo());

    System.out.println();
    System.out.println(IntSetStatistics.summary());
  }

  /**
   * Stress test for {@link ConciseSet#remove(Object)}
   * <p>
   * It starts from a very dense set (most of the words will be 1's
   * sequences) and progressively become very sparse (words first
   * become 1's sequences with 1 unset bit and there will be few
   * words per item, then words become literals, and finally they
   * become 0's sequences and drastically reduce in number)
   *
   * @param c class to test
   */
  private static void testForRemovalStress(Class<? extends ExtendedSet<Integer>> c)
  {
    ExtendedSet<Integer> previousBits = empty(c);
    ExtendedSet<Integer> currentBits = empty(c);
    TreeSet<Integer> currentItems = new TreeSet<Integer>();

    Random rnd = new MersenneTwister();

    // create a 1-filled bitset
    currentBits.add((1 << MatrixIntSet.COL_POW) * 5 - 1);
    currentBits.complement();
    currentItems.addAll(currentBits);
    if (currentItems.size() != (1 << MatrixIntSet.COL_POW) * 5 - 1) {
      System.out.println("Unexpected error!");
      System.out.println(currentBits.size());
      System.out.println(currentItems.size());
      return;
    }

    // remove 100000 random numbers
    for (int i = 0; i < 100000 & !currentBits.isEmpty(); i++) {
      // random number to remove
      int item = rnd.nextInt(10000 + 1);

      // keep the previous results
      previousBits = currentBits;
      currentBits = currentBits.clone();

      // remove the element
      System.out.format("Removing %d...\n", item);
      boolean itemExistsBefore = currentItems.contains(item);
      boolean itemRemoved = currentItems.remove(item);
      boolean itemExistsAfter = currentItems.contains(item);
      boolean bitExistsBefore = currentBits.contains(item);
      boolean bitRemoved = currentBits.remove(item);
      boolean bitExistsAfter = currentBits.contains(item);
      if (itemRemoved ^ bitRemoved) {
        System.out.println("wrong remove() result");
        return;
      }
      if (itemExistsBefore ^ bitExistsBefore) {
        System.out.println("wrong contains() before");
        return;
      }
      if (itemExistsAfter ^ bitExistsAfter) {
        System.out.println("wrong contains() after");
        return;
      }

      // check the list of elements
      if (!checkContent(currentBits, currentItems)) {
        System.out.println("remove() error");
        System.out.println("Same elements: " + (currentItems.toString().equals(currentBits.toString())));
        System.out.println("Original: " + currentItems);
        System.out.println(currentBits.debugInfo());
        System.out.println(previousBits.debugInfo());

        return;
      }

      // check the representation
      ExtendedSet<Integer> otherBits = empty(c);
      otherBits.addAll(currentItems);
      if (otherBits.hashCode() != currentBits.hashCode()) {
        System.out.println("Representation error");
        System.out.println(currentBits.debugInfo());
        System.out.println(otherBits.debugInfo());
        System.out.println(previousBits.debugInfo());

        return;
      }

      // check the union size
      ExtendedSet<Integer> singleBitSet = empty(c);
      singleBitSet.add(item);
      if (currentItems.size() != currentBits.differenceSize(singleBitSet)) {
        System.out.println("Size error");
        System.out.println("Original: " + currentItems);
        System.out.println(currentBits.debugInfo());
        System.out.println(previousBits.debugInfo());

        return;
      }
    }

    System.out.println("Final");
    System.out.println(currentBits.debugInfo());

    System.out.println();
    System.out.println(IntSetStatistics.summary());
  }

  /**
   * Random operations on random sets.
   * <p>
   * It randomly chooses among {@link ConciseSet#addAll(Collection)},
   * {@link ConciseSet#removeAll(Collection)}, and
   * {@link ConciseSet#retainAll(Collection)}, and perform the operation over
   * random sets
   *
   * @param c class to test
   */
  private static void testForRandomOperationsStress(Class<? extends ExtendedSet<Integer>> c, boolean testFillAndClear)
  {
    ExtendedSet<Integer> bitsLeft = empty(c);
    ExtendedSet<Integer> bitsRight = empty(c);
    SortedSet<Integer> itemsLeft = new TreeSet<Integer>();
    SortedSet<Integer> itemsRight = new TreeSet<Integer>();

    Random r = new MersenneTwister();
    final int maxCardinality = 1000;

    // random operation loop
    for (int i = 0; i < 1000000; i++) {
      System.out.format("Test %,d (%,d): ", i, System.currentTimeMillis());

      RandomNumbers rn;
      switch (r.nextInt(3)) {
        case 0:
          rn = new RandomNumbers.Uniform(
              r.nextInt(maxCardinality),
              r.nextDouble() * 0.999,
              r.nextInt(maxCardinality / 10)
          );
          break;
        case 1:
          rn = new RandomNumbers.Zipfian(
              r.nextInt(maxCardinality),
              r.nextDouble() * 0.9,
              r.nextInt(maxCardinality / 10),
              2
          );
          break;
        case 2:
          rn = new RandomNumbers.Markovian(
              r.nextInt(maxCardinality),
              r.nextDouble() * 0.999,
              r.nextInt(maxCardinality / 10)
          );
          break;
        default:
          throw new RuntimeException("unexpected");
      }

			/*
       * fill() and clear()
			 */
      if (testFillAndClear) {
        bitsRight.clear();
        itemsRight.clear();
        Iterator<Integer> itr1 = rn.generate().iterator();
        Iterator<Integer> itr2 = rn.generate().iterator();
        while (itr1.hasNext() && itr2.hasNext()) {
          ExtendedSet<Integer> clone = bitsRight.clone();
          Integer from = itr1.next();
          Integer to = itr2.next();
          if (from.compareTo(to) > 0) {
            Integer s = from;
            from = to;
            to = s;
          }

          boolean fill = r.nextBoolean();
          if (fill) {
            for (int j = from; j <= to; j++) {
              itemsRight.add(j);
            }
            bitsRight.fill(from, to);
          } else {
            for (int j = from; j <= to; j++) {
              itemsRight.remove(j);
            }
            bitsRight.clear(from, to);
          }

          if (!checkContent(bitsLeft, itemsLeft)) {
            System.out.println("FILL/CLEAR ERROR!");
            System.out.println("Same elements: " + (itemsLeft.toString().equals(bitsLeft.toString())));
            System.out.println("itemsLeft:");
            System.out.println(itemsLeft);
            System.out.println("bitsLeft:");
            System.out.println(bitsLeft.debugInfo());

            System.out.println("itemsLeft.size(): " + itemsLeft.size() + " ?= bitsLeft.size(): " + bitsLeft.size());
            for (Integer x : bitsLeft) {
              if (!itemsLeft.contains(x)) {
                System.out.println("itemsLeft does not contain " + x);
              }
            }
            for (Integer x : itemsLeft) {
              if (!bitsLeft.contains(x)) {
                System.out.println("itemsLeft does not contain " + x);
              }
            }
            System.out.println("bitsLeft.last(): " + bitsLeft.last() + " ?= itemsLeft.last(): " + itemsLeft.last());
            System.out.println("bitsLeft.first(): " + bitsLeft.first() + " ?= itemsLeft.first(): " + itemsLeft.first());

            return;
          }
          ExtendedSet<Integer> app = empty(c);
          app.addAll(itemsRight);
          if (bitsRight.hashCode() != app.hashCode()) {
            System.out.println("FILL/CLEAR FORMAT ERROR!");
            System.out.println("fill: " + fill);
            System.out.println("from " + from + " to " + to);
            System.out.println("itemsRight:");
            System.out.println(itemsRight);
            System.out.println("bitsRight:");
            System.out.println(bitsRight.debugInfo());
            System.out.println("Append:");
            System.out.println(app.debugInfo());
            System.out.println("Clone:");
            System.out.println(clone.debugInfo());
            return;
          }
        }
      }
			
			
			/*
			 * contains(), add(), and remove()
			 */
      bitsRight.clear();
      itemsRight.clear();
      for (Integer e : rn.generate()) {
        if (itemsRight.contains(e) ^ bitsRight.contains(e)) {
          System.out.println("CONTAINS ERROR!");
          System.out.println("itemsRight.contains(" + e + "): " + itemsRight.contains(e));
          System.out.println("bitsRight.contains(" + e + "): " + bitsRight.contains(e));
          System.out.println("itemsRight:");
          System.out.println(itemsRight);
          System.out.println("bitsRight:");
          System.out.println(bitsRight.debugInfo());
          return;
        }
        ExtendedSet<Integer> clone = bitsRight.clone();
        boolean resItems = itemsRight.add(e);
        boolean resBits = bitsRight.add(e);
        ExtendedSet<Integer> app = empty(c);
        app.addAll(itemsRight);
        if (bitsRight.hashCode() != app.hashCode()) {
          System.out.println("ADD ERROR!");
          System.out.println("itemsRight.contains(" + e + "): " + itemsRight.contains(e));
          System.out.println("bitsRight.contains(" + e + "): " + bitsRight.contains(e));
          System.out.println("itemsRight:");
          System.out.println(itemsRight);
          System.out.println("bitsRight:");
          System.out.println(bitsRight.debugInfo());
          System.out.println("Append:");
          System.out.println(app.debugInfo());
          System.out.println("Clone:");
          System.out.println(clone.debugInfo());
          return;
        }
        if (resItems != resBits) {
          System.out.println("ADD BOOLEAN ERROR!");
          System.out.println("itemsRight.add(" + e + "): " + resItems);
          System.out.println("bitsRight.add(" + e + "): " + resBits);
          System.out.println("itemsRight:");
          System.out.println(itemsRight);
          System.out.println("bitsRight:");
          System.out.println(bitsRight.debugInfo());
          return;
        }
      }
      for (Integer e : rn.generate()) {
        ExtendedSet<Integer> clone = bitsRight.clone();
        boolean resItems = itemsRight.remove(e);
        boolean resBits = bitsRight.remove(e);
        ExtendedSet<Integer> app = empty(c);
        app.addAll(itemsRight);
        if (bitsRight.hashCode() != app.hashCode()) {
          System.out.println("REMOVE ERROR!");
          System.out.println("itemsRight.contains(" + e + "): " + itemsRight.contains(e));
          System.out.println("bitsRight.contains(" + e + "): " + bitsRight.contains(e));
          System.out.println("itemsRight:");
          System.out.println(itemsRight);
          System.out.println("bitsRight:");
          System.out.println(bitsRight.debugInfo());
          System.out.println("Append:");
          System.out.println(app.debugInfo());
          System.out.println("Clone:");
          System.out.println(clone.debugInfo());
          return;
        }
        if (resItems != resBits) {
          System.out.println("REMOVE BOOLEAN ERROR!");
          System.out.println("itemsRight.remove(" + e + "): " + resItems);
          System.out.println("bitsRight.remove(" + e + "): " + resBits);
          System.out.println("itemsRight:");
          System.out.println(itemsRight);
          System.out.println("bitsRight:");
          System.out.println(bitsRight.debugInfo());
          System.out.println("Clone:");
          System.out.println(clone.debugInfo());
          return;
        }
      }
      for (Integer e : rn.generate()) {
        ExtendedSet<Integer> clone = bitsRight.clone();
        if (!itemsRight.remove(e)) {
          itemsRight.add(e);
        }
        bitsRight.flip(e);
        ExtendedSet<Integer> app = empty(c);
        app.addAll(itemsRight);
        if (bitsRight.hashCode() != app.hashCode()) {
          System.out.println("FLIP ERROR!");
          System.out.println("itemsRight.contains(" + e + "): " + itemsRight.contains(e));
          System.out.println("bitsRight.contains(" + e + "): " + bitsRight.contains(e));
          System.out.println("itemsRight:");
          System.out.println(itemsRight);
          System.out.println("bitsRight:");
          System.out.println(bitsRight.debugInfo());
          System.out.println("Append:");
          System.out.println(app.debugInfo());
          System.out.println("Clone:");
          System.out.println(clone.debugInfo());
          return;
        }
      }

      // new right operand
      itemsRight = rn.generate();
      bitsRight.clear();
      bitsRight.addAll(itemsRight);

			/*
			 * check for content correctness, first(), and last() 
			 */
      if (!checkContent(bitsRight, itemsRight)) {
        System.out.println("RIGHT OPERAND ERROR!");
        System.out.println("Same elements: " + (itemsRight.toString().equals(bitsRight.toString())));
        System.out.println("itemsRight:");
        System.out.println(itemsRight);
        System.out.println("bitsRight:");
        System.out.println(bitsRight.debugInfo());

        System.out.println("itemsRight.size(): " + itemsRight.size() + " ?= bitsRight.size(): " + bitsRight.size());
        for (Integer x : bitsRight) {
          if (!itemsRight.contains(x)) {
            System.out.println("itemsRight does not contain " + x);
          }
        }
        for (Integer x : itemsRight) {
          if (!bitsRight.contains(x)) {
            System.out.println("itemsRight does not contain " + x);
          }
        }
        System.out.println("bitsRight.last(): " + bitsRight.last() + " ?= itemsRight.last(): " + itemsRight.last());
        System.out.println("bitsRight.first(): " + bitsRight.first() + " ?= itemsRight.first(): " + itemsRight.first());

        return;
      }
			
			/*
			 * containsAll()
			 */
      boolean bitsRes = bitsLeft.containsAll(bitsRight);
      boolean itemsRes = itemsLeft.containsAll(itemsRight);
      if (bitsRes != itemsRes) {
        System.out.println("CONTAINS_ALL ERROR!");
        System.out.println("bitsLeft.containsAll(bitsRight): " + bitsRes);
        System.out.println("itemsLeft.containsAll(itemsRight): " + itemsRes);
        System.out.println("bitsLeft:");
        System.out.println(bitsLeft.debugInfo());
        System.out.println("bitsRight:");
        System.out.println(bitsRight.debugInfo());
        System.out.println("bitsLeft.intersection(bitsRight)");
        System.out.println(bitsLeft.intersection(bitsRight));
        System.out.println("itemsLeft.retainAll(itemsRight)");
        itemsLeft.retainAll(itemsRight);
        System.out.println(itemsLeft);
        return;
      }

			/*
			 * containsAny()
			 */
      bitsRes = bitsLeft.containsAny(bitsRight);
      itemsRes = true;
      for (Integer x : itemsRight) {
        itemsRes = itemsLeft.contains(x);
        if (itemsRes) {
          break;
        }
      }
      if (bitsRes != itemsRes) {
        System.out.println("bitsLeft.containsAny(bitsRight): " + bitsRes);
        System.out.println("itemsLeft.containsAny(itemsRight): " + itemsRes);
        System.out.println("bitsLeft:");
        System.out.println(bitsLeft.debugInfo());
        System.out.println("bitsRight:");
        System.out.println(bitsRight.debugInfo());
        System.out.println("bitsLeft.intersection(bitsRight)");
        System.out.println(bitsLeft.intersection(bitsRight));
        System.out.println("itemsLeft.retainAll(itemsRight)");
        itemsLeft.retainAll(itemsRight);
        System.out.println(itemsLeft);
        return;
      }
			
			/*
			 * containsAtLeast()
			 */
      int l = 1 + r.nextInt(bitsRight.size() + 1);
      bitsRes = bitsLeft.containsAtLeast(bitsRight, l);
      int itemsResCnt = 0;
      for (Integer x : itemsRight) {
        if (itemsLeft.contains(x)) {
          itemsResCnt++;
        }
        if (itemsResCnt >= l) {
          break;
        }
      }
      if (bitsRes != (itemsResCnt >= l)) {
        System.out.println("bitsLeft.containsAtLeast(bitsRight, " + l + "): " + bitsRes);
        System.out.println("itemsLeft.containsAtLeast(itemsRight, " + l + "): " + (itemsResCnt >= l));
        System.out.println("bitsLeft:");
        System.out.println(bitsLeft.debugInfo());
        System.out.println("bitsRight:");
        System.out.println(bitsRight.debugInfo());
        System.out.println("bitsLeft.intersection(bitsRight)");
        System.out.println(bitsLeft.intersection(bitsRight));
        System.out.println("itemsLeft.retainAll(itemsRight)");
        itemsLeft.retainAll(itemsRight);
        System.out.println(itemsLeft);
        return;
      }

			/*
			 * Perform a random operation with the previous set:
			 * addAll() and unionSize()
			 * removeAll() and differenceSize()
			 * retainAll() and intersectionSize()
			 * symmetricDifference() and symmetricDifferenceSize()
			 * complement() and complementSize()
			 */
      ExtendedSet<Integer> alternative = null;
      int operationSize = 0;
      boolean resItems = true, resBits = true;
      switch (1 + r.nextInt(5)) {
        case 1:
          System.out.format(" union of %d elements with %d elements... ", itemsLeft.size(), itemsRight.size());
          System.out.flush();
          operationSize = bitsLeft.unionSize(bitsRight);
          resItems = itemsLeft.addAll(itemsRight);
          alternative = bitsLeft.union(bitsRight);
          resBits = bitsLeft.addAll(bitsRight);
          break;

        case 2:
          System.out.format(" difference of %d elements with %d elements... ", itemsLeft.size(), itemsRight.size());
          System.out.flush();
          operationSize = bitsLeft.differenceSize(bitsRight);
          resItems = itemsLeft.removeAll(itemsRight);
          alternative = bitsLeft.difference(bitsRight);
          resBits = bitsLeft.removeAll(bitsRight);
          break;

        case 3:
          System.out.format(" intersection of %d elements with %d elements... ", itemsLeft.size(), itemsRight.size());
          System.out.flush();
          operationSize = bitsLeft.intersectionSize(bitsRight);
          resItems = itemsLeft.retainAll(itemsRight);
          alternative = bitsLeft.intersection(bitsRight);
          resBits = bitsLeft.retainAll(bitsRight);
          break;

        case 4:
          System.out.format(
              " symmetric difference of %d elements with %d elements... ",
              itemsLeft.size(),
              itemsRight.size()
          );
          System.out.flush();
          operationSize = bitsLeft.symmetricDifferenceSize(bitsRight);
          TreeSet<Integer> temp = new TreeSet<Integer>(itemsRight);
          temp.removeAll(itemsLeft);
          itemsLeft.removeAll(itemsRight);
          itemsLeft.addAll(temp);
          bitsLeft = bitsLeft.symmetricDifference(bitsRight);
          alternative = bitsLeft;
          break;

        case 5:
          System.out.format(" complement of %d elements... ", itemsLeft.size());
          System.out.flush();
          operationSize = bitsLeft.complementSize();
          if (!itemsLeft.isEmpty()) {
            if ((bitsLeft instanceof IntegerSet) && (((IntegerSet) bitsLeft).intSet() instanceof MatrixIntSet)) {
              BinaryMatrix m = ((MatrixIntSet) ((IntegerSet) bitsLeft).intSet()).matrix;
              int x = m.maxCol();
              for (int rx = m.maxRow(); rx >= 0; rx--) {
                for (int cx = x; cx >= 0; cx--) {
                  if (!itemsLeft.add(MatrixIntSet.toInt(rx, cx))) {
                    itemsLeft.remove(MatrixIntSet.toInt(rx, cx));
                  }
                }
              }
            } else {
              for (int j = itemsLeft.last(); j >= 0; j--) {
                if (!itemsLeft.add(j)) {
                  itemsLeft.remove(j);
                }
              }
            }
          }
          bitsLeft.complement();
          alternative = bitsLeft;
          break;
        default:
          throw new RuntimeException("Unexpected error!");
      }

      // check the list of elements
      if (!checkContent(bitsLeft, itemsLeft)) {
        System.out.println("OPERATION ERROR!");
        System.out.println("Same elements: " + (itemsLeft.toString().equals(bitsLeft.toString())));
        System.out.println("itemsLeft:");
        System.out.println(itemsLeft);
        System.out.println("bitsLeft:");
        System.out.println(bitsLeft.debugInfo());

        System.out.println("itemsLeft.size(): " + itemsLeft.size() + " ?= bitsLeft.size(): " + bitsLeft.size());
        for (Integer x : bitsLeft) {
          if (!itemsLeft.contains(x)) {
            System.out.println("itemsLeft does not contain " + x);
          }
        }
        for (Integer x : itemsLeft) {
          if (!bitsLeft.contains(x)) {
            System.out.println("itemsLeft does not contain " + x);
          }
        }
        System.out.println("bitsLeft.last(): " + bitsLeft.last() + " ?= itemsLeft.last(): " + itemsLeft.last());
        System.out.println("bitsLeft.first(): " + bitsLeft.first() + " ?= itemsLeft.first(): " + itemsLeft.first());

        return;
      }

      // check the size
      if (itemsLeft.size() != operationSize) {
        System.out.println("OPERATION SIZE ERROR");
        System.out.println("Wrong size: " + operationSize);
        System.out.println("Correct size: " + itemsLeft.size());
        System.out.println("bitsLeft:");
        System.out.println(bitsLeft.debugInfo());
        return;
      }

      // check the boolean result
      if (resItems != resBits) {
        System.out.println("OPERATION BOOLEAN ERROR!");
        System.out.println("resItems: " + resItems);
        System.out.println("resBits: " + resBits);
        System.out.println("bitsLeft:");
        System.out.println(bitsLeft.debugInfo());
        return;
      }

      // check the internal representation of the result
      ExtendedSet<Integer> x = bitsLeft.empty();
      x.addAll(itemsLeft);
      if (x.hashCode() != bitsLeft.hashCode()) {
        System.out.println("Internal representation error!");
        System.out.println("FROM APPEND:");
        System.out.println(x.debugInfo());
        System.out.println("FROM OPERATION:");
        System.out.println(bitsLeft.debugInfo());
        return;
      }

      // check similar results
      if (!bitsLeft.equals(alternative)) {
        System.out.println("ALTERNATIVE OPERATION ERROR!");
        System.out.println("bitsLeft:");
        System.out.println(bitsLeft.debugInfo());
        System.out.println("alternative:");
        System.out.println(alternative.debugInfo());
        return;
      }

      System.out.println("done.");
    }
  }

  /**
   * Stress test (addition) for {@link #subSet(Integer, Integer)}
   */
  private static void testForSubSetAdditionStress()
  {
    IntegerSet previousBits = new IntegerSet(new ConciseSet());
    IntegerSet currentBits = new IntegerSet(new ConciseSet());
    TreeSet<Integer> currentItems = new TreeSet<Integer>();

    Random rnd = new MersenneTwister();

    for (int j = 0; j < 100000; j++) {
      // keep the previous result
      previousBits = currentBits;
      currentBits = currentBits.clone();

      // generate a new subview
      int min = rnd.nextInt(10000);
      int max = min + 1 + rnd.nextInt(10000 - (min + 1) + 1);
      int item = min + rnd.nextInt((max - 1) - min + 1);
      System.out.println("Adding " + item + " to the subview from " + min + " to " + max + " - 1");
      SortedSet<Integer> subBits = currentBits.subSet(min, max);
      SortedSet<Integer> subItems = currentItems.subSet(min, max);
      boolean subBitsResult = subBits.add(item);
      boolean subItemsResult = subItems.add(item);

      if (subBitsResult != subItemsResult
          || subBits.size() != subItems.size()
          || !subBits.toString().equals(subItems.toString())) {
        System.out.println("Subset error!");
        return;
      }

      if (!checkContent(currentBits, currentItems)) {
        System.out.println("Subview not correct!");
        System.out.println("Same elements: " + (currentItems.toString().equals(currentBits.toString())));
        System.out.println("Original: " + currentItems);
        System.out.println(currentBits.debugInfo());
        System.out.println(previousBits.debugInfo());
        return;
      }

      // check the representation
      IntegerSet otherBits = new IntegerSet(new ConciseSet());
      otherBits.addAll(currentItems);
      if (otherBits.hashCode() != currentBits.hashCode()) {
        System.out.println("Representation not correct!");
        System.out.println(currentBits.debugInfo());
        System.out.println(otherBits.debugInfo());
        System.out.println(previousBits.debugInfo());
        return;
      }
    }

    System.out.println(currentBits.debugInfo());
    System.out.println(IntSetStatistics.summary());
  }

  /**
   * Stress test (addition) for {@link ConciseSet#subSet(Integer, Integer)}
   */
  private static void testForSubSetRemovalStress()
  {
    IntegerSet previousBits = new IntegerSet(new ConciseSet());
    IntegerSet currentBits = new IntegerSet(new ConciseSet());
    TreeSet<Integer> currentItems = new TreeSet<Integer>();

    // create a 1-filled bitset
    currentBits.add(10001);
    currentBits.complement();
    currentItems.addAll(currentBits);
    if (currentItems.size() != 10001) {
      System.out.println("Unexpected error!");
      return;
    }

    Random rnd = new MersenneTwister();

    for (int j = 0; j < 100000; j++) {
      // keep the previous result
      previousBits = currentBits;
      currentBits = currentBits.clone();

      // generate a new subview
      int min = rnd.nextInt(10000);
      int max = min + 1 + rnd.nextInt(10000 - (min + 1) + 1);
      int item = rnd.nextInt(10000 + 1);
      System.out.println("Removing " + item + " from the subview from " + min + " to " + max + " - 1");
      SortedSet<Integer> subBits = currentBits.subSet(min, max);
      SortedSet<Integer> subItems = currentItems.subSet(min, max);
      boolean subBitsResult = subBits.remove(item);
      boolean subItemsResult = subItems.remove(item);

      if (subBitsResult != subItemsResult
          || subBits.size() != subItems.size()
          || !subBits.toString().equals(subItems.toString())) {
        System.out.println("Subset error!");
        return;
      }

      if (!checkContent(currentBits, currentItems)) {
        System.out.println("Subview not correct!");
        System.out.println("Same elements: " + (currentItems.toString().equals(currentBits.toString())));
        System.out.println("Original: " + currentItems);
        System.out.println(currentBits.debugInfo());
        System.out.println(previousBits.debugInfo());
        return;
      }

      // check the representation
      IntegerSet otherBits = new IntegerSet(new ConciseSet());
      otherBits.addAll(currentItems);
      if (otherBits.hashCode() != currentBits.hashCode()) {
        System.out.println("Representation not correct!");
        System.out.println(currentBits.debugInfo());
        System.out.println(otherBits.debugInfo());
        System.out.println(previousBits.debugInfo());
        return;
      }
    }

    System.out.println(currentBits.debugInfo());
    System.out.println(IntSetStatistics.summary());
  }

  /**
   * Random operations on random sub sets.
   * <p>
   * It randomly chooses among all operations and performs the operation over
   * random sets
   */
  private static void testForSubSetRandomOperationsStress()
  {
    IntegerSet bits = new IntegerSet(new ConciseSet());
    IntegerSet bitsPrevious = new IntegerSet(new ConciseSet());
    TreeSet<Integer> items = new TreeSet<Integer>();

    Random rnd = new MersenneTwister();

    // random operation loop
    for (int i = 0; i < 100000; i++) {
      System.out.print("Test " + i + ": ");

      // new set
      bitsPrevious = bits.clone();
      if (!bitsPrevious.toString().equals(bits.toString())) {
        throw new RuntimeException("clone() error!");
      }
      bits.clear();
      items.clear();
      final int size = 1 + rnd.nextInt(10000);
      final int min = 1 + rnd.nextInt(10000 - 1);
      final int max = min + rnd.nextInt(10000 - min + 1);
      final int minSub = 1 + rnd.nextInt(10000 - 1);
      final int maxSub = minSub + rnd.nextInt(10000 - minSub + 1);
      for (int j = 0; j < size; j++) {
        int item = min + rnd.nextInt(max - min + 1);
        bits.add(item);
        items.add(item);
      }

      // perform base checks
      SortedSet<Integer> bitsSubSet = bits.subSet(minSub, maxSub);
      SortedSet<Integer> itemsSubSet = items.subSet(minSub, maxSub);
      if (!bitsSubSet.toString().equals(itemsSubSet.toString())) {
        System.out.println("toString() difference!");
        System.out.println("value: " + bitsSubSet.toString());
        System.out.println("actual: " + itemsSubSet.toString());
        return;
      }
      if (bitsSubSet.size() != itemsSubSet.size()) {
        System.out.println("size() difference!");
        System.out.println("value: " + bitsSubSet.size());
        System.out.println("actual: " + itemsSubSet.size());
        System.out.println("bits: " + bits.toString());
        System.out.println("items: " + items.toString());
        System.out.println("bitsSubSet: " + bitsSubSet.toString());
        System.out.println("itemsSubSet: " + itemsSubSet.toString());
        return;
      }
      if (!itemsSubSet.isEmpty() && (!bitsSubSet.first().equals(itemsSubSet.first()))) {
        System.out.println("first() difference!");
        System.out.println("value: " + bitsSubSet.first());
        System.out.println("actual: " + itemsSubSet.first());
        System.out.println("bits: " + bits.toString());
        System.out.println("items: " + items.toString());
        System.out.println("bitsSubSet: " + bitsSubSet.toString());
        System.out.println("itemsSubSet: " + itemsSubSet.toString());
        return;
      }
      if (!itemsSubSet.isEmpty() && (!bitsSubSet.last().equals(itemsSubSet.last()))) {
        System.out.println("last() difference!");
        System.out.println("value: " + bitsSubSet.last());
        System.out.println("actual: " + itemsSubSet.last());
        System.out.println("bits: " + bits.toString());
        System.out.println("items: " + items.toString());
        System.out.println("bitsSubSet: " + bitsSubSet.toString());
        System.out.println("itemsSubSet: " + itemsSubSet.toString());
        return;
      }

      // perform the random operation
      boolean resBits = false;
      boolean resItems = false;
      boolean exceptionBits = false;
      boolean exceptionItems = false;
      switch (1 + rnd.nextInt(4)) {
        case 1:
          System.out.format(" addAll() of %d elements on %d elements... ", bitsPrevious.size(), bits.size());
          try {
            resBits = bitsSubSet.addAll(bitsPrevious);
          }
          catch (Exception e) {
            bits.clear();
            System.out.print("\n\tEXCEPTION on bitsSubSet: " + e.getClass() + " ");
            exceptionBits = true;
          }
          try {
            resItems = itemsSubSet.addAll(bitsPrevious);
          }
          catch (Exception e) {
            items.clear();
            System.out.print("\n\tEXCEPTION on itemsSubSet: " + e.getClass() + " ");
            exceptionItems = true;
          }
          break;

        case 2:
          System.out.format(" removeAll() of %d elements on %d elements... ", bitsPrevious.size(), bits.size());
          try {
            resBits = bitsSubSet.removeAll(bitsPrevious);
          }
          catch (Exception e) {
            bits.clear();
            System.out.print("\n\tEXCEPTION on bitsSubSet: " + e.getClass() + " ");
            exceptionBits = true;
          }
          try {
            resItems = itemsSubSet.removeAll(bitsPrevious);
          }
          catch (Exception e) {
            items.clear();
            System.out.print("\n\tEXCEPTION on itemsSubSet: " + e.getClass() + " ");
            exceptionItems = true;
          }
          break;

        case 3:
          System.out.format(" retainAll() of %d elements on %d elements... ", bitsPrevious.size(), bits.size());
          try {
            resBits = bitsSubSet.retainAll(bitsPrevious);
          }
          catch (Exception e) {
            bits.clear();
            System.out.print("\n\tEXCEPTION on bitsSubSet: " + e.getClass() + " ");
            exceptionBits = true;
          }
          try {
            resItems = itemsSubSet.retainAll(bitsPrevious);
          }
          catch (Exception e) {
            items.clear();
            System.out.print("\n\tEXCEPTION on itemsSubSet: " + e.getClass() + " ");
            exceptionItems = true;
          }
          break;

        case 4:
          System.out.format(" clear() of %d elements on %d elements... ", bitsPrevious.size(), bits.size());
          try {
            bitsSubSet.clear();
          }
          catch (Exception e) {
            bits.clear();
            System.out.print("\n\tEXCEPTION on bitsSubSet: " + e.getClass() + " ");
            exceptionBits = true;
          }
          try {
            itemsSubSet.clear();
          }
          catch (Exception e) {
            items.clear();
            System.out.print("\n\tEXCEPTION on itemsSubSet: " + e.getClass() + " ");
            exceptionItems = true;
          }
          break;
      }

      if (exceptionBits != exceptionItems) {
        System.out.println("Incorrect exception!");
        return;
      }

      if (resBits != resItems) {
        System.out.println("Incorrect results!");
        System.out.println("resBits: " + resBits);
        System.out.println("resItems: " + resItems);
        return;
      }

      if (!checkContent(bits, items)) {
        System.out.println("Subview not correct!");
        System.out.format("min: %d, max: %d, minSub: %d, maxSub: %d\n", min, max, minSub, maxSub);
        System.out.println("Same elements: " + (items.toString().equals(bits.toString())));
        System.out.println("Original: " + items);
        System.out.println(bits.debugInfo());
        System.out.println(bitsPrevious.debugInfo());
        return;
      }

      // check the representation
      IntegerSet otherBits = new IntegerSet(new ConciseSet());
      otherBits.addAll(items);
      if (otherBits.hashCode() != bits.hashCode()) {
        System.out.println("Representation not correct!");
        System.out.format("min: %d, max: %d, minSub: %d, maxSub: %d\n", min, max, minSub, maxSub);
        System.out.println(bits.debugInfo());
        System.out.println(otherBits.debugInfo());
        System.out.println(bitsPrevious.debugInfo());
        return;
      }

      System.out.println("done.");
    }
  }

  /**
   * Test the method {@link ExtendedSet#compareTo(ExtendedSet)}
   *
   * @param c class to test
   */
  private static void testForComparatorSimple(Class<? extends ExtendedSet<Integer>> c)
  {
    ExtendedSet<Integer> bitsLeft = empty(c);
    ExtendedSet<Integer> bitsRight = empty(c);

    bitsLeft.add(1);
    bitsLeft.add(2);
    bitsLeft.add(3);
    bitsLeft.add(100);
    bitsRight.add(1000000);
    System.out.println("A: " + bitsLeft);
    System.out.println("B: " + bitsRight);
    System.out.println("A.compareTo(B): " + bitsLeft.compareTo(bitsRight));
    System.out.println();

    bitsLeft.add(1000000);
    bitsRight.add(1);
    bitsRight.add(2);
    bitsRight.add(3);
    System.out.println("A: " + bitsLeft);
    System.out.println("B: " + bitsRight);
    System.out.println("A.compareTo(B): " + bitsLeft.compareTo(bitsRight));
    System.out.println();

    bitsLeft.remove(100);
    System.out.println("A: " + bitsLeft);
    System.out.println("B: " + bitsRight);
    System.out.println("A.compareTo(B): " + bitsLeft.compareTo(bitsRight));
    System.out.println();

    bitsRight.remove(1);
    System.out.println("A: " + bitsLeft);
    System.out.println("B: " + bitsRight);
    System.out.println("A.compareTo(B): " + bitsLeft.compareTo(bitsRight));
    System.out.println();

    bitsLeft.remove(1);
    bitsLeft.remove(2);
    System.out.println("A: " + bitsLeft);
    System.out.println("B: " + bitsRight);
    System.out.println("A.compareTo(B): " + bitsLeft.compareTo(bitsRight));
    System.out.println();
  }

  /**
   * Another test for {@link ExtendedSet#compareTo(ExtendedSet)}
   *
   * @param c class to test
   */
  private static void testForComparatorComplex(Class<? extends ExtendedSet<Integer>> c)
  {
    ExtendedSet<Integer> bitsLeft = empty(c);
    ExtendedSet<Integer> bitsRight = empty(c);

    Random rnd = new MersenneTwister();
    for (int i = 0; i < 10000; i++) {
      // empty numbers
      BigInteger correctLeft = BigInteger.ZERO;
      BigInteger correctRight = BigInteger.ZERO;
      bitsLeft.clear();
      bitsRight.clear();

      int size = 10 + rnd.nextInt(10000);
      RandomNumbers rn;
      if (rnd.nextBoolean()) {
        rn = new RandomNumbers.Uniform(rnd.nextInt(size), rnd.nextDouble() * 0.999, rnd.nextInt(size / 10));
      } else {
        rn = new RandomNumbers.Markovian(rnd.nextInt(size), rnd.nextDouble() * 0.999, rnd.nextInt(size / 10));
      }
      bitsLeft.addAll(rn.generate());
      if (rnd.nextBoolean()) {
        bitsRight.addAll(bitsLeft);
        bitsRight.add(rnd.nextInt(size));
      } else {
        bitsRight.addAll(rn.generate());
      }
      for (int x : bitsLeft.descending()) {
        correctLeft = correctLeft.setBit(x);
      }
      for (int x : bitsRight) {
        correctRight = correctRight.setBit(x);
      }

      // compare them!
      boolean correct = bitsLeft.compareTo(bitsRight) == correctLeft.compareTo(correctRight);
      System.out.println(i + ": " + correct);
      if (!correct) {
        System.out.println("ERROR!");
        System.out.println("bitsLeft:  " + bitsLeft);
        System.out.println("           " + bitsLeft.debugInfo());
        System.out.println("bitsRight: " + bitsRight);
        System.out.println("           " + bitsRight.debugInfo());
        int maxLength = Math.max(correctLeft.bitLength(), correctRight.bitLength());
        System.out.format("correctLeft.toString(2):  %" + maxLength + "s\n", correctLeft.toString(2));
        System.out.format("correctRight.toString(2): %" + maxLength + "s\n", correctRight.toString(2));
        System.out.println("correctLeft.compareTo(correctRight): " + correctLeft.compareTo(correctRight));
        System.out.println("bitsLeft.compareTo(bitsRight):  " + bitsLeft.compareTo(bitsRight));

        Iterator<Integer> itrLeft = bitsLeft.descendingIterator();
        Iterator<Integer> itrRight = bitsRight.descendingIterator();
        while (itrLeft.hasNext() && itrRight.hasNext()) {
          int l = itrLeft.next();
          int r = itrRight.next();
          if (l != r) {
            System.out.println("l != r --> " + l + ", " + r);
            break;
          }
        }
        return;
      }
    }
    System.out.println("Done!");
  }

  /**
   * Stress test for {@link ExtendedSet#descendingIterator()}
   *
   * @param c class to test
   */
  private static void testForDescendingIterator(Class<? extends ExtendedSet<Integer>> c)
  {
    ExtendedSet<Integer> bits = empty(c);

    Random rnd = new MersenneTwister();
    for (int i = 0; i < 100000; i++) {
      int n = rnd.nextInt(10000);
      System.out.print(i + ": add " + n);
      bits.add(n);

      Set<Integer> x = new HashSet<Integer>(bits);
      Set<Integer> y = new HashSet<Integer>();
      try {
        for (Integer e : bits.descending()) {
          y.add(e);
        }
      }
      catch (Exception e) {
        System.out.println("\nERROR!");
        System.out.println(e.getMessage());
        System.out.println(bits.debugInfo());
        break;
      }
      boolean correct = x.equals(y);
      System.out.println(" --> " + correct);
      if (!correct) {
        System.out.println(bits.debugInfo());
        System.out.print("result: ");
        for (Integer e : bits.descending()) {
          System.out.print(e + ", ");
        }
        System.out.println();
        break;
      }
    }

    System.out.println("Done!");
  }

  /**
   * Stress test for {@link ConciseSet#get(int)}
   *
   * @param c class to test
   */
  private static void testForPosition(Class<? extends ExtendedSet<Integer>> c)
  {
    ExtendedSet<Integer> bits = empty(c);

    Random rnd = new MersenneTwister(31);
    for (int i = 0; i < 1000; i++) {
      // new set
      bits.clear();
      final int size = 1 + rnd.nextInt(10000);
      final int min = 1 + rnd.nextInt(10000 - 1);
      final int max = min + rnd.nextInt(10000 - min + 1);
      for (int j = 0; j < size; j++) {
        int item = min + rnd.nextInt(max - min + 1);
        bits.add(item);
      }

      // check correctness
      String good = bits.toString();
      StringBuilder other = new StringBuilder();
      int s = bits.size();
      other.append('[');
      for (int j = 0; j < s; j++) {
        other.append(bits.get(j));
        if (j < s - 1) {
          other.append(", ");
        }
      }
      other.append(']');

      if (good.equals(other.toString())) {
        System.out.println(i + ") OK");
      } else {
        System.out.println("ERROR");
        System.out.println(bits.debugInfo());
        System.out.println(bits);
        System.out.println(other);
        return;
      }

      int pos = 0;
      for (Integer x : bits) {
        if (bits.indexOf(x) != pos) {
          System.out.println("ERROR! " + pos + " != " + bits.indexOf(x) + " for element " + x);
          System.out.println(bits.debugInfo());
          return;
        }
        pos++;
      }
    }
  }

  /**
   * Test for {@link ExtendedIterator#skipAllBefore(Object)}
   *
   * @param c class to test
   */
  private static void testForSkip(Class<? extends ExtendedSet<Integer>> c)
  {
    ExtendedSet<Integer> bits = empty(c);

    Random rnd = new MersenneTwister(31);
    for (int i = 0; i < 10000; i++) {
      int max = rnd.nextInt(10000);
      bits = bits.convert(new RandomNumbers.Uniform(
          rnd.nextInt(1000),
          rnd.nextDouble() * 0.999,
          rnd.nextInt(100)
      ).generate());

      for (int j = 0; j < 100; j++) {
        int skip = rnd.nextInt(max + 1);
        boolean reverse = rnd.nextBoolean();
        System.out.format("%d) size=%d, skip=%d, reverse=%b ---> ", (i * 100) + j + 1, bits.size(), skip, reverse);

        ExtendedIterator<Integer> itr1, itr2;
        if (!reverse) {
          itr1 = bits.iterator();
          itr2 = bits.iterator();
          while (itr1.hasNext() && itr1.next() < skip) {/* nothing */}
        } else {
          itr1 = bits.descendingIterator();
          itr2 = bits.descendingIterator();
          while (itr1.hasNext() && itr1.next() > skip) {/* nothing */}
        }
        if (!itr1.hasNext()) {
          System.out.println("Skipped!");
          continue;
        }
        itr2.skipAllBefore(skip);
        itr2.next();
        Integer i1, i2;
        if (!(i1 = itr1.next()).equals(i2 = itr2.next())) {
          System.out.println("Error!");
          System.out.println("i1 = " + i1);
          System.out.println("i2 = " + i2);
          System.out.println(bits.debugInfo());
          return;
        }
        System.out.println("OK!");
      }
    }
    System.out.println("Done!");
  }

  /**
   * Test launcher
   *
   * @param args ID of the test to execute
   */
  public static void main(String[] args)
  {
    // NOTE: the most complete test is TestCase.RANDOM_OPERATION_STRESS
//		TestCase testCase = TestCase.ADDITION_STRESS;
//		TestCase testCase = TestCase.REMOVAL_STRESS;
//		TestCase testCase = TestCase.RANDOM_OPERATION_STRESS;
//		TestCase testCase = TestCase.FILL_CLEAR_STRESS;
//		TestCase testCase = TestCase.SKIP;
    TestCase testCase = TestCase.POSITION;
//		TestCase testCase = TestCase.COMPARATOR_COMPLEX;
//		TestCase testCase = TestCase.DESCENDING_ITERATOR;

//		Class<? extends ExtendedSet<Integer>> classToTest = IntegerHashSet.class;
//		Class<? extends ExtendedSet<Integer>> classToTest = IntegerFastSet.class;
//		Class<? extends ExtendedSet<Integer>> classToTest = IntegerConciseSet.class;
//		Class<? extends ExtendedSet<Integer>> classToTest = IntegerConcise2Set.class;
//		Class<? extends ExtendedSet<Integer>> classToTest = IntegerConcisePlusSet.class;
//		Class<? extends ExtendedSet<Integer>> classToTest = IntegerWAHSet.class;
//		Class<? extends ExtendedSet<Integer>> classToTest = ListSet.class;
//		Class<? extends ExtendedSet<Integer>> classToTest = LinkedSet.class;
    Class<? extends ExtendedSet<Integer>> classToTest = MatrixSet.class;

    if (args != null && args.length > 0) {
      try {
        testCase = TestCase.values()[Integer.parseInt(args[0])];
      }
      catch (NumberFormatException ignore) {
        // nothing to do
      }
    }

    switch (testCase) {
      case ADDITION_STRESS:
        testForAdditionStress(classToTest);
        break;
      case REMOVAL_STRESS:
        testForRemovalStress(classToTest);
        break;
      case RANDOM_OPERATION_STRESS:
        testForRandomOperationsStress(classToTest, false);
        break;
      case FILL_CLEAR_STRESS:
        testForRandomOperationsStress(classToTest, true);
        break;
      case SUBSET_ADDITION_STRESS_CONCISESET:
        testForSubSetAdditionStress();
        break;
      case SUBSET_REMOVAL_STRESS_CONCISESET:
        testForSubSetRemovalStress();
        break;
      case SUBSET_RANDOM_OPERATION_STRESS_CONCISESET:
        testForSubSetRandomOperationsStress();
        break;
      case COMPARATOR_SIMPLE:
        testForComparatorSimple(classToTest);
        break;
      case COMPARATOR_COMPLEX:
        testForComparatorComplex(classToTest);
        break;
      case DESCENDING_ITERATOR:
        testForDescendingIterator(classToTest);
        break;
      case POSITION:
        testForPosition(classToTest);
        break;
      case SKIP:
        testForSkip(classToTest);
    }
  }

  /**
   * @author alessandrocolantonio
   */
  private enum TestCase
  {
    /**
     * @uml.property name="aDDITION_STRESS"
     * @uml.associationEnd
     */
    ADDITION_STRESS,
    /**
     * @uml.property name="rEMOVAL_STRESS"
     * @uml.associationEnd
     */
    REMOVAL_STRESS,
    /**
     * @uml.property name="rANDOM_OPERATION_STRESS"
     * @uml.associationEnd
     */
    RANDOM_OPERATION_STRESS,
    /**
     * @uml.property name="fILL_CLEAR_STRESS"
     * @uml.associationEnd
     */
    FILL_CLEAR_STRESS,
    /**
     * @uml.property name="sUBSET_ADDITION_STRESS_CONCISESET"
     * @uml.associationEnd
     */
    SUBSET_ADDITION_STRESS_CONCISESET,
    /**
     * @uml.property name="sUBSET_REMOVAL_STRESS_CONCISESET"
     * @uml.associationEnd
     */
    SUBSET_REMOVAL_STRESS_CONCISESET,
    /**
     * @uml.property name="sUBSET_RANDOM_OPERATION_STRESS_CONCISESET"
     * @uml.associationEnd
     */
    SUBSET_RANDOM_OPERATION_STRESS_CONCISESET,
    /**
     * @uml.property name="cOMPARATOR_SIMPLE"
     * @uml.associationEnd
     */
    COMPARATOR_SIMPLE,
    /**
     * @uml.property name="cOMPARATOR_COMPLEX"
     * @uml.associationEnd
     */
    COMPARATOR_COMPLEX,
    /**
     * @uml.property name="dESCENDING_ITERATOR"
     * @uml.associationEnd
     */
    DESCENDING_ITERATOR,
    /**
     * @uml.property name="pOSITION"
     * @uml.associationEnd
     */
    POSITION,
    /**
     * @uml.property name="sKIP"
     * @uml.associationEnd
     */
    SKIP,;
  }

  @SuppressWarnings("unused")
  private static class ListSet extends GenericExtendedSet<Integer>
  {
    ListSet()
    {
      super(ArrayList.class);
    }
  }

  @SuppressWarnings("unused")
  private static class LinkedSet extends GenericExtendedSet<Integer>
  {
    LinkedSet()
    {
      super(LinkedList.class);
    }
  }

  @SuppressWarnings("unused")
  private static class IntegerHashSet extends IntegerSet
  {
    IntegerHashSet() {super(new IntSetStatistics(new HashIntSet()));}
  }

  @SuppressWarnings("unused")
  private static class IntegerFastSet extends IntegerSet
  {
    IntegerFastSet() {super(new IntSetStatistics(new FastSet()));}
  }

  @SuppressWarnings("unused")
  private static class IntegerConciseSet extends IntegerSet
  {
    IntegerConciseSet() {super(new IntSetStatistics(new ConciseSet()));}
  }

  //	@SuppressWarnings("unused")
//	private static class IntegerConcise2Set extends IntegerSet {IntegerConcise2Set() {super(new IntSetStatistics(new Concise2Set()));}}
  @SuppressWarnings("unused")
  private static class IntegerWAHSet extends IntegerSet
  {
    IntegerWAHSet() {super(new IntSetStatistics(new ConciseSet(true)));}
  }

  @SuppressWarnings("unused")
  private static class IntegerArraySet extends IntegerSet
  {
    IntegerArraySet() {super(new IntSetStatistics(new ArraySet()));}
  }

  //	@SuppressWarnings("unused")
  private static class MatrixSet extends IntegerSet
  {
    MatrixSet() {super(new MatrixIntSet());}
  }

  /**
   * @author alessandrocolantonio
   */
  final static class MatrixIntSet extends AbstractIntSet
  {
    final static int COL_POW = 10;
    /**
     * @uml.property name="matrix"
     * @uml.associationEnd
     */
    BinaryMatrix matrix = new BinaryMatrix(new FastSet());

    final static int toInt(int row, int col) {return (row << COL_POW) + col;}

    final static int toRow(int index) {return index >>> COL_POW;}

    final static int toCol(int index) {return index & (0xFFFFFFFF >>> -COL_POW);}

    IntSet convert(BinaryMatrix m)
    {
      MatrixIntSet res = new MatrixIntSet();
      res.matrix = m;
      return res;
    }

    BinaryMatrix convert(IntSet s)
    {
      return ((MatrixIntSet) s).matrix;
    }

    @Override
    public IntSet convert(int... a)
    {
      MatrixIntSet res = new MatrixIntSet();
      for (int i : a) {
        res.add(i);
      }
      return res;
    }

    @Override
    public IntSet convert(Collection<Integer> c)
    {
      MatrixIntSet res = new MatrixIntSet();
      for (int i : c) {
        res.add(i);
      }
      return res;
    }

    @Override
    public boolean add(int i) {return matrix.add(toRow(i), toCol(i));}

    @Override
    public boolean addAll(IntSet c) {return matrix.addAll(convert(c));}

    @Override
    public double bitmapCompressionRatio() {return matrix.bitmapCompressionRatio();}

    @Override
    public void clear(int from, int to) {matrix.clear(toRow(from), toCol(from), toRow(to), toCol(to));}

    @Override
    public void clear() {matrix.clear();}

    @Override
    public double collectionCompressionRatio() {return matrix.collectionCompressionRatio();}

    @Override
    public void complement() {matrix.complement();}

    @Override
    public int complementSize() {return matrix.complementSize();}

    @Override
    public IntSet complemented() {return convert(matrix.complemented());}

    @Override
    public boolean contains(int i) {return matrix.contains(toRow(i), toCol(i));}

    @Override
    public boolean containsAll(IntSet c) {return matrix.containsAll(convert(c));}

    @Override
    public boolean containsAny(IntSet other) {return matrix.containsAny(convert(other));}

    @Override
    public boolean containsAtLeast(IntSet other, int minElements)
    {
      return matrix.containsAtLeast(
          convert(other),
          minElements
      );
    }

    @Override
    public IntSet difference(IntSet other) {return convert(matrix.difference(convert(other)));}

    @Override
    public int differenceSize(IntSet other) {return matrix.differenceSize(convert(other));}

    @Override
    public IntSet empty() {return new MatrixIntSet();}

    @Override
    public void fill(int from, int to) {matrix.fill(toRow(from), toCol(from), toRow(to), toCol(to));}

    @Override
    public int first() {return toInt(matrix.first()[0], matrix.first()[1]);}

    @Override
    public void flip(int e) {matrix.flip(toRow(e), toCol(e));}

    @Override
    public int get(int i) {return toInt(matrix.get(i)[0], matrix.get(i)[1]);}

    @Override
    public int indexOf(int e) {return matrix.indexOf(toRow(e), toCol(e));}

    @Override
    public IntSet intersection(IntSet other) {return convert(matrix.intersection(convert(other)));}

    @Override
    public int intersectionSize(IntSet other) {return matrix.intersectionSize(convert(other));}

    @Override
    public boolean isEmpty() {return matrix.isEmpty();}

    @Override
    public int last() {return toInt(matrix.last()[0], matrix.last()[1]);}

    @Override
    public boolean remove(int i) {return matrix.remove(toRow(i), toCol(i));}

    @Override
    public boolean removeAll(IntSet c) {return matrix.removeAll(convert(c));}

    @Override
    public boolean retainAll(IntSet c) {return matrix.retainAll(convert(c));}

    @Override
    public int size() {return matrix.size();}

    @Override
    public IntSet symmetricDifference(IntSet other) {return convert(matrix.symmetricDifference(convert(other)));}

    @Override
    public int symmetricDifferenceSize(IntSet other) {return matrix.symmetricDifferenceSize(convert(other));}

    @Override
    public IntSet union(IntSet other) {return convert(matrix.union(convert(other)));}

    @Override
    public int unionSize(IntSet other) {return matrix.unionSize(convert(other));}

    @Override
    public int compareTo(IntSet o) {return matrix.compareTo(convert(o));}

    @Override
    public double jaccardDistance(IntSet other) {return 0;}

    @Override
    public double jaccardSimilarity(IntSet other) {return 0;}

    @Override
    public double weightedJaccardDistance(IntSet other) {return 0;}

    @Override
    public double weightedJaccardSimilarity(IntSet other) {return 0;}

    @Override
    public List<? extends IntSet> powerSet() {return null;}

    @Override
    public List<? extends IntSet> powerSet(int min, int max) {return null;}

    @Override
    public int powerSetSize() {return 0;}

    @Override
    public int powerSetSize(int min, int max) {return 0;}

    @Override
    public IntIterator iterator()
    {
      return new IntIterator()
      {
        CellIterator itr = matrix.iterator();

        @Override
        public boolean hasNext() {return itr.hasNext();}

        @Override
        public int next()
        {
          int[] c = itr.next();
          return toInt(c[0], c[1]);
        }

        @Override
        public void skipAllBefore(int element) {itr.skipAllBefore(toRow(element), toCol(element));}

        @Override
        public void remove() {itr.remove();}

        @Override
        public IntIterator clone() {throw new UnsupportedOperationException();}
      };
    }

    @Override
    public IntIterator descendingIterator()
    {
      return new IntIterator()
      {
        CellIterator itr = matrix.descendingIterator();

        @Override
        public boolean hasNext() {return itr.hasNext();}

        @Override
        public int next()
        {
          int[] c = itr.next();
          return toInt(c[0], c[1]);
        }

        @Override
        public void skipAllBefore(int element) {itr.skipAllBefore(toRow(element), toCol(element));}

        @Override
        public void remove() {itr.remove();}

        @Override
        public IntIterator clone() {throw new UnsupportedOperationException();}
      };
    }

    @Override
    public IntSet clone()
    {
      MatrixIntSet res = new MatrixIntSet();
      res.matrix = matrix.clone();
      return res;
    }

    @Override
    public int hashCode() {return matrix.hashCode();}

    @Override
    public boolean equals(Object obj) {return matrix.equals(((MatrixIntSet) obj).matrix);}

    @Override
    public String debugInfo()
    {
      return super.toString() + "\n" + matrix.debugInfo();
    }
  }
}

