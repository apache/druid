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


package io.druid.extendedset.wrappers.matrix;

import io.druid.extendedset.AbstractExtendedSet;
import io.druid.extendedset.ExtendedSet;
import io.druid.extendedset.intset.IntSet;
import io.druid.extendedset.wrappers.IndexedSet;
import io.druid.extendedset.wrappers.IntegerSet;
import io.druid.extendedset.wrappers.matrix.BinaryMatrix.CellIterator;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A set of pairs internally represented by a binary matrix. <p> This class can be used to represent a set of transactions, where each transaction is a set of items. Rows are transactions, columns are the items involved with each transaction.
 *
 * @param < T  >  transaction type
 * @param < I  >  item type
 *
 * @author Alessandro Colantonio
 * @version $Id: PairSet.java 153 2011-05-30 16:39:57Z cocciasik $
 * @see Pair
 * @see IntSet
 */
public class PairSet<T, I> extends AbstractExtendedSet<Pair<T, I>> implements Serializable
{
  /**
   * generated serial ID
   */
  private static final long serialVersionUID = 7902458899512666217L;

  /**
   * binary matrix
   *
   * @uml.property name="matrix"
   * @uml.associationEnd
   */
  private final BinaryMatrix matrix;

  /**
   * all possible transactions
   *
   * @uml.property name="allTransactions"
   * @uml.associationEnd
   */
  private final IndexedSet<T> allTransactions;

  /**
   * all possible items
   *
   * @uml.property name="allItems"
   * @uml.associationEnd
   */
  private final IndexedSet<I> allItems;

  /**
   * Initializes the set by specifying all possible transactions and items.
   *
   * @param matrix       {@link BinaryMatrix} instance used to internally represent the matrix
   * @param transactions collection of <i>all</i> possible transactions. The specified
   *                     order will be preserved within when iterating over the
   *                     {@link PairSet} instance.
   * @param items        collection of <i>all</i> possible items. The specified order
   *                     will be preserved within each transaction {@link PairSet}.
   */
  public PairSet(BinaryMatrix matrix, Collection<T> transactions, Collection<I> items)
  {
    if (transactions == null || items == null) {
      throw new NullPointerException();
    }
    this.matrix = matrix;

    IntSet tmp = matrix.emptyRow();
    if (transactions instanceof IndexedSet<?>) {
      allTransactions = (IndexedSet<T>) transactions;
    } else {
      allTransactions = new IndexedSet<T>(tmp.empty(), transactions).universe(); //.unmodifiable();
    }
    if (items instanceof IndexedSet<?>) {
      allItems = (IndexedSet<I>) items;
    } else {
      allItems = new IndexedSet<I>(tmp.empty(), items).universe(); //.unmodifiable();
    }
  }

  /**
   * Initializes the set by specifying all possible transactions and items.
   *
   * @param matrix {@link BinaryMatrix} instance used to internally represent the
   *               matrix
   * @param pairs  arrays <code>n x 2</code> of pairs of transactions (first) and items (second).
   */
  public PairSet(BinaryMatrix matrix, final Object[][] pairs)
  {
    this(matrix, new AbstractCollection<Pair<T, I>>()
    {
      @Override
      public Iterator<Pair<T, I>> iterator()
      {
        return new Iterator<Pair<T, I>>()
        {
          int i = 0;

          @SuppressWarnings("unchecked")
          @Override
          public Pair<T, I> next() {return new Pair(pairs[i][0], pairs[i++][1]);}

          @Override
          public boolean hasNext() {return i < pairs.length;}

          @Override
          public void remove() {throw new UnsupportedOperationException();}
        };
      }

      @Override
      public int size() {return pairs.length;}
    });
  }

  /**
   * Converts a generic collection of transaction-item pairs to a
   * {@link PairSet} instance.
   *
   * @param matrix {@link IntSet} instance used to internally represent the set
   * @param pairs  collection of {@link Pair} instances
   */
  public PairSet(BinaryMatrix matrix, Collection<? extends Pair<T, I>> pairs)
  {
    if (pairs == null) {
      throw new RuntimeException("null pair set");
    }
    if (pairs.isEmpty()) {
      throw new RuntimeException("empty pair set");
    }

    // identify all possible transactions and items and their frequencies
    final Map<T, Integer> ts = new HashMap<T, Integer>();
    final Map<I, Integer> is = new HashMap<I, Integer>();
    for (Pair<T, I> p : pairs) {
      Integer f;

      f = ts.get(p.transaction);
      f = f == null ? 1 : f + 1;
      ts.put(p.transaction, f);

      f = is.get(p.item);
      f = f == null ? 1 : f + 1;
      is.put(p.item, f);
    }

    // sort transactions and items by descending frequencies
    List<Pair<T, I>> sortedPairs = new ArrayList<Pair<T, I>>(pairs);
    Collections.sort(sortedPairs, new Comparator<Pair<T, I>>()
    {
      @Override
      public int compare(Pair<T, I> o1, Pair<T, I> o2)
      {
        int r = ts.get(o2.transaction).compareTo(ts.get(o1.transaction));
        if (r == 0) {
          r = is.get(o2.item).compareTo(is.get(o1.item));
        }
        return r;
      }
    });
    List<T> sortedTransactions = new ArrayList<T>(ts.keySet());
    Collections.sort(sortedTransactions, new Comparator<T>()
    {
      @Override
      public int compare(T o1, T o2)
      {
        return ts.get(o2).compareTo(ts.get(o1));
      }
    });
    List<I> sortedItems = new ArrayList<I>(is.keySet());
    Collections.sort(sortedItems, new Comparator<I>()
    {
      @Override
      public int compare(I o1, I o2)
      {
        return is.get(o2).compareTo(is.get(o1));
      }
    });

    // identify all transactions and items
    this.matrix = matrix;
    matrix.add(0, 0);
    allTransactions = new IndexedSet<T>(matrix.getRow(0), sortedTransactions).universe(); // .unmodifiable();
    allItems = new IndexedSet<I>(matrix.getRow(0), sortedItems).universe(); // .unmodifiable();
    matrix.clear();

    // create the matrix
    for (Pair<T, I> p : sortedPairs) {
      add(p);
    }
  }

  /**
   * Wraps a {@link BinaryMatrix} instance with a {@link PairSet} instance.
   * <p>
   * <b>NOTE:</b> the maximum item and transaction IDs are those existing in
   * the binary matrix when the wrapping take place
   *
   * @param b a {@link BinaryMatrix} instance to wrap
   *
   * @return a new {@link PairSet} instance, indexed by the given matrix
   */
  public static PairSet<Integer, Integer> createFromBinaryMatrix(BinaryMatrix b)
  {
    // TODO this is a little bit costly since PairSet will allocate an array
    // and a HashMap of Integers to map elements of BinaryMatrix...
    // Think about a IntegerPairSet class or to an "fake" IntegerIndexedSet
    // just for this purpose.

    IntegerSet t = new IntegerSet(b.emptyRow());
    t.intSet().add(b.maxRow() + 1);
    t.intSet().complement();

    IntegerSet i = new IntegerSet(b.emptyRow());
    i.intSet().add(b.maxCol() + 1);
    i.intSet().complement();

    return new PairSet<Integer, Integer>(b, t, i);
  }

  /**
   * maps a transaction to its index and returns -1 if not found
   */
  private int transactionToIndex(T t)
  {
    Integer r = allTransactions.absoluteIndexOf(t);
    return r == null ? -1 : r.intValue();
  }

  /**
   * maps an item to its index and returns -1 if not found
   */
  private int itemToIndex(I i)
  {
    Integer r = allItems.absoluteIndexOf(i);
    return r == null ? -1 : r.intValue();
  }

  /**
   * maps a pair of indices to the corresponding {@link Pair}
   */
  private Pair<T, I> indexToPair(int[] i)
  {
    return new Pair<T, I>(allTransactions.absoluteGet(i[0]), allItems.absoluteGet(i[1]));
  }

  /**
   * A shortcut for <code>new PairSet&lt;T, I&gt;(matrix, mapping)</code>
   *
   * @param bm {@link BinaryMatrix} instance to link
   *
   * @return the new {@link PairSet} with the given {@link BinaryMatrix}
   * instance and the same mapping of this
   */
  private PairSet<T, I> createFromIndices(BinaryMatrix bm)
  {
    return new PairSet<T, I>(bm, allTransactions, allItems);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PairSet<T, I> clone()
  {
    return createFromIndices(matrix.clone());
  }

  /**
   * Checks if the given collection is a instance of {@link PairSet} with
   * the same index mappings
   *
   * @param c collection to check
   *
   * @return <code>true</code> if the given collection is a instance of
   * {@link PairSet} with the same index mappings
   */
  private boolean hasSameIndices(Collection<?> c)
  {
    return c != null
           && (c instanceof PairSet)
           && (allTransactions == ((PairSet) c).allTransactions)
           && (allItems == ((PairSet) c).allItems);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean add(Pair<T, I> e)
  {
    return add(e.transaction, e.item);
  }

  /**
   * Adds a single transaction-item pair
   *
   * @param transaction the transaction of the pair
   * @param item        the item of the pair
   *
   * @return <code>true</code> if the set has been changed
   */
  public boolean add(T transaction, I item)
  {
    return matrix.add(transactionToIndex(transaction), itemToIndex(item));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean addAll(Collection<? extends Pair<T, I>> c)
  {
    return matrix.addAll(convert(c).matrix);
  }

  /**
   * Add the pairs obtained from the Cartesian product of transactions
   * and items
   *
   * @param trans collection of transactions
   * @param items collection of items
   *
   * @return <code>true</code> if the set set has been changed
   */
  public boolean addAll(Collection<T> trans, Collection<I> items)
  {
    if (trans == null || trans.isEmpty() || items == null || items.isEmpty()) {
      return false;
    }
    return matrix.addAll(allTransactions.convert(trans).indices(), allItems.convert(items).indices());
  }

  /**
   * Add the pairs obtained from the Cartesian product of transactions
   * and items
   *
   * @param trans the given transaction
   * @param items collection of items
   *
   * @return <code>true</code> if the set set has been changed
   */
  public boolean addAll(T trans, Collection<I> items)
  {
    if (trans == null || items == null || items.isEmpty()) {
      return false;
    }
    return matrix.addAll(transactionToIndex(trans), allItems.convert(items).indices());
  }

  /**
   * Add the pairs obtained from the Cartesian product of transactions
   * and items
   *
   * @param trans collection of transactions
   * @param item  the given item
   *
   * @return <code>true</code> if the set set has been changed
   */
  public boolean addAll(Collection<T> trans, I item)
  {
    if (trans == null || trans.isEmpty() || item == null) {
      return false;
    }
    return matrix.addAll(allTransactions.convert(trans).indices(), itemToIndex(item));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear()
  {
    matrix.clear();
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public boolean contains(Object o)
  {
    return o != null
           && o instanceof Pair<?, ?>
           && contains(((Pair<T, I>) o).transaction, ((Pair<T, I>) o).item);
  }

  /**
   * Checks if the given transaction-item pair is contained within the set
   *
   * @param transaction the transaction of the pair
   * @param item        the item of the pair
   *
   * @return <code>true</code> if the given transaction-item pair is contained
   * within the set
   */
  public boolean contains(T transaction, I item)
  {
    int t = transactionToIndex(transaction);
    if (t < 0) {
      return false;
    }
    int i = itemToIndex(item);
    if (i < 0) {
      return false;
    }
    return matrix.contains(t, i);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAll(Collection<?> c)
  {
    return matrix.containsAll(convert(c).matrix);
  }

  /**
   * Checks if the pairs obtained from the Cartesian product of
   * transactions and items are contained
   *
   * @param trans collection of transactions
   * @param items collection of items
   *
   * @return <code>true</code> if the pairs set set has been changed
   */
  public boolean containsAll(Collection<T> trans, Collection<I> items)
  {
    if (trans == null || trans.isEmpty() || items == null || items.isEmpty()) {
      return true;
    }
    if (isEmpty()) {
      return false;
    }
    return matrix.containsAll(allTransactions.convert(trans).indices(), allItems.convert(items).indices());
  }

  /**
   * Checks if the pairs obtained from the Cartesian product of
   * transactions and items are contained
   *
   * @param trans the transaction
   * @param items collection of items
   *
   * @return <code>true</code> if the pairs set set has been changed
   */
  public boolean containsAll(T trans, Collection<I> items)
  {
    if (trans == null || items == null || items.isEmpty()) {
      return true;
    }
    if (isEmpty()) {
      return false;
    }
    return matrix.containsAll(transactionToIndex(trans), allItems.convert(items).indices());
  }

  /**
   * Checks if the pairs obtained from the Cartesian product of
   * transactions and items are contained
   *
   * @param trans collection of transactions
   * @param item  the item
   *
   * @return <code>true</code> if the pairs set set has been changed
   */
  public boolean containsAll(Collection<T> trans, I item)
  {
    if (trans == null || trans.isEmpty() || item == null) {
      return true;
    }
    if (isEmpty()) {
      return false;
    }
    return matrix.containsAll(allTransactions.convert(trans).indices(), itemToIndex(item));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty()
  {
    return matrix.isEmpty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExtendedIterator<Pair<T, I>> iterator()
  {
    return new ExtendedIterator<Pair<T, I>>()
    {
      CellIterator itr = matrix.iterator();

      @Override
      public Pair<T, I> next() {return indexToPair(itr.next());}

      @Override
      public boolean hasNext() {return itr.hasNext();}

      @Override
      public void remove() {itr.remove();}

      @Override
      public void skipAllBefore(Pair<T, I> element)
      {
        itr.skipAllBefore(
            transactionToIndex(element.transaction),
            itemToIndex(element.item)
        );
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExtendedIterator<Pair<T, I>> descendingIterator()
  {
    return new ExtendedIterator<Pair<T, I>>()
    {
      CellIterator itr = matrix.descendingIterator();

      @Override
      public Pair<T, I> next() {return indexToPair(itr.next());}

      @Override
      public boolean hasNext() {return itr.hasNext();}

      @Override
      public void remove() {itr.remove();}

      @Override
      public void skipAllBefore(Pair<T, I> element)
      {
        itr.skipAllBefore(
            transactionToIndex(element.transaction),
            itemToIndex(element.item)
        );
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(Object o)
  {
    return o instanceof Pair<?, ?>
           && remove(((Pair<T, I>) o).transaction, ((Pair<T, I>) o).item);
  }

  /**
   * Removes a single transaction-item pair
   *
   * @param transaction the transaction of the pair
   * @param item        the item of the pair
   *
   * @return <code>true</code> if the pair set has been changed
   */
  public boolean remove(T transaction, I item)
  {
    return matrix.remove(transactionToIndex(transaction), itemToIndex(item));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean removeAll(Collection<?> c)
  {
    return matrix.removeAll(convert(c).matrix);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean retainAll(Collection<?> c)
  {
    return matrix.retainAll(convert(c).matrix);
  }

  /**
   * Removes the pairs obtained from the Cartesian product of transactions and
   * items
   *
   * @param trans collection of transactions
   * @param items collection of items
   *
   * @return <code>true</code> if the set set has been changed
   */
  public boolean removeAll(Collection<T> trans, Collection<I> items)
  {
    if (trans == null || trans.isEmpty() || items == null || items.isEmpty()) {
      return false;
    }
    return matrix.removeAll(allTransactions.convert(trans).indices(), allItems.convert(items).indices());
  }

  /**
   * Removes the pairs obtained from the Cartesian product of transactions and
   * items
   *
   * @param trans a transaction
   * @param items collection of items
   *
   * @return <code>true</code> if the set set has been changed
   */
  public boolean removeAll(T trans, Collection<I> items)
  {
    if (trans == null || items == null || items.isEmpty()) {
      return false;
    }
    return matrix.removeAll(transactionToIndex(trans), allItems.convert(items).indices());
  }

  /**
   * Removes the pairs obtained from the Cartesian product of transactions and
   * items
   *
   * @param trans collection of transactions
   * @param item  collection of items
   *
   * @return <code>true</code> if the set set has been changed
   */
  public boolean removeAll(Collection<T> trans, I item)
  {
    if (trans == null || trans.isEmpty() || item == null) {
      return false;
    }
    return matrix.removeAll(allTransactions.convert(trans).indices(), itemToIndex(item));
  }

  /**
   * Retains the pairs obtained from the Cartesian product of transactions and
   * items
   *
   * @param trans collection of transactions
   * @param items collection of items
   *
   * @return <code>true</code> if the set set has been changed
   */
  public boolean retainAll(Collection<T> trans, Collection<I> items)
  {
    if (isEmpty()) {
      return false;
    }
    if (trans == null || trans.isEmpty() || items == null || items.isEmpty()) {
      clear();
      return true;
    }
    return matrix.retainAll(allTransactions.convert(trans).indices(), allItems.convert(items).indices());
  }

  /**
   * Retains the pairs obtained from the Cartesian product of transactions and
   * items
   *
   * @param trans the transaction
   * @param items collection of items
   *
   * @return <code>true</code> if the set set has been changed
   */
  public boolean retainAll(T trans, Collection<I> items)
  {
    if (isEmpty()) {
      return false;
    }
    if (trans == null || items == null || items.isEmpty()) {
      clear();
      return true;
    }
    return matrix.retainAll(transactionToIndex(trans), allItems.convert(items).indices());
  }

  /**
   * Retains the pairs obtained from the Cartesian product of transactions and
   * items
   *
   * @param trans collection of transactions
   * @param item  the item
   *
   * @return <code>true</code> if the set set has been changed
   */
  public boolean retainAll(Collection<T> trans, I item)
  {
    if (isEmpty()) {
      return false;
    }
    if (trans == null || trans.isEmpty() || item == null) {
      clear();
      return true;
    }
    return matrix.retainAll(allTransactions.convert(trans).indices(), itemToIndex(item));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size()
  {
    return matrix.size();
  }

  /**
   * Gets the set of all possible transactions that can be contained within
   * the set
   *
   * @return the set of all possible transactions that can be contained within
   * the set
   */
  public IndexedSet<T> allTransactions()
  {
    return allTransactions;
  }

  /**
   * Gets the set of all possible items that can be contained within each
   * transaction
   *
   * @return the set of all possible items that can be contained within each
   * transaction
   */
  public IndexedSet<I> allItems()
  {
    return allItems;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    return matrix.hashCode();
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
    if (!(obj instanceof PairSet<?, ?>)) {
      return false;
    }
    final PairSet<?, ?> other = (PairSet<?, ?>) obj;
    return hasSameIndices(other) && matrix.equals(other.matrix);
  }

  /**
   * Lists all items contained within a given transaction
   *
   * @param transaction the given transaction
   *
   * @return items contained within the given transaction
   */
  public IndexedSet<I> itemsOf(T transaction)
  {
    IndexedSet<I> res = allItems.empty();
    res.indices().addAll(matrix.getRow(transactionToIndex(transaction)));
    return res;
  }

  /**
   * Lists all transactions involved with a specified item
   *
   * @param item the given item
   *
   * @return transactions involved with a specified item
   */
  public IndexedSet<T> transactionsOf(I item)
  {
    IndexedSet<T> res = allTransactions.empty();
    res.indices().addAll(matrix.getCol(itemToIndex(item)));
    return res;
  }

  /**
   * Gets the set of transactions in {@link #allTransactions()} that contains
   * at least one item
   *
   * @return the set of transactions in {@link #allTransactions()} that
   * contains at least one item
   */
  public IndexedSet<T> involvedTransactions()
  {
    IndexedSet<T> res = allTransactions.empty();
    res.indices().addAll(matrix.involvedRows());
    return res;
  }

  /**
   * Gets the set of items in {@link #allItems()} that are contained in at
   * least one transaction
   *
   * @return the set of items in {@link #allItems()} that are contained in at
   * least one transaction
   */
  public IndexedSet<I> involvedItems()
  {
    IndexedSet<I> res = allItems.empty();
    res.indices().addAll(matrix.involvedCols());
    return res;
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
  @Override
  public Pair<T, I> get(int index)
  {
    return indexToPair(matrix.get(index));
  }

  /**
   * Provides position of element within the set.
   * <p>
   * It returns -1 if the element does not exist within the set.
   *
   * @param element element of the set
   *
   * @return the element position
   */
  @Override
  public int indexOf(Pair<T, I> element)
  {
    return matrix.indexOf(
        transactionToIndex(element.transaction),
        itemToIndex(element.item)
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String debugInfo()
  {
    StringBuilder s = new StringBuilder();

    s.append("possible transactions: ");
    s.append(allTransactions);
    s.append('\n');
    s.append("possible items: ");
    s.append(allItems);
    s.append('\n');

    s.append("pairs:\n");
    s.append(matrix.toString());
    s.append("info: " + matrix.debugInfo());

    return s.toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double bitmapCompressionRatio()
  {
    return matrix.bitmapCompressionRatio();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double collectionCompressionRatio()
  {
    return matrix.collectionCompressionRatio();
  }

  /**
   * Returns the set of indices. Modifications to this set are reflected to
   * this {@link PairSet} instance. Trying to perform operation on
   * out-of-bound indices will throw an {@link IllegalArgumentException}
   * exception.
   *
   * @return the index set
   */
  public BinaryMatrix matrix()
  {
    return matrix;
  }

//	/**
//	 * Extracts a subset represented by a certain range of transactions and
//	 * items, according to the ordering provided by {@link #allTransactions()}
//	 * and {@link #allItems()}.
//	 * 
//	 * @param fromTransaction
//	 *            the first transaction of the range (if <code>null</code> it
//	 *            represents the first one)
//	 * @param toTransaction
//	 *            the last transaction of the range (if <code>null</code> it
//	 *            represents the last one)
//	 * @param fromItem
//	 *            the first item of the range (if <code>null</code> it
//	 *            represents the first one)
//	 * @param toItem
//	 *            the last item of the range (if <code>null</code> it represents
//	 *            the last one)
//	 * @return the specified subset
//	 */
//	public PairSet<T, I> subSet(T fromTransaction, T toTransaction, I fromItem, I toItem) {
//		BinaryMatrix mask = matrix.empty();
//		mask.fill(
//				transactionToIndex(fromTransaction), 
//				itemToIndex(fromItem), 
//				transactionToIndex(toTransaction), 
//				itemToIndex(toItem));
//		return new PairSet<T, I>(matrix.intersection(mask), allTransactions, allItems);
//	}
//
//	/**
//	 * Extracts a subset represented by a collection of transactions and items
//	 * 
//	 * @param involvedTransactions
//	 *            involved transactions (if <code>null</code>, it represents all
//	 *            transactions in {@link #allTransactions()})
//	 * @param involvedItems
//	 *            involved items (if <code>null</code>, it represents all items
//	 *            in {@link #allItems()})
//	 * @return all the transaction-item pairs that represent the specified
//	 *         subset
//	 */
//	public PairSet<T, I> subSet(Collection<T> involvedTransactions, Collection<I> involvedItems) {
//		BinaryMatrix mask = matrix.empty();
//		mask.addAll(
//				allTransactions.convert(involvedTransactions).indices(), 
//				allItems.convert(involvedItems).indices());
//		return new PairSet<T, I>(matrix.intersection(mask), allTransactions, allItems);
//	}

  /**
   * {@inheritDoc}
   */
  @Override
  public PairSet<T, I> empty()
  {
    return createFromIndices(matrix.empty());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void complement()
  {
    matrix.complement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Comparator<? super Pair<T, I>> comparator()
  {
    return new Comparator<Pair<T, I>>()
    {
      @Override
      public int compare(Pair<T, I> o1, Pair<T, I> o2)
      {
        int t1 = transactionToIndex(o1.transaction);
        int t2 = transactionToIndex(o2.transaction);
        int r = t1 < t2 ? -1 : (t1 == t2 ? 0 : 1);
        if (r == 0) {
          int i1 = itemToIndex(o1.item);
          int i2 = itemToIndex(o2.item);
          r = i1 < i2 ? -1 : (i1 == i2 ? 0 : 1);
        }
        return r;
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public PairSet<T, I> convert(Collection<?> c)
  {
    if (c == null) {
      return empty();
    }

    // useless to convert...
    if (hasSameIndices(c)) {
      return (PairSet<T, I>) c;
    }

    // convert
    PairSet<T, I> res = empty();
    for (Pair<T, I> p : (Collection<Pair<T, I>>) c) {
      res.matrix.add(transactionToIndex(p.transaction), itemToIndex(p.item));
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public PairSet<T, I> convert(Object... e)
  {
    return (PairSet<T, I>) super.convert(e);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear(Pair<T, I> from, Pair<T, I> to)
  {
    matrix.clear(
        transactionToIndex(from.transaction),
        itemToIndex(from.item),
        transactionToIndex(to.transaction),
        itemToIndex(to.item)
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int complementSize()
  {
    return matrix.complementSize();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PairSet<T, I> complemented()
  {
    return createFromIndices(matrix.complemented());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PairSet<T, I> difference(Collection<? extends Pair<T, I>> other)
  {
    return other == null ? clone() : createFromIndices(matrix.difference(convert(other).matrix));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAny(Collection<? extends Pair<T, I>> other)
  {
    return other == null || matrix.containsAny(convert(other).matrix);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsAtLeast(Collection<? extends Pair<T, I>> other, int minElements)
  {
    return other != null && !other.isEmpty() && matrix.containsAtLeast(convert(other).matrix, minElements);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int differenceSize(Collection<? extends Pair<T, I>> other)
  {
    return other == null ? (int) size() : (int) matrix.differenceSize(convert(other).matrix);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void fill(Pair<T, I> from, Pair<T, I> to)
  {
    matrix.fill(
        transactionToIndex(from.transaction),
        itemToIndex(from.item),
        transactionToIndex(to.transaction),
        itemToIndex(to.item)
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flip(Pair<T, I> e)
  {
    matrix.flip(
        transactionToIndex(e.transaction),
        itemToIndex(e.item)
    );
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public PairSet<T, I> subSet(Pair<T, I> fromElement, Pair<T, I> toElement)
  {
    return (PairSet<T, I>) super.subSet(fromElement, toElement);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public PairSet<T, I> headSet(Pair<T, I> toElement)
  {
    return (PairSet<T, I>) super.headSet(toElement);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public PairSet<T, I> tailSet(Pair<T, I> fromElement)
  {
    return (PairSet<T, I>) super.tailSet(fromElement);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PairSet<T, I> intersection(Collection<? extends Pair<T, I>> c)
  {
    return c == null ? empty() : createFromIndices(matrix.intersection(convert(c).matrix));
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<? extends PairSet<T, I>> powerSet()
  {
    return (List<? extends PairSet<T, I>>) super.powerSet();
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<? extends PairSet<T, I>> powerSet(int min, int max)
  {
    return (List<? extends PairSet<T, I>>) super.powerSet(min, max);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PairSet<T, I> symmetricDifference(Collection<? extends Pair<T, I>> other)
  {
    return other == null ? clone() : createFromIndices(matrix.symmetricDifference(convert(other).matrix));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int symmetricDifferenceSize(Collection<? extends Pair<T, I>> other)
  {
    return other == null ? (int) size() : (int) matrix.symmetricDifferenceSize(convert(other).matrix);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PairSet<T, I> union(Collection<? extends Pair<T, I>> other)
  {
    return other == null ? clone() : createFromIndices(matrix.union(convert(other).matrix));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int unionSize(Collection<? extends Pair<T, I>> other)
  {
    return other == null ? (int) size() : (int) matrix.unionSize(convert(other).matrix);
  }

//	/**
//	 * {@inheritDoc}
//	 */
//	@Override
//	public PairSet<T, I> unmodifiable() {
//		return new PairSet<T, I>(allTransactions, allItems, maxTransactionCount, maxItemCount, indices.unmodifiable());
//	}

  /**
   * {@inheritDoc}
   */
  @Override
  public Pair<T, I> first()
  {
    return indexToPair(matrix.first());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Pair<T, I> last()
  {
    return indexToPair(matrix.last());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(ExtendedSet<Pair<T, I>> o)
  {
    return matrix.compareTo(convert(o).matrix);
  }

  /**
   * @return a transposed {@link PairSet} instance
   */
  public PairSet<I, T> transposed()
  {
    return new PairSet<I, T>(matrix.transposed(), allItems, allTransactions);
  }

  /**
   * Creates a new {@link PairSet} instance with the union of all possible
   * transactions and items as result for {@link #allTransactions()} and
   * {@link #allItems()}, respectively, and the union of pairs.
   *
   * @param other the other {@link PairSet} instance to merge
   *
   * @return the merged {@link PairSet} instance
   */
  public PairSet<T, I> merged(PairSet<T, I> other)
  {
    if (other == null) {
      return clone();
    }

    // compute the new universe
    Set<T> newAllTransactions = new LinkedHashSet<T>(allTransactions);
    Set<I> newAllItems = new LinkedHashSet<I>(allItems);
    newAllTransactions.addAll(other.allTransactions);
    newAllItems.addAll(other.allItems);

    // compute the union of pairs
    PairSet<T, I> res = new PairSet<T, I>(
        matrix.clone(),
        newAllTransactions,
        newAllItems
    );
    if (!other.isEmpty()) {
      res.addAll(other);
    }
    return res;
  }

  /**
   * Creates a new {@link PairSet} instance with only non-empty transactions
   * and items.
   *
   * @return the compacted {@link PairSet} instance
   */
  public PairSet<T, I> compacted()
  {
    // trivial case
    if (isEmpty()) {
      return empty();
    }

    // compute the new universe
    final Set<T> newAllTransactions = new LinkedHashSet<T>(involvedTransactions());
    final Set<I> newAllItems = new LinkedHashSet<I>(involvedItems());
    if (newAllTransactions.size() == allTransactions.size()
        && newAllItems.size() == allItems.size()) {
      return clone();
    }

    // compute the union of pairs
    PairSet<T, I> res = new PairSet<T, I>(
        matrix.empty(),
        newAllTransactions,
        newAllItems
    );
    res.addAll(this);
    return res;
  }


//	//
//	// COMPRESSED OBJECT SERIALIZATION
//	//
//	
//	private static class ZipObjectOutputStream extends ObjectOutputStream {
//		private GZIPOutputStream out;
//		ZipObjectOutputStream(ObjectOutputStream out) throws IOException {this(new GZIPOutputStream(out));}
//		ZipObjectOutputStream(GZIPOutputStream out) throws IOException {super(out); this.out = out;}
//		@Override public void close() throws IOException {out.flush(); out.finish();}
//	}
//	
//	private static class ZipObjectInputStream extends ObjectInputStream {
//		ZipObjectInputStream(ObjectInputStream in) throws IOException {super(new GZIPInputStream(in));}
//	}
//	
//    private void writeObject(ObjectOutputStream out) throws IOException {
//		if (out instanceof ZipObjectOutputStream) {
//			out.defaultWriteObject();
//		} else {
//			ObjectOutputStream oos = new ZipObjectOutputStream(out);
//			oos.writeObject(this);
//			oos.close();
//		}
//    }
//
//    private transient Object serialize;
//
//	@SuppressWarnings("unused")
//	private Object readResolve() throws ObjectStreamException {
//		if (serialize == null) 
//			serialize = this;
//		return serialize;
//	}
//	
//    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
//		if (in instanceof ZipObjectInputStream) {
//			in.defaultReadObject();
//		} else {
//			ObjectInputStream ois = new ZipObjectInputStream(in);
//			serialize = ois.readObject();
//		}
//	}
}
