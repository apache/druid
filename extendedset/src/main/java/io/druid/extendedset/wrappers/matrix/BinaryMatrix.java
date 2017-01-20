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

import io.druid.extendedset.intset.IntSet;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Very similar to  {@link IntSet}  but for pairs of <code>int</code>s, that is a binary matrix
 *
 * @author Alessandro Colantonio
 * @version $Id$
 * @see IntSet
 */
public class BinaryMatrix implements Cloneable, Comparable<BinaryMatrix>
{
  /**
   * set of all rows
   */
  private final List<IntSet> rows = new ArrayList<IntSet>();

  /**
   * {@link IntSet}   instance to create empty rows
   *
   * @uml.property name="template"
   * @uml.associationEnd
   */
  private final IntSet template;

  /**
   * used to cache the returned value
   */
  private final int[] resultCache = new int[2];

  /**
   * Creates an empty matrix. The matrix is internally represented by putting
   * rows (transactions) in sequence. The provided constructor allows to
   * specify which {@link IntSet} instance must be used to internally
   * represent rows.
   *
   * @param template {@link IntSet} instance to create empty rows
   */
  public BinaryMatrix(IntSet template)
  {
    this.template = template;
  }

  /**
   * @return {@link IntSet} instance internally used to represent rows
   */
  public IntSet emptyRow()
  {
    return template.empty();
  }

  /**
   * Remove <code>null</code> cells at the end of {@link #rows}
   */
  private void fixRows()
  {
    int last = rows.size() - 1;
    while (last >= 0 && rows.get(last) == null) {
      rows.remove(last--);
    }
  }

  /**
   * Generates the intersection matrix
   *
   * @param other {@link BinaryMatrix} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #retainAll(BinaryMatrix)
   */
  public BinaryMatrix intersection(BinaryMatrix other)
  {
    BinaryMatrix res = empty();
    final int rowCount = Math.min(rows.size(), other.rows.size());
    for (int i = 0; i < rowCount; i++) {
      IntSet s1 = rows.get(i);
      IntSet s2 = other.rows.get(i);
      if (s1 == null || s2 == null) {
        res.rows.add(null);
      } else {
        IntSet r = s1.intersection(s2);
        if (r.isEmpty()) {
          res.rows.add(null);
        } else {
          res.rows.add(r);
        }
      }
      assert res.rows.get(i) == null || !res.rows.get(i).isEmpty();
    }
    res.fixRows();
    return res;
  }

  /**
   * Generates the union matrix
   *
   * @param other {@link BinaryMatrix} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #addAll(BinaryMatrix)
   */
  public BinaryMatrix union(BinaryMatrix other)
  {
    BinaryMatrix res = empty();
    final int rowCount = Math.min(rows.size(), other.rows.size());
    int i = 0;
    for (; i < rowCount; i++) {
      IntSet s1 = rows.get(i);
      IntSet s2 = other.rows.get(i);
      if (s1 == null) {
        if (s2 == null) {
          res.rows.add(null);
        } else {
          res.rows.add(s2.clone());
        }
      } else {
        if (s2 == null) {
          res.rows.add(s1.clone());
        } else {
          res.rows.add(s1.union(s2));
        }
      }
      assert res.rows.get(i) == null || !res.rows.get(i).isEmpty();
    }
    for (; i < rows.size(); i++) {
      IntSet s = rows.get(i);
      res.rows.add(s == null ? null : s.clone());
      assert res.rows.get(i) == null || !res.rows.get(i).isEmpty();
    }
    for (; i < other.rows.size(); i++) {
      IntSet s = other.rows.get(i);
      res.rows.add(s == null ? null : s.clone());
      assert res.rows.get(i) == null || !res.rows.get(i).isEmpty();
    }
    return res;
  }

  /**
   * Generates the difference matrix
   *
   * @param other {@link BinaryMatrix} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #removeAll(BinaryMatrix)
   */
  public BinaryMatrix difference(BinaryMatrix other)
  {
    BinaryMatrix res = empty();
    final int rowCount = Math.min(rows.size(), other.rows.size());
    int i = 0;
    for (; i < rowCount; i++) {
      IntSet s1 = rows.get(i);
      IntSet s2 = other.rows.get(i);
      if (s1 == null) {
        res.rows.add(null);
      } else {
        if (s2 == null) {
          res.rows.add(s1.clone());
        } else {
          IntSet r = s1.difference(s2);
          res.rows.add(r.isEmpty() ? null : r);
        }
      }
      assert res.rows.get(i) == null || !res.rows.get(i).isEmpty();
    }
    for (; i < rows.size(); i++) {
      IntSet s = rows.get(i);
      res.rows.add(s == null ? null : s.clone());
      assert res.rows.get(i) == null || !res.rows.get(i).isEmpty();
    }
    res.fixRows();
    return res;
  }

  /**
   * Generates the symmetric difference matrix
   *
   * @param other {@link BinaryMatrix} instance that represents the right
   *              operand
   *
   * @return the result of the operation
   *
   * @see #flip(int, int)
   */
  public BinaryMatrix symmetricDifference(BinaryMatrix other)
  {
    BinaryMatrix res = empty();
    final int rowCount = Math.min(rows.size(), other.rows.size());
    int i = 0;
    for (; i < rowCount; i++) {
      IntSet s1 = rows.get(i);
      IntSet s2 = other.rows.get(i);
      if (s1 == null) {
        if (s2 == null) {
          res.rows.add(null);
        } else {
          res.rows.add(s2.clone());
        }
      } else {
        if (s2 == null) {
          res.rows.add(s1.clone());
        } else {
          res.rows.add(s1.symmetricDifference(s2));
        }
      }
      assert res.rows.get(i) == null || !res.rows.get(i).isEmpty();
    }
    for (; i < rows.size(); i++) {
      IntSet s = rows.get(i);
      res.rows.add(s == null ? null : s.clone());
      assert res.rows.get(i) == null || !res.rows.get(i).isEmpty();
    }
    for (; i < other.rows.size(); i++) {
      IntSet s = other.rows.get(i);
      res.rows.add(s == null ? null : s.clone());
      assert res.rows.get(i) == null || !res.rows.get(i).isEmpty();
    }
    res.fixRows();
    return res;
  }

  /**
   * Generates the complement matrix, namely flipping all the cells.
   *
   * @return the complement matrix
   *
   * @see BinaryMatrix#complement()
   */
  public BinaryMatrix complemented()
  {
    BinaryMatrix res = empty();

    final int maxCol = maxCol();

    for (int i = 0; i < rows.size(); i++) {
      IntSet s = rows.get(i);

      if (s == null) {
        s = template.empty();
        s.fill(0, maxCol);
      } else {
        s.add(maxCol + 1);
        s.complemented();
        if (s.isEmpty()) {
          s = null;
        }
      }

      res.rows.add(s);
    }

    res.fixRows();
    return res;
  }

  /**
   * Complements the current matrix.
   *
   * @see BinaryMatrix#complemented()
   */
  public void complement()
  {
    final int maxCol = maxCol();

    for (int i = 0; i < rows.size(); i++) {
      IntSet s = rows.get(i);

      if (s == null) {
        s = template.empty();
        s.fill(0, maxCol - 1);
        rows.set(i, s);
      } else {
        s.add(maxCol + 1);
        s.complement();
        if (s.isEmpty()) {
          rows.set(i, null);
        }
      }
    }

    fixRows();
  }

  /**
   * Returns <code>true</code> if the specified {@link BinaryMatrix} instance
   * contains any cell that is also contained within this {@link BinaryMatrix}
   * instance
   *
   * @param other {@link BinaryMatrix} to intersect with
   *
   * @return a boolean indicating whether this {@link BinaryMatrix} intersects
   * the specified {@link BinaryMatrix}.
   */
  public boolean containsAny(BinaryMatrix other)
  {
    final int rowCount = Math.min(rows.size(), other.rows.size());
    for (int i = 0; i < rowCount; i++) {
      IntSet s1 = rows.get(i);
      IntSet s2 = other.rows.get(i);
      if (s1 != null && s2 != null) {
        if (s1.containsAny(s2)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns <code>true</code> if the specified {@link BinaryMatrix} instance
   * contains at least <code>minElements</code> cells that are also contained
   * within this {@link BinaryMatrix} instance
   *
   * @param other    {@link BinaryMatrix} instance to intersect with
   * @param minCells minimum number of cells to be contained within this
   *                 {@link BinaryMatrix} instance
   *
   * @return a boolean indicating whether this {@link BinaryMatrix} intersects
   * the specified {@link BinaryMatrix}.
   *
   * @throws IllegalArgumentException if <code>minElements &lt; 1</code>
   */
  public boolean containsAtLeast(BinaryMatrix other, int minCells)
  {
    // special cases
    if (minCells < 1) {
      throw new IllegalArgumentException();
    }
    int size = size();
    if ((size < minCells) || other == null || other.isEmpty() || isEmpty()) {
      return false;
    }
    if (this == other) {
      return size >= minCells;
    }

    // exact count before the last row
    int res = 0;
    final int last = Math.min(rows.size(), other.rows.size()) - 1;
    for (int i = 0; i < last; i++) {
      IntSet s1 = rows.get(i);
      IntSet s2 = other.rows.get(i);
      if (s1 != null && s2 != null) {
        res += s1.intersectionSize(s2);
        if (res >= minCells) {
          return true;
        }
      }
    }

    // last row more efficient!
    IntSet l1 = rows.get(last);
    IntSet l2 = other.rows.get(last);
    if (l1 == null || l2 == null) {
      return false;
    }
    return l1.containsAtLeast(l2, minCells - res);
  }

  /**
   * Computes the intersection matrix size.
   * <p>
   * This is faster than calling {@link #intersection(BinaryMatrix)} and then
   * {@link #size()}
   *
   * @param other {@link BinaryMatrix} instance that represents the right
   *              operand
   *
   * @return the size
   */
  public int intersectionSize(BinaryMatrix other)
  {
    int res = 0;
    final int rowCount = Math.min(rows.size(), other.rows.size());
    for (int i = 0; i < rowCount; i++) {
      IntSet s1 = rows.get(i);
      IntSet s2 = other.rows.get(i);
      if (s1 != null && s2 != null) {
        res += s1.intersectionSize(s2);
      }
    }
    return res;
  }

  /**
   * Computes the union matrix size.
   * <p>
   * This is faster than calling {@link #union(BinaryMatrix)} and then
   * {@link #size()}
   *
   * @param other {@link BinaryMatrix} instance that represents the right
   *              operand
   *
   * @return the size
   */
  public int unionSize(BinaryMatrix other)
  {
    return other == null ? size() : size() + other.size() - intersectionSize(other);
  }

  /**
   * Computes the symmetric difference matrix size.
   * <p>
   * This is faster than calling {@link #symmetricDifference(BinaryMatrix)}
   * and then {@link #size()}
   *
   * @param other {@link BinaryMatrix} instance that represents the right
   *              operand
   *
   * @return the size
   */
  public int symmetricDifferenceSize(BinaryMatrix other)
  {
    return other == null ? size() : size() + other.size() - 2 * intersectionSize(other);
  }

  /**
   * Computes the difference matrix size.
   * <p>
   * This is faster than calling {@link #difference(BinaryMatrix)} and then
   * {@link #size()}
   *
   * @param other {@link BinaryMatrix} instance that represents the right
   *              operand
   *
   * @return the size
   */
  public int differenceSize(BinaryMatrix other)
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
  public int complementSize()
  {
    final int maxCol = maxCol();
    int res = 0;
    for (int i = 0; i < rows.size(); i++) {
      IntSet s = rows.get(i);
      res += maxCol + 1;
      if (s != null) {
        res -= s.size();
      }
    }
    return res;
  }

  /**
   * Generates an empty matrix of the same dimension
   *
   * @return the empty matrix
   */
  public BinaryMatrix empty()
  {
    return new BinaryMatrix(template);
  }

  /**
   * See the <code>clone()</code> of {@link Object}
   *
   * @return cloned object
   */
  @Override
  public BinaryMatrix clone()
  {
    BinaryMatrix res = empty();
    for (IntSet r : rows) {
      res.rows.add(r == null ? null : r.clone());
    }
    return res;
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
    throw new UnsupportedOperationException("TODO"); //TODO
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
    throw new UnsupportedOperationException("TODO"); //TODO
  }

  /**
   * @return a {@link CellIterator} instance to iterate over the matrix
   */
  public CellIterator iterator()
  {
    if (isEmpty()) {
      return new CellIterator()
      {
        @Override
        public boolean hasNext() {return false;}

        @Override
        public int[] next() {throw new NoSuchElementException();}

        @Override
        public void remove() {throw new IllegalStateException();}

        @Override
        public void skipAllBefore(int row, int col) {return;}
      };
    }

    return new CellIterator()
    {
      private final int[] itrResultCache = new int[2];
      int curRow = 0;
      IntSet.IntIterator curRowItr;

      {
        while (rows.get(curRow) == null) {
          curRow++;
        }
        curRowItr = rows.get(curRow).iterator();
        itrResultCache[0] = curRow;
      }

      @Override
      public int[] next()
      {
        if (!curRowItr.hasNext()) {
          IntSet s;
          while ((s = rows.get(++curRow)) == null) {/**/}
          curRowItr = s.iterator();
          itrResultCache[0] = curRow;
        }
        itrResultCache[1] = curRowItr.next();
        return itrResultCache;
      }

      @Override
      public boolean hasNext()
      {
        return curRow < rows.size() - 1 || curRowItr.hasNext();
      }

      @Override
      public void skipAllBefore(int row, int col)
      {
        throw new UnsupportedOperationException("TODO"); //TODO
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException("TODO"); //TODO
      }
    };
  }

  /**
   * @return a {@link CellIterator} instance to iterate over the matrix in
   * descending order
   */
  public CellIterator descendingIterator()
  {
    if (isEmpty()) {
      return new CellIterator()
      {
        @Override
        public boolean hasNext() {return false;}

        @Override
        public int[] next() {throw new NoSuchElementException();}

        @Override
        public void remove() {throw new IllegalStateException();}

        @Override
        public void skipAllBefore(int row, int col) {return;}
      };
    }

    return new CellIterator()
    {
      final int minRow;
      private final int[] itrResultCache = new int[2];
      int curRow = rows.size() - 1;
      IntSet.IntIterator curRowItr;

      {
        int m = 0;
        while (rows.get(m) == null) {
          m++;
        }
        minRow = m;
        curRowItr = rows.get(curRow).descendingIterator();
        itrResultCache[0] = curRow;
      }

      @Override
      public int[] next()
      {
        if (!curRowItr.hasNext()) {
          IntSet s;
          while ((s = rows.get(--curRow)) == null) {/**/}
          curRowItr = s.descendingIterator();
          itrResultCache[0] = curRow;
        }
        itrResultCache[1] = curRowItr.next();
        return itrResultCache;
      }

      @Override
      public boolean hasNext()
      {
        return curRow > minRow || curRowItr.hasNext();
      }

      @Override
      public void skipAllBefore(int row, int col)
      {
        throw new UnsupportedOperationException("TODO"); //TODO
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException("TODO"); //TODO
      }
    };
  }

  /**
   * Prints debug info about the given {@link BinaryMatrix} implementation
   *
   * @return a string that describes the internal representation of the
   * instance
   */
  public String debugInfo()
  {
    if (isEmpty()) {
      return "empty";
    }

    StringBuilder s = new StringBuilder();
    Formatter f = new Formatter(s);

    String format = String.format("%%%dd) ", (int) Math.log10(rows.size()) + 1);
    for (int i = 0; i < rows.size(); i++) {
      f.format(format, i);
      s.append(rows.get(i) == null ? "-" : rows.get(i).toString());
      s.append('\n');
    }

    return s.toString();
  }

  /**
   * Adds to the matrix all the cells of the specified sub-matrix, both
   * corners included.
   *
   * @param fromRow first row of the sub-matrix
   * @param fromCol first column of the sub-matrix
   * @param toRow   last row of the sub-matrix
   * @param toCol   last column of the sub-matrix
   */
  public void fill(int fromRow, int fromCol, int toRow, int toCol)
  {
    if (fromRow > toRow) {
      throw new IndexOutOfBoundsException("fromRow: " + fromRow + " > toRow: " + toRow);
    }
    if (fromCol > toCol) {
      throw new IndexOutOfBoundsException("fromCol: " + fromCol + " > toCol: " + toCol);
    }

    for (int r = rows.size(); r <= toRow; r++) {
      rows.add(null);
    }

    for (int r = fromRow; r <= toRow; r++) {
      IntSet s = rows.get(r);
      if (s == null) {
        rows.set(r, s = template.empty());
      }
      s.fill(fromCol, toCol);
    }
  }

  /**
   * Removes from the set all the cells of the specified sub-matrix, both
   * corners included.
   *
   * @param fromRow first row of the sub-matrix
   * @param fromCol first column of the sub-matrix
   * @param toRow   last row of the sub-matrix
   * @param toCol   last column of the sub-matrix
   */
  public void clear(int fromRow, int fromCol, int toRow, int toCol)
  {
    if (fromRow > toRow) {
      throw new IndexOutOfBoundsException("fromRow: " + fromRow + " > toRow: " + toRow);
    }
    if (fromCol > toCol) {
      throw new IndexOutOfBoundsException("fromCol: " + fromCol + " > toCol: " + toCol);
    }

    for (int r = Math.min(toRow, rows.size() - 1); r >= fromRow; r--) {
      IntSet s = rows.get(r);
      if (s == null) {
        continue;
      }
      s.clear(fromCol, toCol);
      if (s.isEmpty()) {
        rows.set(r, null);
      }
    }
    fixRows();
  }

  /**
   * Adds the cell if it not existing, or removes it if existing
   *
   * @param row row of the cell to flip
   * @param col column of the cell to flip
   *
   * @see #symmetricDifference(BinaryMatrix)
   */
  public void flip(int row, int col)
  {
    while (row >= rows.size()) {
      rows.add(null);
    }
    IntSet r = rows.get(row);
    if (r == null) {
      rows.set(row, r = template.empty());
    }
    r.flip(col);
    if (r.isEmpty()) {
      rows.set(row, null);
      fixRows();
    }
  }

  /**
   * Gets the <code>i</code><sup>th</sup> cell of the matrix.
   * <b>IMPORTANT</b>: each call returns an array of two elements, where the
   * first element is the row, while the second element is the column of the
   * current cell. In order to reduce the produced heap garbage, there is only
   * <i>one</i> array instantiated for each {@link BinaryMatrix} instance,
   * whose content is overridden at each method call.
   *
   * @param i position of the cell in the sorted matrix
   *
   * @return the <code>i</code><sup>th</sup> cell of the matrix, as a pair
   * &lt;row,column&gt;
   *
   * @throws IndexOutOfBoundsException if <code>i</code> is less than zero, or greater or equal to
   *                                   {@link #size()}
   */
  public int[] get(int i)
  {
    for (int r = 0; r < rows.size(); r++) {
      IntSet s = rows.get(r);
      if (s == null) {
        continue;
      }
      int ss = s.size();
      if (ss <= i) {
        i -= ss;
      } else {
        resultCache[0] = r;
        resultCache[1] = s.get(i);
        return resultCache;
      }
    }
    throw new NoSuchElementException();
  }

  /**
   * Provides position of cell within the matrix.
   * <p>
   * It returns -1 if the cell does not exist within the set.
   *
   * @param row row of the cell
   * @param col column of the cell
   *
   * @return the cell position
   */
  public int indexOf(int row, int col)
  {
    if (row >= rows.size() || rows.get(row) == null) {
      return -1;
    }
    int res = rows.get(row).indexOf(col);
    if (res == -1) {
      return -1;
    }
    for (int r = 0; r < row; r++) {
      IntSet s = rows.get(r);
      if (s == null) {
        continue;
      }
      res += s.size();
    }
    return res;
  }

  /**
   * Converts a given matrix of boolean <i>n</i> x <i>m</i> into an instance
   * of the current class.
   *
   * @param a array to use to generate the new instance
   *
   * @return the converted collection
   */
  public BinaryMatrix convert(boolean[][] a)
  {
    throw new UnsupportedOperationException("TODO"); //TODO
  }

  /**
   * Returns the first (lowest) cell currently in this set. <b>IMPORTANT</b>:
   * each call returns an array of two elements, where the first element is
   * the row, while the second element is the column of the current cell. In
   * order to reduce the produced heap garbage, there is only <i>one</i> array
   * instantiated for each {@link BinaryMatrix} instance, whose content is
   * overridden at each method call.
   *
   * @return the first (lowest) cell currently in this set
   *
   * @throws NoSuchElementException if this set is empty
   */
  public int[] first()
  {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }

    // find the first non-empty row
    int i = 0;
    IntSet s;
    while ((s = rows.get(i)) == null) {
      i++;
    }

    // prepare the result
    resultCache[0] = i;
    resultCache[1] = s.first();
    return resultCache;
  }

  /**
   * Returns the last (highest) cell currently in this set. <b>IMPORTANT</b>:
   * each call returns an array of two elements, where the first element is
   * the row, while the second element is the column of the current cell. In
   * order to reduce the produced heap garbage, there is only <i>one</i> array
   * instantiated for each {@link BinaryMatrix} instance, whose content is
   * overridden at each method call.
   *
   * @return the last (highest) cell currently in this set
   *
   * @throws NoSuchElementException if this set is empty
   */
  public int[] last()
  {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    resultCache[0] = rows.size() - 1;
    resultCache[1] = rows.get(rows.size() - 1).last();
    return resultCache;
  }

  /**
   * @return the number of cells in this matrix (its cardinality)
   */
  public int size()
  {
    int res = 0;
    for (IntSet s : rows) {
      if (s != null) {
        res += s.size();
      }
    }
    return res;
  }

  /**
   * @return <tt>true</tt> if this matrix contains no cells
   */
  public boolean isEmpty()
  {
    return rows.isEmpty();
  }

  /**
   * Returns <tt>true</tt> if this set contains the specified cell.
   *
   * @param row row of the cell
   * @param col column of the cell
   *
   * @return <tt>true</tt> if this matrix contains the specified cell
   */
  public boolean contains(int row, int col)
  {
    return row >= 0 && col >= 0 && row < rows.size()
           && rows.get(row) != null && rows.get(row).contains(col);
  }

  /**
   * Adds the specified cell to this matrix if it is not already present. It
   * ensures that matrices never contain duplicate cells.
   *
   * @param row row of the cell
   * @param col column of the cell
   *
   * @return <tt>true</tt> if this matrix did not already contain the
   * specified cell
   *
   * @throws IllegalArgumentException if some property of the specified cell prevents it from being
   *                                  added to this matrix
   */
  public boolean add(int row, int col)
  {
    while (row >= rows.size()) {
      rows.add(null);
    }
    IntSet r = rows.get(row);
    if (r == null) {
      rows.set(row, r = template.empty());
    }
    return r.add(col);
  }

  /**
   * Adds the specified cells to this matrix, if not already present. The
   * cells are represented by a given row and a set of columns.
   *
   * @param row  index of the row
   * @param cols indices of the columns
   *
   * @return <tt>true</tt> if this matrix did not already contain the
   * specified cells
   *
   * @throws IllegalArgumentException if some property of the specified cell prevents it from being
   *                                  added to this matrix
   */
  public boolean addAll(int row, IntSet cols)
  {
    while (row >= rows.size()) {
      rows.add(null);
    }
    IntSet r = rows.get(row);
    if (r == null) {
      rows.set(row, r = template.empty());
    }
    return r.addAll(cols);
  }

  /**
   * Adds the specified cells to this matrix, if not already present. The
   * cells are represented by a given set of rows and a given column
   *
   * @param rowSet indices of the rows
   * @param col    index of the column
   *
   * @return <tt>true</tt> if this matrix did not already contain the
   * specified cells
   *
   * @throws IllegalArgumentException if some property of the specified cell prevents it from being
   *                                  added to this matrix
   */
  public boolean addAll(IntSet rowSet, int col)
  {
    if (rowSet == null || rowSet.isEmpty()) {
      return false;
    }

    // prepare the space
    final int l = rowSet.last();
    while (l >= rows.size()) {
      rows.add(null);
    }

    boolean res = false;
    IntSet.IntIterator itr = rowSet.iterator();
    while (itr.hasNext()) {
      int r = itr.next();
      IntSet s = rows.get(r);
      if (s == null) {
        rows.set(r, template.convert(col));
        res = true;
      } else {
        res |= s.add(col);
      }
    }
    return res;
  }

  /**
   * Adds the specified cells to this matrix, if not already present. The
   * cells are represented by the Cartesian product of a given set of rows and
   * columns
   *
   * @param rowSet indices of the rows
   * @param colSet indices of the columns
   *
   * @return <tt>true</tt> if this matrix did not already contain the
   * specified cells
   *
   * @throws IllegalArgumentException if some property of the specified cell prevents it from being
   *                                  added to this matrix
   */
  public boolean addAll(IntSet rowSet, IntSet colSet)
  {
    if (rowSet == null || rowSet.isEmpty() || colSet == null || colSet.isEmpty()) {
      return false;
    }

    // prepare the space
    final int l = rowSet.last();
    while (l >= rows.size()) {
      rows.add(null);
    }

    boolean res = false;
    IntSet.IntIterator itr = rowSet.iterator();
    while (itr.hasNext()) {
      int row = itr.next();
      IntSet cols = rows.get(row);
      if (cols == null) {
        IntSet newCols = template.empty();
        newCols.addAll(colSet);
        rows.set(row, newCols);
        res = true;
      } else {
        res |= cols.addAll(colSet);
      }
    }
    return res;
  }

  /**
   * Removes the specified cell from this matrix if it is present.
   *
   * @param row row of the cell
   * @param col column of the cell
   *
   * @return <tt>true</tt> if this matrix contained the specified cell
   *
   * @throws UnsupportedOperationException if the <tt>remove</tt> operation is not supported by this
   *                                       matrix
   */
  public boolean remove(int row, int col)
  {
    if (row < 0 || col < 0 || row >= rows.size()) {
      return false;
    }
    IntSet r = rows.get(row);
    if (r == null) {
      return false;
    }
    if (r.remove(col)) {
      if (r.isEmpty()) {
        rows.set(row, null);
        fixRows();
      }
      return true;
    }
    return false;
  }

  /**
   * Removes the specified cells from this matrix. The cells are represented by
   * a given row and a set of columns.
   *
   * @param row  index of the row
   * @param cols indices of the columns
   *
   * @return <tt>true</tt> if this matrix contains at least one of the
   * specified cells
   *
   * @throws IllegalArgumentException if some property of the specified cell prevents it from being
   *                                  removed from this matrix
   */
  public boolean removeAll(int row, IntSet cols)
  {
    if (row < 0 || row >= rows.size()) {
      return false;
    }
    IntSet r = rows.get(row);
    if (r == null) {
      return false;
    }
    if (r.removeAll(cols)) {
      if (r.isEmpty()) {
        rows.set(row, null);
        fixRows();
      }
      return true;
    }
    return false;
  }

  /**
   * Removes the specified cells from this matrix. The cells are represented
   * by a given set of rows and a given column
   *
   * @param rowSet indices of the rows
   * @param col    index of the column
   *
   * @return <tt>true</tt> if this matrix contains at least one of the
   * specified cells
   *
   * @throws IllegalArgumentException if some property of the specified cell prevents it from being
   *                                  added to this matrix
   */
  public boolean removeAll(IntSet rowSet, int col)
  {
    if (rowSet == null || rowSet.isEmpty()) {
      return false;
    }

    boolean res = false;
    IntSet.IntIterator itr = rowSet.iterator();
    while (itr.hasNext()) {
      int r = itr.next();
      IntSet s = rows.get(r);
      if (s == null) {
        continue;
      }
      res |= s.remove(col);
      if (s.isEmpty()) {
        rows.set(r, null);
      }
    }
    if (res) {
      fixRows();
    }
    return res;
  }

  /**
   * Removes the specified cells from this matrix. The cells are represented
   * by the Cartesian product of a given set of rows and columns
   *
   * @param rowSet indices of the rows
   * @param colSet indices of the columns
   *
   * @return <tt>true</tt> if this matrix contains at least one of the
   * specified cells
   *
   * @throws IllegalArgumentException if some property of the specified cell prevents it from being
   *                                  added to this matrix
   */
  public boolean removeAll(IntSet rowSet, IntSet colSet)
  {
    if (rowSet == null || rowSet.isEmpty() || colSet == null || colSet.isEmpty()) {
      return false;
    }

    boolean res = false;
    IntSet.IntIterator itr = rowSet.iterator();
    while (itr.hasNext()) {
      int r = itr.next();
      IntSet s = rows.get(r);
      if (s == null) {
        continue;
      }
      res |= s.removeAll(colSet);
      if (s.isEmpty()) {
        rows.set(r, null);
      }
    }
    if (res) {
      fixRows();
    }
    return res;
  }

  /**
   * Retains the specified cells from this matrix. The cells are represented by
   * a given row and a set of columns.
   *
   * @param row  index of the row
   * @param cols indices of the columns
   *
   * @return <tt>true</tt> if this matrix contains at least one of the
   * specified cells
   *
   * @throws IllegalArgumentException if some property of the specified cell prevents it from being
   *                                  removed from this matrix
   */
  public boolean retainAll(int row, IntSet cols)
  {
    if (isEmpty()) {
      return false;
    }
    if (row < 0 || row >= rows.size()) {
      clear();
      return true;
    }

    IntSet r = rows.get(row);
    if (r == null) {
      clear();
      return true;
    }
    boolean res = false;
    for (int i = 0; i < rows.size(); i++) {
      if (i == row) {
        continue;
      }
      final IntSet r1 = rows.get(i);
      if (r1 != null) {
        res = true;
        rows.set(i, null);
      }
    }
    res |= r.retainAll(cols);
    fixRows();
    return res;
  }

  /**
   * Removes the specified cells from this matrix. The cells are represented
   * by a given set of rows and a given column
   *
   * @param rowSet indices of the rows
   * @param col    index of the column
   *
   * @return <tt>true</tt> if this matrix contains at least one of the
   * specified cells
   *
   * @throws IllegalArgumentException if some property of the specified cell prevents it from being
   *                                  added to this matrix
   */
  public boolean retainAll(IntSet rowSet, int col)
  {
    if (isEmpty()) {
      return false;
    }
    if (rowSet == null || rowSet.isEmpty()) {
      clear();
      return false;
    }

    boolean res = false;
    IntSet.IntIterator itr = rowSet.iterator();
    int i = 0;
    int r = itr.next();
    do {
      IntSet rr = rows.get(i);
      if (rr == null) {
        i++;
      } else if (i < r) {
        rows.set(i, null);
        res = true;
        i++;
      } else if (i > r) {
        r = itr.next();
      } else {
        if (!rr.contains(col)) {
          rows.set(i, null);
          res = true;
        } else if (rr.size() > 1) {
          rr.clear();
          rr.add(col);
          res = true;
        }
        i++;
        r = itr.next();
      }
    } while (i < rows.size() && itr.hasNext());
    res |= i < rows.size();
    for (; i < rows.size(); i++) {
      rows.set(i, null);
    }
    if (res) {
      fixRows();
    }
    return res;
  }

  /**
   * Removes the specified cells from this matrix. The cells are represented
   * by the Cartesian product of a given set of rows and columns
   *
   * @param rowSet indices of the rows
   * @param colSet indices of the columns
   *
   * @return <tt>true</tt> if this matrix contains at least one of the
   * specified cells
   *
   * @throws IllegalArgumentException if some property of the specified cell prevents it from being
   *                                  added to this matrix
   */
  public boolean retainAll(IntSet rowSet, IntSet colSet)
  {
    if (isEmpty()) {
      return false;
    }
    if (rowSet == null || rowSet.isEmpty() || colSet == null || colSet.isEmpty()) {
      clear();
      return false;
    }

    boolean res = false;
    IntSet.IntIterator itr = rowSet.iterator();
    int i = 0;
    int r = itr.next();
    do {
      IntSet rr = rows.get(i);
      if (rr == null) {
        i++;
      } else if (i < r) {
        rows.set(i, null);
        res = true;
        i++;
      } else if (i > r) {
        r = itr.next();
      } else {
        res |= rr.retainAll(colSet);
        if (rr.isEmpty()) {
          rows.set(i, null);
        }
        i++;
        r = itr.next();
      }
    } while (i < rows.size() && itr.hasNext());
    res |= i < rows.size();
    for (; i < rows.size(); i++) {
      rows.set(i, null);
    }
    if (res) {
      fixRows();
    }
    return res;
  }

  /**
   * Returns <tt>true</tt> if this matrix contains all of the cells of the
   * specified collection.
   *
   * @param other matrix to be checked for containment in this matrix
   *
   * @return <tt>true</tt> if this matrix contains all of the cells of the
   * specified collection
   *
   * @throws NullPointerException if the specified collection contains one or more null cells
   *                              and this matrix does not permit null cells (optional), or if
   *                              the specified collection is null
   * @see #contains(int, int)
   */
  public boolean containsAll(BinaryMatrix other)
  {
    if (other == null || other.isEmpty() || other == this) {
      return true;
    }
    if (isEmpty() || rows.size() < other.rows.size()) {
      return false;
    }

    for (int i = 0; i < other.rows.size(); i++) {
      IntSet s1 = rows.get(i);
      IntSet s2 = other.rows.get(i);
      if (s2 == null) {
        continue;
      }
      if (s1 == null || !s1.containsAll(s2)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns <tt>true</tt> if this matrix contains all of the cells of the
   * specified collection.
   *
   * @param rowSet indices of the rows
   * @param colSet indices of the columns
   *
   * @return <tt>true</tt> if this matrix contains all of the cells of the
   * specified collection
   */
  public boolean containsAll(IntSet rowSet, IntSet colSet)
  {
    if (rowSet == null || rowSet.isEmpty() || colSet == null || colSet.isEmpty()) {
      return true;
    }
    if (isEmpty()) {
      return false;
    }

    IntSet.IntIterator itr = rowSet.iterator();
    while (itr.hasNext()) {
      int i = itr.next();
      IntSet cols = rows.get(i);
      if (cols == null || !cols.containsAll(colSet)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns <tt>true</tt> if this matrix contains all of the cells of the
   * specified collection.
   *
   * @param row    index of the row
   * @param colSet indices of the columns
   *
   * @return <tt>true</tt> if this matrix contains all of the cells of the
   * specified collection
   */
  public boolean containsAll(int row, IntSet colSet)
  {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    if (isEmpty() || row < 0 || row >= rows.size()) {
      return false;
    }
    IntSet cols = rows.get(row);
    return cols != null && cols.containsAll(colSet);
  }

  /**
   * Returns <tt>true</tt> if this matrix contains all of the cells of the
   * specified collection.
   *
   * @param rowSet indices of the rows
   * @param col    index of the column
   *
   * @return <tt>true</tt> if this matrix contains all of the cells of the
   * specified collection
   */
  public boolean containsAll(IntSet rowSet, int col)
  {
    if (rowSet == null || rowSet.isEmpty()) {
      return true;
    }
    if (isEmpty() || col < 0) {
      return false;
    }

    IntSet.IntIterator itr = rowSet.iterator();
    while (itr.hasNext()) {
      int i = itr.next();
      IntSet cols = rows.get(i);
      if (cols == null || !cols.contains(col)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Adds all of the cells in the specified collection to this matrix if
   * they're not already present.
   *
   * @param other matrix containing cells to be added to this matrix
   *
   * @return <tt>true</tt> if this matrix changed as a result of the call
   *
   * @throws NullPointerException     if the specified collection contains one or more null cells
   *                                  and this matrix does not permit null cells, or if the
   *                                  specified collection is null
   * @throws IllegalArgumentException if some property of an cell of the specified collection
   *                                  prevents it from being added to this matrix
   * @see #add(int, int)
   */
  public boolean addAll(BinaryMatrix other)
  {
    boolean res = false;
    final int rowCount = Math.min(rows.size(), other.rows.size());
    int i = 0;
    for (; i < rowCount; i++) {
      IntSet s1 = rows.get(i);
      IntSet s2 = other.rows.get(i);
      if (s2 == null) {
        continue;
      }
      if (s1 == null) {
        rows.set(i, s2.clone());
        res = true;
      } else {
        res |= s1.addAll(s2);
      }
      assert rows.get(i) == null || !rows.get(i).isEmpty();
    }
    res |= i < other.rows.size();
    for (; i < other.rows.size(); i++) {
      IntSet s = other.rows.get(i);
      rows.add(s == null ? null : s.clone());
      assert rows.get(i) == null || !rows.get(i).isEmpty();
    }
    return res;
  }

  /**
   * Retains only the cells in this matrix that are contained in the specified
   * collection. In other words, removes from this matrix all of its cells
   * that are not contained in the specified collection.
   *
   * @param other matrix containing cells to be retained in this matrix
   *
   * @return <tt>true</tt> if this matrix changed as a result of the call
   *
   * @throws NullPointerException if this matrix contains a null cell and the specified
   *                              collection does not permit null cells (optional), or if the
   *                              specified collection is null
   * @see #remove(int, int)
   */
  public boolean retainAll(BinaryMatrix other)
  {
    boolean res = false;
    final int rowCount = Math.min(rows.size(), other.rows.size());
    int i = 0;
    for (; i < rowCount; i++) {
      IntSet s1 = rows.get(i);
      IntSet s2 = other.rows.get(i);
      if (s1 == null) {
        continue;
      }
      if (s2 == null) {
        rows.set(i, null);
        res = true;
      } else {
        res |= s1.retainAll(s2);
        if (s1.isEmpty()) {
          rows.set(i, null);
        }
      }
      assert rows.get(i) == null || !rows.get(i).isEmpty();
    }
    res |= i < rows.size();
    for (; i < rows.size(); i++) {
      rows.set(i, null);
    }
    if (res) {
      fixRows();
    }
    return res;
  }

  /**
   * Removes from this matrix all of its cells that are contained in the
   * specified collection.
   *
   * @param other matrix containing cells to be removed from this matrix
   *
   * @return <tt>true</tt> if this matrix changed as a result of the call
   *
   * @throws NullPointerException if this matrix contains a null cell and the specified
   *                              collection does not permit null cells (optional), or if the
   *                              specified collection is null
   * @see #remove(int, int)
   * @see #contains(int, int)
   */
  public boolean removeAll(BinaryMatrix other)
  {
    boolean res = false;
    final int rowCount = Math.min(rows.size(), other.rows.size());
    int i = 0;
    for (; i < rowCount; i++) {
      IntSet s1 = rows.get(i);
      IntSet s2 = other.rows.get(i);
      if (s1 == null || s2 == null) {
        continue;
      }
      res |= s1.removeAll(s2);
      if (s1.isEmpty()) {
        rows.set(i, null);
      }
      assert rows.get(i) == null || !rows.get(i).isEmpty();
    }
    if (i < rows.size()) {
      return res;
    }
    if (res) {
      fixRows();
    }
    return res;
  }

  /**
   * Removes all of the cells from this matrix. The matrix will be empty after
   * this call returns.
   *
   * @throws UnsupportedOperationException if the <tt>clear</tt> method is not supported by this matrix
   */
  public void clear()
  {
    rows.clear();
  }

  /**
   * @return an array containing all the cells in this matrix
   */
  public boolean[][] toArray()
  {
    throw new UnsupportedOperationException("TODO"); //TODO
  }

  /**
   * Returns an array containing all of the cells in this matrix.
   * <p>
   * If this matrix fits in the specified array with room to spare (i.e., the
   * array has more cells than this matrix), the cell in the array immediately
   * following the end of the matrix are left unchanged.
   *
   * @param a the array into which the cells of this matrix are to be
   *          stored.
   *
   * @return the array containing all the cells in this matrix
   *
   * @throws NullPointerException     if the specified array is null
   * @throws IllegalArgumentException if this matrix does not fit in the specified array
   */
  public boolean[][] toArray(boolean[][] a)
  {
    throw new UnsupportedOperationException("TODO"); //TODO
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(BinaryMatrix o)
  {
    throw new UnsupportedOperationException("TODO"); //TODO
  }

  /**
   * Gets a copy of the row with the given index
   *
   * @param row the row index
   *
   * @return the content of the row
   */
  public IntSet getRow(int row)
  {
    if (row < 0) {
      throw new IllegalArgumentException("negative row index: " + row);
    }
    if (row >= rows.size()) {
      return template.empty();
    }
    IntSet res = rows.get(row);
    if (res == null) {
      return template.empty();
    }
    return res.clone();
  }

  // /**
  // * Computes the power-set of the current matrix.
  // * <p>
  // * It is a particular implementation of the algorithm <i>Apriori</i> (see:
  // * Rakesh Agrawal, Ramakrishnan Srikant, <i>Fast Algorithms for Mining
  // * Association Rules in Large Databases</i>, in Proceedings of the
  // * 20<sup>th</sup> International Conference on Very Large Data Bases,
  // * p.487-499, 1994). The returned power-set does <i>not</i> contain the
  // * empty matrix.
  // * <p>
  // * The sub-matrices composing the power-set are returned in a list that is
  // * sorted according to the lexicographical order provided by the integer
  // * matrix.
  // *
  // * @return the power-set
  // * @see #powerSet(int, int)
  // * @see #powerSetSize()
  // */
  // public List<? extends BinaryMatrix> powerSet();
  //
  // /**
  // * Computes a subset of the power-set of the current matrix, composed by
  // * those sub-matrices that have cardinality between <code>min</code> and
  // * <code>max</code>.
  // * <p>
  // * It is a particular implementation of the algorithm <i>Apriori</i> (see:
  // * Rakesh Agrawal, Ramakrishnan Srikant, <i>Fast Algorithms for Mining
  // * Association Rules in Large Databases</i>, in Proceedings of the
  // * 20<sup>th</sup> International Conference on Very Large Data Bases,
  // * p.487-499, 1994). The power-set does <i>not</i> contains the empty
  // * matrix.
  // * <p>
  // * The sub-matrices composing the power-set are returned in a list that is
  // * sorted according to the lexicographical order provided by the integer
  // * matrix.
  // *
  // * @param min
  // * minimum sub-matrix size (greater than zero)
  // * @param max
  // * maximum sub-matrix size
  // * @return the power-set
  // * @see #powerSet()
  // * @see #powerSetSize(int, int)
  // */
  // public List<? extends BinaryMatrix> powerSet(int min, int max);
  //
  // /**
  // * Computes the power-set size of the current matrix.
  // * <p>
  // * The power-set does <i>not</i> contains the empty matrix.
  // *
  // * @return the power-set size
  // * @see #powerSet()
  // */
  // public int powerSetSize();
  //
  // /**
  // * Computes the power-set size of the current matrix, composed by those
  // * sub-matrices that have cardinality between <code>min</code> and
  // * <code>max</code>.
  // * <p>
  // * The returned power-set does <i>not</i> contain the empty matrix.
  // *
  // * @param min
  // * minimum sub-matrix size (greater than zero)
  // * @param max
  // * maximum sub-matrix size
  // * @return the power-set size
  // * @see #powerSet(int, int)
  // */
  // public int powerSetSize(int min, int max);
  //
  // /**
  // * Computes the Jaccard similarity coefficient between this matrix and the
  // * given matrix.
  // * <p>
  // * The coefficient is defined as
  // * <code>|A intersection B| / |A union B|</code>.
  // *
  // * @param other
  // * the other matrix
  // * @return the Jaccard similarity coefficient
  // * @see #jaccardDistance(BinaryMatrix)
  // */
  // public double jaccardSimilarity(BinaryMatrix other);
  //
  // /**
  // * Computes the Jaccard distance between this matrix and the given matrix.
  // * <p>
  // * The coefficient is defined as <code>1 - </code>
  // * {@link #jaccardSimilarity(BinaryMatrix)}.
  // *
  // * @param other
  // * the other matrix
  // * @return the Jaccard distance
  // * @see #jaccardSimilarity(BinaryMatrix)
  // */
  // public double jaccardDistance(BinaryMatrix other);
  //
  // /**
  // * Computes the weighted version of the Jaccard similarity coefficient
  // * between this matrix and the given matrix.
  // * <p>
  // * The coefficient is defined as
  // * <code>sum of min(A_i, B_i) / sum of max(A_i, B_i)</code>.
  // *
  // * @param other
  // * the other matrix
  // * @return the weighted Jaccard similarity coefficient
  // * @see #weightedJaccardDistance(BinaryMatrix)
  // */
  // public double weightedJaccardSimilarity(BinaryMatrix other);
  //
  // /**
  // * Computes the weighted version of the Jaccard distance between this
  // matrix
  // * and the given matrix.
  // * <p>
  // * The coefficient is defined as <code>1 - </code>
  // * {@link #weightedJaccardSimilarity(BinaryMatrix)}.
  // *
  // * @param other
  // * the other matrix
  // * @return the weighted Jaccard distance
  // * @see #weightedJaccardSimilarity(BinaryMatrix)
  // */
  // public double weightedJaccardDistance(BinaryMatrix other);

  /**
   * Gets a copy of the column with the given index
   *
   * @param col the column index
   *
   * @return the content of the column
   */
  public IntSet getCol(int col)
  {
    if (col < 0) {
      throw new IllegalArgumentException("negative column index: " + col);
    }
    IntSet res = template.empty();
    for (int row = 0; row < rows.size(); row++) {
      final IntSet r = rows.get(row);
      if (r != null && r.contains(col)) {
        res.add(row);
      }
    }
    return res;
  }

  /**
   * Generated a transposed matrix
   *
   * @return the transposed matrix
   */
  public BinaryMatrix transposed()
  {
    BinaryMatrix res = empty();
    for (int row = 0; row < rows.size(); row++) {
      IntSet r = rows.get(row);
      if (r == null) {
        continue;
      }
      IntSet.IntIterator itr = r.iterator();
      while (itr.hasNext()) {
        res.add(itr.next(), row);
      }
    }
    return res;
  }

  /**
   * Generates an ASCII-art matrix representation
   */
  @Override
  public String toString()
  {
    StringBuilder s = new StringBuilder();

    final int maxCol = maxCol();

    // initial line
    s.append('+');
    for (int i = 0; i <= maxCol; i++) {
      s.append('-');
    }
    s.append("+\n");

    // cells
    for (IntSet row : rows) {
      s.append('|');
      int col = 0;
      if (row != null) {
        IntSet.IntIterator itr = row.iterator();
        while (itr.hasNext()) {
          int c = itr.next();
          while (col++ < c) {
            s.append(' ');
          }
          s.append('*');
        }
      }
      while (col++ <= maxCol) {
        s.append(' ');
      }
      s.append("|\n");
    }

    // final line
    s.append('+');
    for (int i = 0; i <= maxCol; i++) {
      s.append('-');
    }
    s.append("+\n");

    return s.toString();
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
    if (!(obj instanceof BinaryMatrix)) {
      return false;
    }
    return rows.equals(((BinaryMatrix) obj).rows);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode()
  {
    int h = 1;
    for (IntSet s : rows) {
      h = (h << 5) - h;
      if (s != null) {
        h += s.hashCode();
      }
    }
    return h;
  }

  /**
   * @return the greatest non-empty row
   */
  public int maxRow()
  {
    return rows.size() - 1;
  }

  /**
   * @return the greatest non-empty column
   */
  public int maxCol()
  {
    int res = 0;
    for (IntSet row : rows) {
      if (row != null) {
        assert !row.isEmpty();
        res = Math.max(res, row.last());
      }
    }
    return res;
  }

  /**
   * @return the index set of non-empty rows
   */
  public IntSet involvedRows()
  {
    IntSet res = template.empty();
    for (int i = 0; i < rows.size(); i++) {
      if (rows.get(i) != null) {
        res.add(i);
      }
    }
    return res;
  }

  /**
   * @return the index set of non-empty columns
   */
  public IntSet involvedCols()
  {
    IntSet res = template.empty();
    for (int i = 0; i < rows.size(); i++) {
      res.addAll(rows.get(i));
    }
    return res;
  }

  /**
   * An {@link Iterator}-like interface
   */
  public interface CellIterator
  {
    /**
     * @return <tt>true</tt> if the iterator has more cells.
     */
    boolean hasNext();

    /**
     * Returns the next cell in the iteration. <b>IMPORTANT</b>: each
     * iteration returns an array of two elements, where the first element
     * is the row, while the second element is the column of the current
     * cell. In order to reduce the produced heap garbage, there is only
     * <i>one</i> array instantiated for each iterator, whose content is
     * overridden at each iteration.
     *
     * @return the next cell in the iteration.
     *
     * @throws NoSuchElementException iteration has no more cells.
     */
    int[] next();

    /**
     * Removes from the underlying matrix the last cell returned by the
     * iterator (optional operation). This method can be called only once
     * per call to <tt>next</tt>. The behavior of an iterator is unspecified
     * if the underlying collection is modified while the iteration is in
     * progress in any way other than by calling this method.
     *
     * @throws UnsupportedOperationException if the <tt>remove</tt> operation is not supported by
     *                                       this Iterator.
     * @throws IllegalStateException         if the <tt>next</tt> method has not yet been called,
     *                                       or the <tt>remove</tt> method has already been called
     *                                       after the last call to the <tt>next</tt> method.
     */
    void remove();

    /**
     * Skips all the cells before the the specified cell, so that
     * {@link #next()} gives the given cell or, if it does not exist, the
     * cell immediately after according to the sorting provided by this set.
     * <p>
     * If <code>cell</code> is less than the next cell, it does nothing
     *
     * @param row row of the cell
     * @param col column of the cell
     */
    public void skipAllBefore(int row, int col);
  }
}
