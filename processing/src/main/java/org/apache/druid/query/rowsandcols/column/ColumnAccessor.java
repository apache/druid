package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

/**
 * Allows for accessing a column, provides methods to enable cell-by-cell access.
 */
public interface ColumnAccessor
{
  /**
   * Get the type of the Column
   * @return the type of the Column
   */
  ColumnType getType();

  /**
   * Get the number of cells
   * @return the number of cells
   */
  int numCells();

  /**
   * Get whether the value of a cell is null
   *
   * @param cell the cell id, 0-indexed
   * @return true if the value is null
   */
  boolean isNull(int cell);

  /**
   * Get the {@link Object} representation of the cell.
   *
   * @param cell the cell id, 0-indexed
   * @return the {@link Object} representation of the cell.  Returns {@code null} If {@link #isNull} is true.
   */
  @Nullable
  Object getObject(int cell);

  /**
   * Get the primitive {@code double} representation of the cell.
   *
   * @param cell the cell id, 0-indexed
   * @return the primitive {@code double} representation of the cell.  Returns {@code 0D} If {@link #isNull} is true.
   */
  double getDouble(int cell);

  /**
   * Get the primitive {@code float} representation of the cell.
   *
   * @param cell the cell id, 0-indexed
   * @return the primitive {@code float} representation of the cell.  Returns {@code 0F} If {@link #isNull} is true.
   */
  float getFloat(int cell);

  /**
   * Get the primitive {@code long} representation of the cell.
   *
   * @param cell the cell id, 0-indexed
   * @return the primitive {@code long} representation of the cell.  Returns {@code 0L} If {@link #isNull} is true.
   */
  long getLong(int cell);

  /**
   * Get the primitive {@code int} representation of the cell.
   *
   * @param cell the cell id, 0-indexed
   * @return the primitive {@code int} representation of the cell.  Returns {@code 0} If {@link #isNull} is true.
   */
  int getInt(int cell);

  /**
   * Compares two cells using a comparison that follows the same semantics as {@link java.util.Comparator#compare}
   *
   * This is not comparing the cell Ids, but the values referred to by the cell ids.
   *
   * @param lhsCell the cell id of the left-hand-side of the comparison
   * @param rhsCell the cell id of the right-hand-side of the comparison
   * @return the result of the comparison of the two cells
   */
  int compareCells(int lhsCell, int rhsCell);
}
