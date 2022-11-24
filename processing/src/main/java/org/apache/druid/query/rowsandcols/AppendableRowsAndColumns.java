package org.apache.druid.query.rowsandcols;

import org.apache.druid.query.rowsandcols.column.Column;

/**
 * A RowsAndColumns that supposed appending columns.  This interface is particularly useful because even if there is
 * some composition of code that works with RowsAndColumns, we would like to add the columns to a singular base object
 * instead of build up a complex object graph.
 */
public interface AppendableRowsAndColumns extends RowsAndColumns
{
  /**
   * Mutates the RowsAndColumns by appending the requested Column.
   *
   * @param name   the name of the new column
   * @param column the Column object representing the new column
   */
  void addColumn(String name, Column column);
}
