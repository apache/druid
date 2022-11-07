package org.apache.druid.query.rowsandcols;

import org.apache.druid.query.rowsandcols.column.Column;

public interface AppendableRowsAndColumns extends RowsAndColumns
{
  AppendableRowsAndColumns addColumn(String name, Column column);
}
