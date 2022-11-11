package org.apache.druid.query.operator;

import org.apache.druid.query.rowsandcols.RowsAndColumns;

public interface Operator
{
  void open();
  RowsAndColumns next();
  boolean hasNext();
  void close(boolean cascade);
}
