package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

public interface ColumnAccessor
{
  ColumnType getType();
  int numCells();
  boolean isNull(int cell);
  @Nullable
  Object getObject(int cell);
  double getDouble(int cell);
  float getFloat(int cell);
  long getLong(int cell);
  int getInt(int cell);

  int compareCells(int lhsCell, int rhsCell);
}
