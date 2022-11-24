package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

public class NullColumnAccessor implements ColumnAccessor
{
  private final ColumnType type;
  private final int size;

  public NullColumnAccessor(int size)
  {
    this(ColumnType.UNKNOWN_COMPLEX, size);
  }

  public NullColumnAccessor(ColumnType type, int size)
  {
    this.type = type;
    this.size = size;
  }

  @Override
  public ColumnType getType()
  {
    return type;
  }

  @Override
  public int numCells()
  {
    return size;
  }

  @Override
  public boolean isNull(int cell)
  {
    return true;
  }

  @Nullable
  @Override
  public Object getObject(int cell)
  {
    return null;
  }

  @Override
  public double getDouble(int cell)
  {
    return 0;
  }

  @Override
  public float getFloat(int cell)
  {
    return 0;
  }

  @Override
  public long getLong(int cell)
  {
    return 0;
  }

  @Override
  public int getInt(int cell)
  {
    return 0;
  }

  @Override
  public int compareCells(int lhsCell, int rhsCell)
  {
    return 0;
  }
}
