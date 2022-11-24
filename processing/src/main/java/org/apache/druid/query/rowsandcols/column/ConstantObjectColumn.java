package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.segment.column.ColumnType;

public class ConstantObjectColumn implements Column
{
  private final Object obj;
  private final int numCells;
  private final ColumnType type;

  public ConstantObjectColumn(Object obj, int numCells, ColumnType type)
  {
    this.obj = obj;
    this.numCells = numCells;
    this.type = type;
  }

  @Override
  public ColumnAccessor toAccessor()
  {
    return new ColumnAccessor()
    {
      @Override
      public ColumnType getType()
      {
        return type;
      }

      @Override
      public int numCells()
      {
        return numCells;
      }

      @Override
      public boolean isNull(int cell)
      {
        return obj == null;
      }

      @Override
      public Object getObject(int cell)
      {
        return obj;
      }

      @Override
      public double getDouble(int cell)
      {
        return ((Number) obj).doubleValue();
      }

      @Override
      public float getFloat(int cell)
      {
        return ((Number) obj).floatValue();
      }

      @Override
      public long getLong(int cell)
      {
        return ((Number) obj).longValue();
      }

      @Override
      public int getInt(int cell)
      {
        return ((Number) obj).intValue();
      }

      @Override
      public int compareCells(int lhsCell, int rhsCell)
      {
        return 0;
      }
    };
  }

  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    return null;
  }
}
