package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.segment.column.ColumnType;

public class DoubleArrayColumn implements Column
{
  private final double[] vals;

  public DoubleArrayColumn(
      double[] vals
  ) {
    this.vals = vals;
  }

  @Override
  public ColumnAccessor toAccessor()
  {
    return new ColumnAccessor()
    {
      @Override
      public ColumnType getType()
      {
        return ColumnType.DOUBLE;
      }

      @Override
      public int numCells()
      {
        return vals.length;
      }

      @Override
      public boolean isNull(int cell)
      {
        return false;
      }

      @Override
      public Object getObject(int cell)
      {
        return vals[cell];
      }

      @Override
      public double getDouble(int cell)
      {
        return vals[cell];
      }

      @Override
      public float getFloat(int cell)
      {
        return (float) vals[cell];
      }

      @Override
      public long getLong(int cell)
      {
        return (long) vals[cell];
      }

      @Override
      public int getInt(int cell)
      {
        return (int) vals[cell];
      }

      @Override
      public int compareCells(int lhsCell, int rhsCell)
      {
        return Double.compare(lhsCell, rhsCell);
      }
    };
  }

  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    // TODO
    return null;
  }
}
