package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.segment.column.ColumnType;

import java.util.Comparator;

public class ObjectArrayColumn implements Column
{
  private final Object[] objects;
  private final ColumnType resultType;
  private final Comparator<Object> comparator;

  public ObjectArrayColumn(Object[] objects, ColumnType resultType)
  {
    this(objects, resultType, resultType.getStrategy());
  }

  public ObjectArrayColumn(Object[] objects, ColumnType resultType, Comparator<Object> comparator)
  {
    this.objects = objects;
    this.resultType = resultType;
    this.comparator = comparator;
  }

  @Override
  public ColumnAccessor toAccessor()
  {
    return new ObjectColumnAccessorBase(){
      @Override
      protected Object getVal(int cell)
      {
        return objects[cell];
      }

      @Override
      protected Comparator<Object> getComparator()
      {
        return comparator;
      }

      @Override
      public ColumnType getType()
      {
        return resultType;
      }

      @Override
      public int numCells()
      {
        return objects.length;
      }
    };
  }

  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    return null;
  }

}
