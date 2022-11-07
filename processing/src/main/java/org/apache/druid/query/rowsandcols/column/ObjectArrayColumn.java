package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
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
    return new ColumnAccessor()
    {
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

      @Override
      public boolean isNull(int cell)
      {
        return objects[cell] == null;
      }

      @Nullable
      @Override
      public Object getObject(int cell)
      {
        return objects[cell];
      }

      @Override
      public double getDouble(int cell)
      {
        if (objects[cell] instanceof Number) {
          return ((Number) objects[cell]).doubleValue();
        } else if (objects[cell] instanceof String) {
          try {
            return Double.parseDouble((String) objects[cell]);
          } catch (NumberFormatException e) {
            return 0d;
          }
        } else {
          return 0d;
        }
      }

      @Override
      public float getFloat(int cell)
      {
        if (objects[cell] instanceof Number) {
          return ((Number) objects[cell]).floatValue();
        } else if (objects[cell] instanceof String) {
          try {
            return Float.parseFloat((String) objects[cell]);
          } catch (NumberFormatException e) {
            return 0f;
          }
        } else {
          return 0f;
        }
      }

      @Override
      public long getLong(int cell)
      {
        if (objects[cell] instanceof Number) {
          return ((Number) objects[cell]).longValue();
        } else if (objects[cell] instanceof String) {
          try {
            return Long.parseLong((String) objects[cell]);
          } catch (NumberFormatException e) {
            return 0L;
          }
        } else {
          return 0L;
        }
      }

      @Override
      public int getInt(int cell)
      {
        if (objects[cell] instanceof Number) {
          return ((Number) objects[cell]).intValue();
        } else if (objects[cell] instanceof String) {
          try {
            return Integer.parseInt((String) objects[cell]);
          } catch (NumberFormatException e) {
            return 0;
          }
        } else {
          return 0;
        }
      }

      @Override
      public int compareCells(int lhsCell, int rhsCell)
      {
        return comparator.compare(objects[lhsCell], objects[rhsCell]);
      }
    };
  }

  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    return null;
  }
}
