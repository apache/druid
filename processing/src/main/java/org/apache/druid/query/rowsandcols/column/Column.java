package org.apache.druid.query.rowsandcols.column;

public interface Column
{
  ColumnAccessor toAccessor();
  <T> T as(Class<? extends T> clazz);
}
