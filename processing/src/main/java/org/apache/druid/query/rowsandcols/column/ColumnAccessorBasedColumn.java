package org.apache.druid.query.rowsandcols.column;

public class ColumnAccessorBasedColumn implements Column
{
  private final ColumnAccessor base;

  public ColumnAccessorBasedColumn(
      ColumnAccessor base
  )
  {
    this.base = base;
  }

  @Override
  public ColumnAccessor toAccessor()
  {
    return base;
  }

  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    // TODO:
    return null;
  }
}
