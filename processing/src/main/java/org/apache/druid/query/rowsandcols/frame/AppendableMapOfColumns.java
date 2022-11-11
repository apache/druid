package org.apache.druid.query.rowsandcols.frame;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;

import java.util.LinkedHashMap;

public class AppendableMapOfColumns implements AppendableRowsAndColumns
{

  private final RowsAndColumns base;
  private final LinkedHashMap<String, Column> appendedColumns;

  public AppendableMapOfColumns(
      RowsAndColumns base
  )
  {
    this.base = base;
    this.appendedColumns = new LinkedHashMap<>();
  }

  @Override
  public AppendableRowsAndColumns addColumn(String name, Column column)
  {
    final Column prevValue = appendedColumns.put(name, column);
    if (prevValue != null) {
      throw new ISE("Tried to override column[%s]!?  Was[%s], now[%s]", name, prevValue, column);
    }
    return this;
  }

  @Override
  public int numRows()
  {
    return base.numRows();
  }

  @Override
  public Column findColumn(String name)
  {
    Column retVal = base.findColumn(name);
    if (retVal == null) {
      retVal = appendedColumns.get(name);
    }
    return retVal;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(Class<T> clazz)
  {
    if (AppendableRowsAndColumns.class.equals(clazz)) {
      return (T) this;
    }
    return null;
  }
}
