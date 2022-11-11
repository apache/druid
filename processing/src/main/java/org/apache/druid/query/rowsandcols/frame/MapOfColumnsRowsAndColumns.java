package org.apache.druid.query.rowsandcols.frame;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;

import java.util.Map;

public class MapOfColumnsRowsAndColumns implements RowsAndColumns
{
  public static MapOfColumnsRowsAndColumns fromMap(Map<String, Column> map) {
    if (map == null || map.isEmpty()) {
      throw new ISE("map[%s] cannot be null or empty", map);
    }

    return new MapOfColumnsRowsAndColumns(map, map.values().iterator().next().toAccessor().numCells());
  }

  private final Map<String, Column> mapOfColumns;
  private final int numRows;

  public MapOfColumnsRowsAndColumns(
      Map<String, Column> mapOfColumns,
      int numRows
  ) {
    this.mapOfColumns = mapOfColumns;
    this.numRows = numRows;
  }

  @Override
  public int numRows()
  {
    return numRows;
  }

  @Override
  public Column findColumn(String name)
  {
    return mapOfColumns.get(name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(Class<T> clazz)
  {
    if (AppendableRowsAndColumns.class.equals(clazz)) {
      return (T) new AppendableMapOfColumns(this);
    }
     return null;
  }

}
