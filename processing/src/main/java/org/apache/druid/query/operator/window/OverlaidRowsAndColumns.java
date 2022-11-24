package org.apache.druid.query.operator.window;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;

import javax.annotation.Nullable;
import java.util.Map;

public class OverlaidRowsAndColumns implements AppendableRowsAndColumns
{
  private final RowsAndColumns base;
  private final Map<String, Column> overlay;

  public OverlaidRowsAndColumns(
      RowsAndColumns base,
      Map<String, Column> overlay
  ) {
    this.base = base;
    this.overlay = overlay;
  }

  @Override
  public int numRows()
  {
    return base.numRows();
  }

  @Override
  public Column findColumn(String name)
  {
    final Column overlayCol = overlay.get(name);
    if (overlayCol == null) {
      return base.findColumn(name);
    }
    return overlayCol;
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    return null;
  }

  @Override
  public void addColumn(String name, Column column)
  {
    final Column prevValue = overlay.put(name, column);
    if (prevValue != null) {
      throw new ISE("Tried to override column[%s]!?  Was[%s], now[%s]", name, prevValue, column);
    }
  }
}
