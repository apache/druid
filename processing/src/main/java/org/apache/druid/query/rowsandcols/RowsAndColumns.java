package org.apache.druid.query.rowsandcols;

import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.frame.AppendableMapOfColumns;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface RowsAndColumns
{
  @Nonnull
  static AppendableRowsAndColumns expectAppendable(RowsAndColumns input)
  {
    if (input instanceof AppendableRowsAndColumns) {
      return (AppendableRowsAndColumns) input;
    }

    AppendableRowsAndColumns retVal = input.as(AppendableRowsAndColumns.class);
    if (retVal == null) {
      retVal = new AppendableMapOfColumns(input);
    }
    return retVal;
  }

  int numRows();
  Column findColumn(String name);

  @Nullable
  <T> T as(Class<T> clazz);
}
