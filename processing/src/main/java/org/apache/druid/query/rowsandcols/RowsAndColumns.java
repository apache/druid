package org.apache.druid.query.rowsandcols;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.column.Column;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface RowsAndColumns
{
  @Nonnull
  static AppendableRowsAndColumns expectAppendable(RowsAndColumns inputPartition)
  {
    if (inputPartition instanceof AppendableRowsAndColumns) {
      return (AppendableRowsAndColumns) inputPartition;
    }

    AppendableRowsAndColumns retVal = inputPartition.as(AppendableRowsAndColumns.class);
    if (retVal == null) {
      throw new ISE(
          "Unable to force appendability, RowsAndColumns class[%s] cannot append.",
          inputPartition.getClass()
      );
    }
    return retVal;
  }

  int numRows();
  Column findColumn(String name);

  @Nullable
  <T> T as(Class<T> clazz);
}
