package org.apache.druid.query.operator.window.value;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.ConstantObjectColumn;

public class WindowLastProcessor extends WindowValueProcessorBase
{
  public WindowLastProcessor(
      String inputColumn,
      String outputColumn
  ) {
    super(inputColumn, outputColumn);
  }

  @Override
  public RowsAndColumns process(RowsAndColumns input) {
    final int lastIndex = input.numRows() - 1;
    if (lastIndex < 0) {
      throw new ISE("Called with an input partition of size 0.  The call site needs to not do that.");
    }

    return processInternal(input, column -> {
      final ColumnAccessor accessor = column.toAccessor();
      return new ConstantObjectColumn(accessor.getObject(lastIndex), accessor.numCells(), accessor.getType());
    });
  }
}
