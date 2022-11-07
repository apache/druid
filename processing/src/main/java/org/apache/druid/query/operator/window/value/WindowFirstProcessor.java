package org.apache.druid.query.operator.window.value;

import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.ConstantObjectColumn;

public class WindowFirstProcessor extends WindowValueProcessorBase
{
  public WindowFirstProcessor(
      String inputColumn,
      String outputColumn
  ) {
    super(inputColumn, outputColumn);
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    return processInternal(
        incomingPartition,
        column -> {
          final ColumnAccessor accessor = column.toAccessor();
          return new ConstantObjectColumn(accessor.getObject(0), accessor.numCells(), accessor.getType());
        }
    );
  }
}
