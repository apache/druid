package org.apache.druid.query.operator.window.value;

import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;

public class WindowLagProcessor extends WindowValueProcessorBase
{
  private final int lagRows;

  public WindowLagProcessor(
      String inputColumn,
      String outputColumn,
      int lagRows
  ) {
    super(inputColumn, outputColumn);
    this.lagRows = lagRows;
  }

  @Override
  public RowsAndColumns process(RowsAndColumns input) {
    return processInternal(input, column -> new ColumnAccessorBasedColumn(
        new ShiftedColumnAccessorBase(column.toAccessor())
        {
          @Override
          protected int getActualCell(int cell)
          {
            return cell - lagRows;
          }

          @Override
          protected boolean outsideBounds(int actualCell)
          {
            return actualCell < 0;
          }
        }));
  }
}
