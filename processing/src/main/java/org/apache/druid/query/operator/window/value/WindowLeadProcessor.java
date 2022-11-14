package org.apache.druid.query.operator.window.value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;

public class WindowLeadProcessor extends WindowValueProcessorBase
{
  private final int leadRows;

  @JsonCreator
  public WindowLeadProcessor(
      @JsonProperty("inputColumn") String inputColumn,
      @JsonProperty("outputColumn") String outputColumn,
      @JsonProperty("lead") int leadRows
  ) {
    super(inputColumn, outputColumn);
    this.leadRows = leadRows;
  }

  @Override
  public RowsAndColumns process(RowsAndColumns input) {
    final int numRows = input.numRows();

    return processInternal(input, column -> new ColumnAccessorBasedColumn(
        new ShiftedColumnAccessorBase(column.toAccessor())
        {
          @Override
          protected int getActualCell(int cell)
          {
            return cell + leadRows;
          }

          @Override
          protected boolean outsideBounds(int actualLhsCell)
          {
            return actualLhsCell >= numRows;
          }
        }
    ));
  }
}
