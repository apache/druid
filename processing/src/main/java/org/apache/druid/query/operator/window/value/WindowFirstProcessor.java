package org.apache.druid.query.operator.window.value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.ConstantObjectColumn;

public class WindowFirstProcessor extends WindowValueProcessorBase
{
  @JsonCreator
  public WindowFirstProcessor(
      @JsonProperty("inputColumn") String inputColumn,
      @JsonProperty("outputColumn") String outputColumn
  )
  {
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
