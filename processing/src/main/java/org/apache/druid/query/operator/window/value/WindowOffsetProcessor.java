package org.apache.druid.query.operator.window.value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;

public class WindowOffsetProcessor extends WindowValueProcessorBase
{
  private final int offset;

  @JsonCreator
  public WindowOffsetProcessor(
      @JsonProperty("inputColumn") String inputColumn,
      @JsonProperty("outputColumn") String outputColumn,
      @JsonProperty("offset") int offset
  ) {
    super(inputColumn, outputColumn);
    this.offset = offset;
  }

  @JsonProperty("offset")
  public int getOffset()
  {
    return offset;
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
            return cell + offset;
          }

          @Override
          protected boolean outsideBounds(int actualCell)
          {
            return actualCell < 0 || actualCell >= numRows;
          }
        }));
  }

  @Override
  public boolean validateEquivalent(Processor otherProcessor)
  {
    if (otherProcessor instanceof WindowOffsetProcessor) {
      WindowOffsetProcessor other = (WindowOffsetProcessor) otherProcessor;
      return offset == other.offset && intervalValidation(other);
    }
    return false;
  }

  @Override
  public String toString()
  {
    return "WindowOffsetProcessor{" +
           internalToString() +
           ", offset=" + offset +
           '}';
  }
}
