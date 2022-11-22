package org.apache.druid.query.operator.window.value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;

public class WindowLagProcessor extends WindowValueProcessorBase
{
  private final int lagRows;

  @JsonCreator
  public WindowLagProcessor(
      @JsonProperty("inputColumn") String inputColumn,
      @JsonProperty("outputColumn") String outputColumn,
      @JsonProperty("lag")int lagRows
  ) {
    super(inputColumn, outputColumn);
    this.lagRows = lagRows;
  }

  @JsonProperty("lag")
  public int getLagRows()
  {
    return lagRows;
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
