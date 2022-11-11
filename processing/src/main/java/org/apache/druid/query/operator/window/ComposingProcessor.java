package org.apache.druid.query.operator.window;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

public class ComposingProcessor implements Processor
{
  private final Processor[] processors;

  @JsonCreator
  public ComposingProcessor(
      @JsonProperty("processors") Processor... processors
  ) {
    this.processors = processors;
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    RowsAndColumns retVal = incomingPartition;
    for (int i = processors.length - 1; i >= 0; --i) {
      retVal = processors[i].process(retVal);
    }
    return retVal;
  }
}
