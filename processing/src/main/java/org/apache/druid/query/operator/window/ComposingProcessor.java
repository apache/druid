package org.apache.druid.query.operator.window;

import org.apache.druid.query.rowsandcols.RowsAndColumns;

public class ComposingProcessor implements Processor
{
  private final Processor[] processors;

  public ComposingProcessor(
      Processor... processors
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
