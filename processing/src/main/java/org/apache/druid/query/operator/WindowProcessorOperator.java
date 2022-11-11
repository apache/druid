package org.apache.druid.query.operator;

import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

public class WindowProcessorOperator implements Operator
{
  private final Processor windowProcessor;
  private final Operator child;

  public WindowProcessorOperator(
      Processor windowProcessor,
      Operator child
  ) {
    this.windowProcessor = windowProcessor;
    this.child = child;
  }

  @Override
  public void open()
  {
    child.open();
  }

  @Override
  public RowsAndColumns next()
  {
    return windowProcessor.process(child.next());
  }

  @Override
  public boolean hasNext()
  {
    return child.hasNext();
  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade) {
      child.close(cascade);
    }
  }
}
