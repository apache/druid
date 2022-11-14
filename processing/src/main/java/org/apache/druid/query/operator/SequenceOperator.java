package org.apache.druid.query.operator;

import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.io.IOException;

public class SequenceOperator implements Operator
{
  private final Sequence<RowsAndColumns> child;
  private Yielder<RowsAndColumns> yielder;

  public SequenceOperator(
      Sequence<RowsAndColumns> child
  ) {
    this.child = child;
  }

  @Override
  public void open()
  {
    yielder = Yielders.each(child);
  }

  @Override
  public RowsAndColumns next()
  {
    final RowsAndColumns retVal = yielder.get();
    yielder = yielder.next(null);
    return retVal;
  }

  @Override
  public boolean hasNext()
  {
    return !yielder.isDone();
  }

  @Override
  public void close(boolean cascade)
  {
    try {
      yielder.close();
    }
    catch (IOException e) {
      throw new RE(e, "Exception when closing yielder from Sequence");
    }
  }
}
