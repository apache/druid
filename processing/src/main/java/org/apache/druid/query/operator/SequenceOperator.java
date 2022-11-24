package org.apache.druid.query.operator;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.io.IOException;
import java.util.NoSuchElementException;

public class SequenceOperator implements Operator
{
  private final Sequence<RowsAndColumns> child;
  private Yielder<RowsAndColumns> yielder;
  private boolean closed = false;

  public SequenceOperator(
      Sequence<RowsAndColumns> child
  ) {
    this.child = child;
  }

  @Override
  public void open()
  {
    if (closed) {
      throw new ISE("Operator closed, cannot be re-opened");
    }
    yielder = Yielders.each(child);
  }

  @Override
  public RowsAndColumns next()
  {
    if (closed) {
      throw new NoSuchElementException();
    }
    final RowsAndColumns retVal = yielder.get();
    yielder = yielder.next(null);
    return retVal;
  }

  @Override
  public boolean hasNext()
  {
    return !closed && !yielder.isDone();
  }

  @Override
  public void close(boolean cascade)
  {
    if (closed) {
      return;
    }
    try {
      yielder.close();
    }
    catch (IOException e) {
      throw new RE(e, "Exception when closing yielder from Sequence");
    } finally {
      closed = true;
    }
  }
}
