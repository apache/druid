package org.apache.druid.query.operator;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.Segment;

public class SegmentToRowsAndColumnsOperator implements Operator
{
  private final Segment segment;
  private boolean hasNext = true;

  public SegmentToRowsAndColumnsOperator(
      Segment segment
  )
  {
    this.segment = segment;
  }

  @Override
  public void open()
  {

  }

  @Override
  public RowsAndColumns next()
  {
    hasNext = false;

    RowsAndColumns rac = segment.as(RowsAndColumns.class);
    if (rac != null) {
      return rac;
    }

    throw new ISE("Cannot work with segment of type[%s]", segment.getClass());
  }

  @Override
  public boolean hasNext()
  {
    return hasNext;
  }

  @Override
  public void close(boolean cascade)
  {

  }
}
