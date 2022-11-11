package org.apache.druid.query.rowsandcols;

public class StartAndEnd
{
  private final int start;
  private final int end;

  public StartAndEnd(int start, int end)
  {
    this.start = start;
    this.end = end;
  }

  public int getStart()
  {
    return start;
  }

  public int getEnd()
  {
    return end;
  }
}
