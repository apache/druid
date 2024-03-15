package org.apache.druid.frame.write;

import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

public class FrameFieldWriterException extends RuntimeException
{
  @Nullable
  protected final String column;

  public FrameFieldWriterException(@Nullable String column, @Nullable String message, @Nullable Throwable cause)
  {
    super(StringUtils.format("Error [%s] while writing frame for column [%s]", message, column), cause);
    this.column = column;
  }

  @Nullable
  public String getColumn()
  {
    return column;
  }
}
