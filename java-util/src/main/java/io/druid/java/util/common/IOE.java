package io.druid.java.util.common;

/**
 * IOException to RuntimeException
 */
public class IOE extends java.lang.RuntimeException
{

  public IOE()
  {
    super();
  }

  public IOE(String message)
  {
    super(message);
  }

  public IOE(String message, Throwable throwable)
  {
    super(message, throwable);
  }


}
