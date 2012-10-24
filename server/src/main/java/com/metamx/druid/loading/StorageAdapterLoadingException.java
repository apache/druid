package com.metamx.druid.loading;

/**
 */
public class StorageAdapterLoadingException extends Exception
{
  public StorageAdapterLoadingException(
      String formatString,
      Object... objs
  )
  {
    super(String.format(formatString, objs));
  }

  public StorageAdapterLoadingException(
      Throwable cause,
      String formatString,
      Object... objs
  )
  {
    super(String.format(formatString, objs), cause);
  }
}
