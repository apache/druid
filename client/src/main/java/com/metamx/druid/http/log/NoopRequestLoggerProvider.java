package com.metamx.druid.http.log;

/**
 */
public class NoopRequestLoggerProvider implements RequestLoggerProvider
{
  @Override
  public RequestLogger get()
  {
    return new NoopRequestLogger();
  }
}
