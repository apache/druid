package com.metamx.druid.http;

/**
 */
public interface RequestLogger
{
  public void log(RequestLogLine requestLogLine) throws Exception;
}
