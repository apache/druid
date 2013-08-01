package com.metamx.druid.http.log;

import com.metamx.druid.http.RequestLogLine;

/**
 */
public class NoopRequestLogger implements RequestLogger
{
  @Override
  public void log(RequestLogLine requestLogLine) throws Exception
  {
    // This is a no op!
  }
}
