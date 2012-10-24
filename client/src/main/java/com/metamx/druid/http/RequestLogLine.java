package com.metamx.druid.http;

import com.google.common.base.Joiner;
import com.metamx.druid.Query;
import org.joda.time.DateTime;

import java.util.Arrays;

public class RequestLogLine
{
  private static final Joiner JOINER = Joiner.on("\t");

  private final DateTime timestamp;
  private final String remoteAddr;
  private final Query query;

  public RequestLogLine(DateTime timestamp, String remoteAddr, Query query)
  {
    this.timestamp = timestamp;
    this.remoteAddr = remoteAddr;
    this.query = query;
  }

  public String getLine()
  {
    return JOINER.join(
        Arrays.asList(
            timestamp,
            remoteAddr,
            query
        )
    );
  }
}