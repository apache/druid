package org.apache.druid.server;

public interface QueryMetricCounter
{
  void incrementSuccess();

  void incrementFailed();

  void incrementInterrupted();

  void incrementTimedOut();
}
