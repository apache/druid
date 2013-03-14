package com.metamx.druid.realtime.plumber;

import org.joda.time.DateTime;

public interface RejectionPolicy
{
  public DateTime getCurrMaxTime();
  public boolean accept(long timestamp);
}
