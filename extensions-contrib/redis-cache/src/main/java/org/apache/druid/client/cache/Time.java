package org.apache.druid.client.cache;

import org.joda.time.Period;

/**
 * Author: frank.chen021@outlook.com
 * Date: 2020/7/22 5:38 下午
 */
public class Time
{
  private long milliseconds;

  public Time(String time)
  {
    this.milliseconds = Period.parse(time).toStandardDuration().getMillis();
  }

  public Time(long milliseconds)
  {
    this.milliseconds = milliseconds;
  }

  public long getMilliseconds()
  {
    return milliseconds;
  }

  public long getSeconds()
  {
    return milliseconds / 1000;
  }
}
