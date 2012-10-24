package com.metamx.druid;

import com.metamx.druid.partition.PartitionHolder;
import org.joda.time.Interval;

/**
*/
public class TimelineObjectHolder<VersionType, ObjectType>
{
  private final Interval interval;
  private final VersionType version;
  private final PartitionHolder<ObjectType> object;

  public TimelineObjectHolder(
      Interval interval,
      VersionType version,
      PartitionHolder<ObjectType> object
  )
  {
    this.interval = interval;
    this.version = version;
    this.object = object;
  }

  public Interval getInterval()
  {
    return interval;
  }

  public VersionType getVersion()
  {
    return version;
  }

  public PartitionHolder<ObjectType> getObject()
  {
    return object;
  }

  @Override
  public String toString()
  {
    return "TimelineObjectHolder{" +
           "interval=" + interval +
           ", version=" + version +
           ", object=" + object +
           '}';
  }
}
