package com.metamx.druid.merger.coordinator.scaling;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

import java.util.List;

/**
 */
public class ScalingStats
{
  private static enum EVENT
  {
    PROVISION,
    TERMINATE
  }

  private final MinMaxPriorityQueue<ScalingEvent> recentNodes;

  public ScalingStats(int capacity)
  {
    this.recentNodes = MinMaxPriorityQueue
        .orderedBy(DateTimeComparator.getInstance())
        .maximumSize(capacity)
        .create();
  }

  public void addProvisionEvent(AutoScalingData data)
  {
    recentNodes.add(
        new ScalingEvent(
            data,
            new DateTime(),
            EVENT.PROVISION
        )
    );
  }

  public void addTerminateEvent(AutoScalingData data)
  {
    recentNodes.add(
        new ScalingEvent(
            data,
            new DateTime(),
            EVENT.TERMINATE
        )
    );
  }

  public List<ScalingEvent> toList()
  {
    List<ScalingEvent> retVal = Lists.newArrayList();
    while (!recentNodes.isEmpty()) {
      retVal.add(recentNodes.poll());
    }
    return retVal;
  }

  public static class ScalingEvent
  {
    private final AutoScalingData data;
    private final DateTime timestamp;
    private final EVENT event;

    private ScalingEvent(
        AutoScalingData data,
        DateTime timestamp,
        EVENT event
    )
    {
      this.data = data;
      this.timestamp = timestamp;
      this.event = event;
    }

    @Override
    public String toString()
    {
      return "ScalingEvent{" +
             "data=" + data +
             ", timestamp=" + timestamp +
             ", event=" + event +
             '}';
    }
  }
}
