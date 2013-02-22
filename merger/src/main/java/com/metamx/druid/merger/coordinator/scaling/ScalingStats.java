package com.metamx.druid.merger.coordinator.scaling;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 */
public class ScalingStats
{
  public static enum EVENT
  {
    PROVISION,
    TERMINATE
  }

  private static final Comparator<ScalingEvent> comparator = new Comparator<ScalingEvent>()

  {
    @Override
    public int compare(ScalingEvent s1, ScalingEvent s2)
    {
      return -s1.getTimestamp().compareTo(s2.getTimestamp());
    }
  };

  private final MinMaxPriorityQueue<ScalingEvent> recentNodes;

  public ScalingStats(int capacity)
  {
    this.recentNodes = MinMaxPriorityQueue
        .orderedBy(comparator)
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

  @JsonProperty
  public List<ScalingEvent> toList()
  {
    List<ScalingEvent> retVal = Lists.newArrayList(recentNodes);
    Collections.sort(retVal, comparator);
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

    @JsonProperty
    public AutoScalingData getData()
    {
      return data;
    }

    @JsonProperty
    public DateTime getTimestamp()
    {
      return timestamp;
    }

    @JsonProperty
    public EVENT getEvent()
    {
      return event;
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
