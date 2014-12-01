/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.overlord.autoscaling;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
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

  private final Object lock = new Object();

  private final MinMaxPriorityQueue<ScalingEvent> recentEvents;

  public ScalingStats(int capacity)
  {
    if (capacity == 0) {
      this.recentEvents = MinMaxPriorityQueue.orderedBy(comparator).create();
    } else {
      this.recentEvents = MinMaxPriorityQueue
          .orderedBy(comparator)
          .maximumSize(capacity)
          .create();
    }
  }

  public void addProvisionEvent(AutoScalingData data)
  {
    synchronized (lock) {
      recentEvents.add(
          new ScalingEvent(
              data,
              new DateTime(),
              EVENT.PROVISION
          )
      );
    }
  }

  public void addTerminateEvent(AutoScalingData data)
  {
    synchronized (lock) {
      recentEvents.add(
          new ScalingEvent(
              data,
              new DateTime(),
              EVENT.TERMINATE
          )
      );
    }
  }

  @JsonValue
  public List<ScalingEvent> toList()
  {
    synchronized (lock) {
      List<ScalingEvent> retVal = Lists.newArrayList(recentEvents);
      Collections.sort(retVal, comparator);
      return retVal;
    }
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
