/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.autoscaling;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import io.druid.guice.annotations.PublicApi;
import io.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 */
@PublicApi
public class ScalingStats
{
  public enum EVENT
  {
    PROVISION,
    TERMINATE
  }

  private static final Comparator<ScalingEvent> COMPARATOR = new Ordering<ScalingEvent>()
  {
    @Override
    public int compare(ScalingEvent s1, ScalingEvent s2)
    {
      return s2.getTimestamp().compareTo(s1.getTimestamp());
    }
  }.nullsLast();

  private final Object lock = new Object();

  private final MinMaxPriorityQueue<ScalingEvent> recentEvents;

  public ScalingStats(int capacity)
  {
    if (capacity == 0) {
      this.recentEvents = MinMaxPriorityQueue.orderedBy(COMPARATOR).create();
    } else {
      this.recentEvents = MinMaxPriorityQueue
          .orderedBy(COMPARATOR)
          .maximumSize(capacity)
          .create();
    }
  }

  public void addAll(ScalingStats stats)
  {
    synchronized (lock) {
      synchronized (stats.lock) {
        recentEvents.addAll(stats.recentEvents);
      }
    }
  }

  public void addProvisionEvent(AutoScalingData data)
  {
    synchronized (lock) {
      recentEvents.add(new ScalingEvent(data, DateTimes.nowUtc(), EVENT.PROVISION));
    }
  }

  public void addTerminateEvent(AutoScalingData data)
  {
    synchronized (lock) {
      recentEvents.add(new ScalingEvent(data, DateTimes.nowUtc(), EVENT.TERMINATE));
    }
  }

  @JsonValue
  public List<ScalingEvent> toList()
  {
    synchronized (lock) {
      List<ScalingEvent> retVal = Lists.newArrayList(recentEvents);
      Collections.sort(retVal, COMPARATOR);
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
