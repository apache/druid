/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.select;

import org.apache.druid.query.Result;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

/**
 */
public class SelectResultValueBuilder
{
  private static final Comparator<EventHolder> comparator = new Comparator<EventHolder>()
  {
    @Override
    public int compare(EventHolder o1, EventHolder o2)
    {
      int retVal = Long.compare(o1.getTimestamp().getMillis(), o2.getTimestamp().getMillis());

      if (retVal == 0) {
        retVal = o1.getSegmentId().compareTo(o2.getSegmentId());
      }

      if (retVal == 0) {
        retVal = Integer.compare(o1.getOffset(), o2.getOffset());
      }

      return retVal;
    }
  };

  protected final DateTime timestamp;
  protected final PagingSpec pagingSpec;
  protected final boolean descending;
  protected Set<String> dimensions;
  protected Set<String> metrics;

  protected List<EventHolder> eventHolders;
  protected final Map<String, Integer> pagingIdentifiers;

  public SelectResultValueBuilder(DateTime timestamp, PagingSpec pagingSpec, boolean descending)
  {
    this.timestamp = timestamp;
    this.pagingSpec = pagingSpec;
    this.descending = descending;
    this.dimensions = new HashSet<>();
    this.metrics = new HashSet<>();
    this.pagingIdentifiers = new LinkedHashMap<>();
    instantiateEventHolders();
  }

  public void addEntry(EventHolder event)
  {
    eventHolders.add(event);
  }

  public void finished(String segmentId, int lastOffset)
  {
    pagingIdentifiers.put(segmentId, lastOffset);
  }
  
  public void addDimension(String dimension)
  {
    dimensions.add(dimension);
  }

  public void addDimensions(Set<String> dimensions)
  {
    this.dimensions.addAll(dimensions);
  }

  public void addMetric(String metric)
  {
    metrics.add(metric);
  }

  public void addMetrics(Set<String> metrics)
  {
    this.metrics.addAll(metrics);
  }

  public Result<SelectResultValue> build()
  {
    prepareEventHolders();
    Result<SelectResultValue> result = new Result<>(
        timestamp,
        new SelectResultValue(pagingIdentifiers, dimensions, metrics, eventHolders)
    );
    // Prohibit use-after-build, because eventHolders is returned directly, without making a defensive copy.
    eventHolders = null;
    return result;
  }

  protected void prepareEventHolders()
  {
    // do nothing
  }

  protected void instantiateEventHolders()
  {
    eventHolders = new ArrayList<>();
  }

  public static class MergeBuilder extends SelectResultValueBuilder
  {
    private Queue<EventHolder> pQueue;

    public MergeBuilder(DateTime timestamp, PagingSpec pagingSpec, boolean descending)
    {
      super(timestamp, pagingSpec, descending);
    }

    @Override
    public void addEntry(EventHolder event)
    {
      int threshold = pagingSpec.getThreshold();
      if (threshold > 0) {
        pQueue.add(event);
        if (pQueue.size() > threshold) {
          pQueue.remove();
        }
      } else {
        eventHolders.add(event);
      }
    }

    @Override
    protected void instantiateEventHolders()
    {
      int threshold = pagingSpec.getThreshold();
      if (threshold > 0) {
        // Specifically creating a heap queue in the opposite order from what is required, to be able to remove the
        // least elements above the threshold.
        Comparator<EventHolder> order = descending ? comparator : comparator.reversed();
        pQueue = new PriorityQueue<>(order);
      } else {
        eventHolders = new ArrayList<>();
      }
    }

    @Override
    protected void prepareEventHolders()
    {
      if (pagingSpec.getThreshold() > 0) {
        eventHolders = new ArrayList<>(pQueue.size());
        // Draining a heap queue into an list and then reversing it to get the right order
        while (!pQueue.isEmpty()) {
          eventHolders.add(pQueue.remove());
        }
        Collections.reverse(eventHolders);
        pQueue = null;
      } else {
        Comparator<EventHolder> order = descending ? comparator.reversed() : comparator;
        eventHolders.sort(order);
      }
      for (EventHolder event : eventHolders) {
        pagingIdentifiers.put(event.getSegmentId(), event.getOffset());
      }
    }
  }
}
