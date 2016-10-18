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

package io.druid.query.select;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.guava.Comparators;
import io.druid.query.Result;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
      int retVal = Longs.compare(o1.getTimestamp().getMillis(), o2.getTimestamp().getMillis());

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

  protected final Queue<EventHolder> pQueue;
  protected final Map<String, Integer> pagingIdentifiers;

  public SelectResultValueBuilder(DateTime timestamp, PagingSpec pagingSpec, boolean descending)
  {
    this.timestamp = timestamp;
    this.pagingSpec = pagingSpec;
    this.descending = descending;
    this.dimensions = Sets.newHashSet();
    this.metrics = Sets.newHashSet();
    this.pagingIdentifiers = Maps.newLinkedHashMap();
    this.pQueue = instantiatePQueue();
  }

  public void addEntry(EventHolder event)
  {
    pQueue.add(event);
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
    return new Result<SelectResultValue>(
        timestamp,
        new SelectResultValue(pagingIdentifiers, dimensions, metrics, getEventHolders())
    );
  }

  protected List<EventHolder> getEventHolders()
  {
    return Lists.newArrayList(pQueue);
  }

  protected Queue<EventHolder> instantiatePQueue()
  {
    return Queues.newArrayDeque();
  }

  public static class MergeBuilder extends SelectResultValueBuilder
  {
    public MergeBuilder(DateTime timestamp, PagingSpec pagingSpec, boolean descending)
    {
      super(timestamp, pagingSpec, descending);
    }

    @Override
    protected Queue<EventHolder> instantiatePQueue()
    {
      int threshold = pagingSpec.getThreshold();
      return MinMaxPriorityQueue.orderedBy(descending ? Comparators.inverse(comparator) : comparator)
                                .maximumSize(threshold > 0 ? threshold : Integer.MAX_VALUE)
                                .create();
    }

    @Override
    protected List<EventHolder> getEventHolders()
    {
      final List<EventHolder> values = Lists.newArrayListWithCapacity(pQueue.size());
      while (!pQueue.isEmpty()) {
        EventHolder event = pQueue.remove();
        pagingIdentifiers.put(event.getSegmentId(), event.getOffset());
        values.add(event);
      }
      return values;
    }
  }
}
