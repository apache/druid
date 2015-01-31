/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.select;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.primitives.Longs;
import io.druid.query.Result;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

  private final DateTime timestamp;

  private MinMaxPriorityQueue<EventHolder> pQueue = null;

  public SelectResultValueBuilder(
      DateTime timestamp,
      int threshold
  )
  {
    this.timestamp = timestamp;

    instantiatePQueue(threshold, comparator);
  }

  public void addEntry(
      EventHolder event
  )
  {
    pQueue.add(event);
  }

  public Result<SelectResultValue> build()
  {
    // Pull out top aggregated values
    List<EventHolder> values = Lists.newArrayListWithCapacity(pQueue.size());
    Map<String, Integer> pagingIdentifiers = Maps.newLinkedHashMap();
    while (!pQueue.isEmpty()) {
      EventHolder event = pQueue.remove();
      pagingIdentifiers.put(event.getSegmentId(), event.getOffset());
      values.add(event);
    }

    return new Result<SelectResultValue>(
        timestamp,
        new SelectResultValue(pagingIdentifiers, values)
    );
  }

  private void instantiatePQueue(int threshold, final Comparator comparator)
  {
    this.pQueue = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(threshold).create();
  }
}
