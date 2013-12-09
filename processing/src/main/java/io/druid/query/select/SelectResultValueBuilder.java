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
