/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.realtime;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.Nullable;

import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IncrementalIndex;
import com.metamx.druid.input.InputRow;

/**
*/
public class Sink implements Iterable<FireHydrant>
{
  private static final Logger log = new Logger(Sink.class);

  private volatile int swapCount = 0;
  private volatile FireHydrant currIndex;
  private volatile boolean hasSwapped = false;

  private final Interval interval;
  private final Schema schema;
  private final CopyOnWriteArrayList<FireHydrant> hydrants = new CopyOnWriteArrayList<FireHydrant>();


  public Sink(
      Interval interval,
      Schema schema
  )
  {
    this.schema = schema;
    this.interval = interval;

    makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public Sink(
      Interval interval,
      Schema schema,
      List<FireHydrant> hydrants
  )
  {
    this.schema = schema;
    this.interval = interval;

    for (int i = 0; i < hydrants.size(); ++i) {
      final FireHydrant hydrant = hydrants.get(i);
      if (hydrant.getCount() != i) {
        throw new ISE("hydrant[%s] not the right count[%s]", hydrant, i);
      }
    }
    this.hydrants.addAll(hydrants);

    swapCount = hydrants.size();
    makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public Interval getInterval()
  {
    return interval;
  }

  public int add(InputRow row)
  {
    if (currIndex == null) {
      throw new IAE("No currIndex but given row[%s]", row);
    }

    synchronized (currIndex) {
      return currIndex.getIndex().add(row);
    }
  }

  /**
   * If currIndex is A, creates a new index B, sets currIndex to B and returns A.
   *
   * @return the current index after swapping in a new one
   */
  public FireHydrant swap()
  {
    hasSwapped = true;
    return makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public boolean swappable()
  {
    synchronized (currIndex) {
      return currIndex.getIndex() != null && currIndex.getIndex().size() != 0;
    }
  }

  public DataSegment getSegment()
  {
    return new DataSegment(
        schema.getDataSource(),
        interval,
        interval.getStart().toString(),
        ImmutableMap.<String, Object>of(),
        Lists.<String>newArrayList(),
        Lists.transform(
            Arrays.asList(schema.getAggregators()), new Function<AggregatorFactory, String>()
        {
          @Override
          public String apply(@Nullable AggregatorFactory input)
          {
            return input.getName();
          }
        }),
        schema.getShardSpec(),
        0
    );
  }

  private FireHydrant makeNewCurrIndex(long minTimestamp, Schema schema)
  {
    IncrementalIndex newIndex = new IncrementalIndex(
        minTimestamp, schema.getIndexGranularity(), schema.getAggregators()
    );

    FireHydrant old;
    if (currIndex == null) {  // Only happens on initialization...
      old = currIndex;
      currIndex = new FireHydrant(newIndex, swapCount);
      hydrants.add(currIndex);
    } else {
      synchronized (currIndex) {
        old = currIndex;
        currIndex = new FireHydrant(newIndex, swapCount);
        hydrants.add(currIndex);
        ++swapCount;
      }
    }

    return old;
  }

  @Override
  public Iterator<FireHydrant> iterator()
  {
    return Iterators.filter(
        hydrants.iterator(),
        new Predicate<FireHydrant>()
        {
          @Override
          public boolean apply(@Nullable FireHydrant input)
          {
            final IncrementalIndex index = input.getIndex();
            return index == null || index.size() != 0;
          }
        }
    );
  }

  @Override
  public String toString()
  {
    return "Sink{" +
           "interval=" + interval +
           ", schema=" + schema +
           '}';
  }
}
