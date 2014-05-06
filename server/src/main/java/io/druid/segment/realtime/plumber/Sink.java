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

package io.druid.segment.realtime.plumber;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireHydrant;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 */
public class Sink implements Iterable<FireHydrant>
{
  private static final Logger log = new Logger(Sink.class);

  private volatile FireHydrant currIndex;

  private final Interval interval;
  private final DataSchema schema;
  private final RealtimeTuningConfig config;
  private final String version;
  private final CopyOnWriteArrayList<FireHydrant> hydrants = new CopyOnWriteArrayList<FireHydrant>();

  public Sink(
      Interval interval,
      DataSchema schema,
      RealtimeTuningConfig config,
      String version
  )
  {
    this.schema = schema;
    this.config = config;
    this.interval = interval;
    this.version = version;

    makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public Sink(
      Interval interval,
      DataSchema schema,
      RealtimeTuningConfig config,
      String version,
      List<FireHydrant> hydrants
  )
  {
    this.schema = schema;
    this.config = config;
    this.interval = interval;
    this.version = version;

    for (int i = 0; i < hydrants.size(); ++i) {
      final FireHydrant hydrant = hydrants.get(i);
      if (hydrant.getCount() != i) {
        throw new ISE("hydrant[%s] not the right count[%s]", hydrant, i);
      }
    }
    this.hydrants.addAll(hydrants);

    makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public String getVersion()
  {
    return version;
  }

  public Interval getInterval()
  {
    return interval;
  }

  public FireHydrant getCurrIndex()
  {
    return currIndex;
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

  public boolean isEmpty()
  {
    synchronized (currIndex) {
      return hydrants.size() == 1 && currIndex.getIndex().isEmpty();
    }
  }

  /**
   * If currIndex is A, creates a new index B, sets currIndex to B and returns A.
   *
   * @return the current index after swapping in a new one
   */
  public FireHydrant swap()
  {
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
        version,
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
        }
        ),
        config.getShardSpec(),
        null,
        0
    );
  }

  private FireHydrant makeNewCurrIndex(long minTimestamp, DataSchema schema)
  {
    List<SpatialDimensionSchema> spatialDimensionSchemas = schema.getParser() == null
                                                           ? Lists.<SpatialDimensionSchema>newArrayList()
                                                           : schema.getParser()
                                                                   .getParseSpec()
                                                                   .getDimensionsSpec()
                                                                   .getSpatialDimensions();
    IncrementalIndex newIndex = new IncrementalIndex(
        new IncrementalIndexSchema.Builder()
            .withMinTimestamp(minTimestamp)
            .withQueryGranularity(schema.getGranularitySpec().getQueryGranularity())
            .withSpatialDimensions(spatialDimensionSchemas)
            .withMetrics(schema.getAggregators())
            .build()
    );

    FireHydrant old;
    if (currIndex == null) {  // Only happens on initialization, cannot synchronize on null
      old = currIndex;
      currIndex = new FireHydrant(newIndex, hydrants.size(), getSegment().getIdentifier());
      hydrants.add(currIndex);
    } else {
      synchronized (currIndex) {
        old = currIndex;
        currIndex = new FireHydrant(newIndex, hydrants.size(), getSegment().getIdentifier());
        hydrants.add(currIndex);
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
