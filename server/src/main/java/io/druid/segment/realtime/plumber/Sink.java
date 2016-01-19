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

package io.druid.segment.realtime.plumber;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.data.input.InputRow;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.QueryableIndex;
import io.druid.segment.data.Indexed;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireHydrant;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 */
public class Sink implements Iterable<FireHydrant>
{

  private final Object hydrantLock = new Object();
  private final Interval interval;
  private final DataSchema schema;
  private final RealtimeTuningConfig config;
  private final String version;
  private final CopyOnWriteArrayList<FireHydrant> hydrants = new CopyOnWriteArrayList<FireHydrant>();
  private final LinkedHashSet<String> dimOrder = Sets.newLinkedHashSet();
  private volatile FireHydrant currHydrant;

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

    int maxCount = -1;
    for (int i = 0; i < hydrants.size(); ++i) {
      final FireHydrant hydrant = hydrants.get(i);
      if (hydrant.getCount() <= maxCount) {
        throw new ISE("hydrant[%s] not the right count[%s]", hydrant, i);
      }
      maxCount = hydrant.getCount();
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

  public FireHydrant getCurrHydrant()
  {
    return currHydrant;
  }

  public int add(InputRow row) throws IndexSizeExceededException
  {
    if (currHydrant == null) {
      throw new IAE("No currHydrant but given row[%s]", row);
    }

    synchronized (hydrantLock) {
      IncrementalIndex index = currHydrant.getIndex();
      if (index == null) {
        return -1; // the hydrant was swapped without being replaced
      }
      return index.add(row);
    }
  }

  public boolean canAppendRow()
  {
    synchronized (currHydrant) {
      return currHydrant != null && currHydrant.getIndex().canAppendRow();
    }
  }

  public boolean isEmpty()
  {
    synchronized (hydrantLock) {
      return hydrants.size() == 1 && currHydrant.getIndex().isEmpty();
    }
  }

  /**
   * If currHydrant is A, creates a new index B, sets currHydrant to B and returns A.
   *
   * @return the current index after swapping in a new one
   */
  public FireHydrant swap()
  {
    return makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public boolean swappable()
  {
    synchronized (hydrantLock) {
      return currHydrant.getIndex() != null && currHydrant.getIndex().size() != 0;
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
    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(minTimestamp)
        .withQueryGranularity(schema.getGranularitySpec().getQueryGranularity())
        .withDimensionsSpec(schema.getParser())
        .withMetrics(schema.getAggregators())
        .build();
    final IncrementalIndex newIndex = new OnheapIncrementalIndex(
        indexSchema,
        config.getMaxRowsInMemory()
    );

    final FireHydrant old;
    synchronized (hydrantLock) {
      old = currHydrant;
      int newCount = 0;
      int numHydrants = hydrants.size();
      if (numHydrants > 0) {
        FireHydrant lastHydrant = hydrants.get(numHydrants - 1);
        newCount = lastHydrant.getCount() + 1;
        if (!indexSchema.getDimensionsSpec().hasCustomDimensions()) {
          if (lastHydrant.hasSwapped()) {
            QueryableIndex oldIndex = lastHydrant.getSegment().asQueryableIndex();
            for (String dim : oldIndex.getAvailableDimensions()) {
              dimOrder.add(dim);
            }
          } else {
            IncrementalIndex oldIndex = lastHydrant.getIndex();
            dimOrder.addAll(oldIndex.getDimensionOrder());
          }
          newIndex.loadDimensionIterable(dimOrder);
        }
      }
      currHydrant = new FireHydrant(newIndex, newCount, getSegment().getIdentifier());
      hydrants.add(currHydrant);
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
