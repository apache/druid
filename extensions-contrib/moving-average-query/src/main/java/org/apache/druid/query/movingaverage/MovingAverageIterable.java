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

package org.apache.druid.query.movingaverage;

import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.movingaverage.averagers.Averager;
import org.apache.druid.query.movingaverage.averagers.AveragerFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link MovingAverageIterable} iterates over days {@link RowBucket}, producing rows for each dimension combination,
 * filling in missing entries with "empty" rows so that the averaging buckets have enough data to operate on.
 * It then computes the moving average on the buckets and returns the row.
 * See computeMovingAverage for more details.
 */
public class MovingAverageIterable implements Iterable<Row>
{

  private final Sequence<RowBucket> seq;
  private final List<DimensionSpec> dims;
  private final List<AveragerFactory<?, ?>> factories;
  private final Map<String, PostAggregator> postAggMap;
  private final Map<String, AggregatorFactory> aggMap;
  private final Map<String, Object> emptyEvents;

  public MovingAverageIterable(
      Sequence<RowBucket> buckets,
      List<DimensionSpec> dims,
      List<AveragerFactory<?, ?>> factories,
      List<PostAggregator> postAggList,
      List<AggregatorFactory> aggList
  )
  {
    this.dims = dims;
    this.factories = factories;
    this.seq = buckets;

    postAggMap = postAggList.stream().collect(Collectors.toMap(postAgg -> postAgg.getName(), postAgg -> postAgg));
    aggMap = aggList.stream().collect(Collectors.toMap(agg -> agg.getName(), agg -> agg));
    emptyEvents = generateEmptyEventsFromAggregators(aggMap, postAggMap);
  }

  // Build a list of empty events from Aggregators/PostAggregators to be used by Iterator to build fake rows.
  // These fake rows will be used by computeMovingAverage() in skip=true mode.
  // See emptyEventsCopy in internalNext() and computeMovingAverage() documentation.
  private Map<String, Object> generateEmptyEventsFromAggregators(Map<String, AggregatorFactory> aggMap,
                                                                 Map<String, PostAggregator> postAggMap)
  {
    Map<String, Object> emptyEvents = new LinkedHashMap<>();
    aggMap.values().forEach(agg -> {
      Aggregator aggFactorized = agg.factorize(getEmptyColumnSelectorFactory());
      emptyEvents.put(agg.getName(), aggFactorized.get());
    });
    postAggMap.values().forEach(postAgg -> emptyEvents.put(postAgg.getName(), postAgg.compute(emptyEvents)));
    return emptyEvents;
  }

  @Nonnull
  private ColumnSelectorFactory getEmptyColumnSelectorFactory()
  {
    return new ColumnSelectorFactory()
    {
      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        // Generating empty records while aggregating on Filtered aggregators requires a dimension selector
        // for initialization.  This dimension selector is not actually used for generating values
        return DimensionSelector.constant(null);
      }

      @Override
      public ColumnValueSelector makeColumnValueSelector(String s)
      {
        return NilColumnValueSelector.instance();
      }

      @Override
      public ColumnCapabilities getColumnCapabilities(String s)
      {
        return null;
      }
    };
  }

  /* (non-Javadoc)
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<Row> iterator()
  {
    return new MovingAverageIterator(seq, dims, factories, emptyEvents, aggMap);
  }

  static class MovingAverageIterator implements Iterator<Row>
  {

    private final List<DimensionSpec> dims;
    // Key: Row's dimension set. Value: Averager. See MovingAverageIterator#computeMovingAverage for more details.
    private final Map<Map<String, Object>, List<Averager<?>>> averagers = new HashMap<>();
    private final List<AveragerFactory<?, ?>> averagerFactories;

    private Yielder<RowBucket> yielder;
    private RowBucket cache = null;
    private Iterator<Row> cacheIter;
    private Iterator<Map<String, Object>> averagersKeysIter;
    private Set<Map<String, Object>> seenKeys = new HashSet<>();
    private Row saveNext;
    private Map<String, AggregatorFactory> aggMap;
    private Map<String, Object> emptyEvents;

    public MovingAverageIterator(
        Sequence<RowBucket> rows,
        List<DimensionSpec> dims,
        List<AveragerFactory<?, ?>> averagerFactories,
        Map<String, Object> emptyEvents,
        Map<String, AggregatorFactory> aggMap
    )
    {
      this.dims = dims;
      this.averagerFactories = averagerFactories;
      this.emptyEvents = emptyEvents;
      this.aggMap = aggMap;

      yielder = Yielders.each(rows);
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext()
    {
      if (saveNext != null) {
        return true;
      }

      saveNext = internalNext();
      return (saveNext != null);
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public Row next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Row retVal = saveNext;
      saveNext = null;
      return retVal;
    }

    private Row internalNext()
    {
      // Iterate until there is a row to return or Yielder is exahusted, in such a case return null.
      // This is used in order to skip empty buckets (iterate to the next one).
      while (true) {
        if (cache == null && !yielder.isDone()) {
          cache = yielder.get();
          yielder = yielder.next(cache);

          cacheIter = cache.getRows().iterator();
        }

        Row r;

        // return rows from the cached RowBucket
        if (cacheIter != null) {
          if (cacheIter.hasNext()) {
            r = cacheIter.next();
            // Convert full event (key + metrics) to key
            Map<String, Object> key = MovingAverageHelper.getDimKeyFromRow(dims, r);
            seenKeys.add(key);
            r = computeMovingAverage((MapBasedRow) r, false);
            if (r != null) {
              return r;
            } else {
              throw new NoSuchElementException();
            }
          } else {
            Set<Map<String, Object>> averagerKeys = new HashSet<>(averagers.keySet());
            averagerKeys.removeAll(seenKeys);
            averagersKeysIter = averagerKeys.iterator();
            cacheIter = null;
          }
        }

        // return empty rows for unseen dimension combinations
        if (averagersKeysIter != null) {
          while (averagersKeysIter.hasNext()) {
            Map<String, Object> dims = averagersKeysIter.next();
            Map<String, Object> emptyEventsCopy = new HashMap<>(emptyEvents);

            // Convert key to a full dummy event (key + dummy metrics).
            dims.forEach((dim, value) -> emptyEventsCopy.put(dim, value));

            r = computeMovingAverage(new MapBasedRow(cache.getDateTime(), emptyEventsCopy), true);
            if (r != null) {
              return r;
            }
          }

          seenKeys.clear();
          averagersKeysIter = null;
          cache = null;
        }

        if (cacheIter == null && yielder.isDone()) {
          // we should never get here. For some reason, there is
          // no more work to do, so continuing to iterate will infinite loop
          return null;
        }
      }
    }

    /**
     * Compute and add any moving average columns.
     *
     * <p>Normally, the row passed in will be added to all the {@link Averager}'s and then results pulled
     * from each averager. If skip is true, then the incoming row is actually a dummy value due to
     * no data being present for this dimension combination in the current bucket. When this happens,
     * {@link Averager#skip()} should be called instead of {@link Averager#addElement(Map, Map)}()} to force proper
     * decaying of the average values.
     *
     * <p>Usually, the contents of key will be contained by the row R being passed in, but in the case of a
     * dummy row, it's possible that the dimensions will be known but the row empty. Hence, the values are
     * passed as two separate arguments.
     *
     * @param r    The Row to operate on
     * @param skip Indicates whether skip or add should be called
     *
     * @return The updated row containing averager results, or null if no averagers computed a result
     */
    @Nullable
    private Row computeMovingAverage(MapBasedRow r, boolean skip)
    {
      Map<String, Object> event = r.getEvent();
      Map<String, Object> result = new HashMap<>(event);
      Map<String, Object> key = MovingAverageHelper.getDimKeyFromRow(dims, r);

      List<Averager<?>> avg = averagers.get(key);

      // Initialize key's averagers.
      if (avg == null) {
        avg = averagerFactories.stream().map(af -> af.createAverager()).collect(Collectors.toList());
        averagers.put(key, avg);
      }

      if (!skip) {
        avg.forEach(af -> af.addElement(event, aggMap));
      } else {
        avg.forEach(af -> af.skip());
      }

      avg.forEach(af -> result.put(af.getName(), af.getResult()));

      // At least one non-dimension value must be in the record for it to be valid.
      if (result.entrySet().stream().anyMatch(e -> !key.containsKey(e.getKey()) && e.getValue() != null)) {
        result.putAll(event);
        return new MapBasedRow(r.getTimestamp(), result);
      } else {
        // No averagers returned anything. All buckets must be empty.
        // skip this row.
        return null;
      }
    }
  }
}
