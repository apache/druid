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

package io.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.FunctionalIterator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.parsers.CloseableIterator;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Global;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 */
public class GroupByQueryEngine
{
  private final Supplier<GroupByQueryConfig> config;
  private final StupidPool<ByteBuffer> intermediateResultsBufferPool;

  @Inject
  public GroupByQueryEngine (
      Supplier<GroupByQueryConfig> config,
      @Global StupidPool<ByteBuffer> intermediateResultsBufferPool
  )
  {
    this.config = config;
    this.intermediateResultsBufferPool = intermediateResultsBufferPool;
  }

  public Sequence<Row> process(final GroupByQuery query, StorageAdapter storageAdapter)
  {
    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
    }

    final Iterable<Cursor> cursors = storageAdapter.makeCursors(
        Filters.convertDimensionFilters(query.getDimFilter()),
        intervals.get(0),
        query.getGranularity()
    );

    final ResourceHolder<ByteBuffer> bufferHolder = intermediateResultsBufferPool.take();

    return Sequences.concat(
        new BaseSequence<Sequence<Row>, Iterator<Sequence<Row>>>(
            new BaseSequence.IteratorMaker<Sequence<Row>, Iterator<Sequence<Row>>>()
            {
              @Override
              public Iterator<Sequence<Row>> make()
              {
                return FunctionalIterator
                    .create(cursors.iterator())
                    .transform(
                        new Function<Cursor, Sequence<Row>>()
                        {
                          @Override
                          public Sequence<Row> apply(@Nullable final Cursor cursor)
                          {
                            return new BaseSequence<Row, RowIterator>(
                                new BaseSequence.IteratorMaker<Row, RowIterator>()
                                {
                                  @Override
                                  public RowIterator make()
                                  {
                                    return new RowIterator(query, cursor, bufferHolder.get(), config.get());
                                  }

                                  @Override
                                  public void cleanup(RowIterator iterFromMake)
                                  {
                                    Closeables.closeQuietly(iterFromMake);
                                  }
                                }
                            );
                          }
                        }
                    );
              }

              @Override
              public void cleanup(Iterator<Sequence<Row>> iterFromMake)
              {
                Closeables.closeQuietly(bufferHolder);
              }
            }
        )
    );

  }

  private static class RowUpdater
  {
    private final ByteBuffer metricValues;
    private final BufferAggregator[] aggregators;
    private final PositionMaintainer positionMaintainer;

    private final TreeMap<ByteBuffer, Integer> positions;

    public RowUpdater(
        ByteBuffer metricValues,
        BufferAggregator[] aggregators,
        PositionMaintainer positionMaintainer
    )
    {
      this.metricValues = metricValues;
      this.aggregators = aggregators;
      this.positionMaintainer = positionMaintainer;

      this.positions = Maps.newTreeMap();
    }

    public int getNumRows()
    {
      return positions.size();
    }

    public TreeMap<ByteBuffer, Integer> getPositions()
    {
      return positions;
    }

    private List<ByteBuffer> updateValues(
        ByteBuffer key,
        List<DimensionSelector> dims
    )
    {
      if (dims.size() > 0) {
        List<ByteBuffer> retVal = null;
        List<ByteBuffer> unaggregatedBuffers = null;

        final DimensionSelector dimSelector = dims.get(0);
        final IndexedInts row = dimSelector.getRow();
        if (row == null || row.size() == 0) {
          ByteBuffer newKey = key.duplicate();
          newKey.putInt(dimSelector.getValueCardinality());
          unaggregatedBuffers = updateValues(newKey, dims.subList(1, dims.size()));
        }
        else {
          for (Integer dimValue : row) {
            ByteBuffer newKey = key.duplicate();
            newKey.putInt(dimValue);
            unaggregatedBuffers = updateValues(newKey, dims.subList(1, dims.size()));
          }
        }
        if (unaggregatedBuffers != null) {
          if (retVal == null) {
            retVal = Lists.newArrayList();
          }
          retVal.addAll(unaggregatedBuffers);
        }
        return retVal;
      }
      else {
        key.clear();
        Integer position = positions.get(key);
        int[] increments = positionMaintainer.getIncrements();
        int thePosition;

        if (position == null) {
          ByteBuffer keyCopy = ByteBuffer.allocate(key.limit());
          keyCopy.put(key.asReadOnlyBuffer());
          keyCopy.clear();

          position = positionMaintainer.getNext();
          if (position == null) {
            return Lists.newArrayList(keyCopy);
          }

          positions.put(keyCopy, position);
          thePosition = position;
          for (int i = 0; i < aggregators.length; ++i) {
            aggregators[i].init(metricValues, thePosition);
            thePosition += increments[i];
          }
        }

        thePosition = position;
        for (int i = 0; i < aggregators.length; ++i) {
          aggregators[i].aggregate(metricValues, thePosition);
          thePosition += increments[i];
        }
        return null;
      }
    }
  }

  private class PositionMaintainer
  {
    private final int[] increments;
    private final int increment;
    private final int max;

    private long nextVal;

    public PositionMaintainer(
        int start,
        int[] increments,
        int max
    )
    {
      this.nextVal = (long) start;
      this.increments = increments;

      int theIncrement = 0;
      for (int i = 0; i < increments.length; i++) {
        theIncrement += increments[i];
      }
      increment = theIncrement;

      this.max = max - increment; // Make sure there is enough room for one more increment
    }

    public Integer getNext()
    {
      if (nextVal > max) {
        return null;
      }
      else {
        int retVal = (int) nextVal;
        nextVal += increment;
        return retVal;
      }
    }

    public int getIncrement()
    {
      return increment;
    }

    public int[] getIncrements()
    {
      return increments;
    }
  }

  private class RowIterator implements CloseableIterator<Row>
  {
    private final GroupByQuery query;
    private final Cursor cursor;
    private final ByteBuffer metricsBuffer;
    private final GroupByQueryConfig config;

    private final List<DimensionSpec> dimensionSpecs;
    private final List<DimensionSelector> dimensions;
    private final ArrayList<String> dimNames;
    private final List<AggregatorFactory> aggregatorSpecs;
    private final BufferAggregator[] aggregators;
    private final String[] metricNames;
    private final int[] sizesRequired;

    private List<ByteBuffer> unprocessedKeys;
    private Iterator<Row> delegate;

    public RowIterator(GroupByQuery query, Cursor cursor, ByteBuffer metricsBuffer, GroupByQueryConfig config)
    {
      this.query = query;
      this.cursor = cursor;
      this.metricsBuffer = metricsBuffer;
      this.config = config;

      unprocessedKeys = null;
      delegate = Iterators.emptyIterator();
      dimensionSpecs = query.getDimensions();
      dimensions = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());
      dimNames = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());
      for (int i = 0; i < dimensionSpecs.size(); ++i) {
        final DimensionSpec dimSpec = dimensionSpecs.get(i);
        final DimensionSelector selector = cursor.makeDimensionSelector(dimSpec.getDimension());
        if (selector != null) {
          dimensions.add(selector);
          dimNames.add(dimSpec.getOutputName());
        }
      }

      aggregatorSpecs = query.getAggregatorSpecs();
      aggregators = new BufferAggregator[aggregatorSpecs.size()];
      metricNames = new String[aggregatorSpecs.size()];
      sizesRequired = new int[aggregatorSpecs.size()];
      for (int i = 0; i < aggregatorSpecs.size(); ++i) {
        AggregatorFactory aggregatorSpec = aggregatorSpecs.get(i);
        aggregators[i] = aggregatorSpec.factorizeBuffered(cursor);
        metricNames[i] = aggregatorSpec.getName();
        sizesRequired[i] = aggregatorSpec.getMaxIntermediateSize();
      }
    }

    @Override
    public boolean hasNext()
    {
      return delegate.hasNext() || !cursor.isDone();
    }

    @Override
    public Row next()
    {
      if (delegate.hasNext()) {
        return delegate.next();
      }

      if (unprocessedKeys == null && cursor.isDone()) {
        throw new NoSuchElementException();
      }

      final PositionMaintainer positionMaintainer = new PositionMaintainer(0, sizesRequired, metricsBuffer.remaining());
      final RowUpdater rowUpdater = new RowUpdater(metricsBuffer, aggregators, positionMaintainer);
      if (unprocessedKeys != null) {
        for (ByteBuffer key : unprocessedKeys) {
          final List<ByteBuffer> unprocUnproc = rowUpdater.updateValues(key, ImmutableList.<DimensionSelector>of());
          if (unprocUnproc != null) {
            throw new ISE("Not enough memory to process the request.");
          }
        }
        cursor.advance();
      }
      while (!cursor.isDone()) {
        ByteBuffer key = ByteBuffer.allocate(dimensions.size() * Ints.BYTES);

        unprocessedKeys = rowUpdater.updateValues(key, dimensions);
        if (unprocessedKeys != null || rowUpdater.getNumRows() > config.getMaxIntermediateRows()) {
          break;
        }

        cursor.advance();
      }

      if (rowUpdater.getPositions().isEmpty() && unprocessedKeys != null) {
        throw new ISE(
            "Not enough memory to process even a single item.  Required [%,d] memory, but only have[%,d]",
            positionMaintainer.getIncrement(), metricsBuffer.remaining()
        );
      }

      delegate = FunctionalIterator
          .create(rowUpdater.getPositions().entrySet().iterator())
          .transform(
              new Function<Map.Entry<ByteBuffer, Integer>, Row>()
              {
                private final DateTime timestamp = cursor.getTime();
                private final int[] increments = positionMaintainer.getIncrements();

                @Override
                public Row apply(@Nullable Map.Entry<ByteBuffer, Integer> input)
                {
                  Map<String, Object> theEvent = Maps.newLinkedHashMap();

                  ByteBuffer keyBuffer = input.getKey().duplicate();
                  for (int i = 0; i < dimensions.size(); ++i) {
                    final DimensionSelector dimSelector = dimensions.get(i);
                    final int dimVal = keyBuffer.getInt();
                    if (dimSelector.getValueCardinality() != dimVal) {
                      theEvent.put(dimNames.get(i), dimSelector.lookupName(dimVal));
                    }
                  }

                  int position = input.getValue();
                  for (int i = 0; i < aggregators.length; ++i) {
                    theEvent.put(metricNames[i], aggregators[i].get(metricsBuffer, position));
                    position += increments[i];
                  }

                  for (PostAggregator postAggregator : query.getPostAggregatorSpecs()) {
                    theEvent.put(postAggregator.getName(), postAggregator.compute(theEvent));
                  }

                  return new MapBasedRow(timestamp, theEvent);
                }
              }
          );

      return delegate.next();
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    public void close() {
      // cleanup
      for(BufferAggregator agg : aggregators) {
        agg.close();
      }
    }
  }
}
