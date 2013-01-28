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

package com.metamx.druid.query.group;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.FunctionalIterator;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.BufferAggregator;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.collect.ResourceHolder;
import com.metamx.druid.collect.StupidPool;
import com.metamx.druid.index.brita.Filters;
import com.metamx.druid.index.v1.processing.Cursor;
import com.metamx.druid.index.v1.processing.DimensionSelector;
import com.metamx.druid.input.MapBasedRow;
import com.metamx.druid.input.Row;
import com.metamx.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 */
public class GroupByQueryEngine
{
  private final GroupByQueryEngineConfig config;
  private final StupidPool<ByteBuffer> intermediateResultsBufferPool;

  public GroupByQueryEngine (
      GroupByQueryEngineConfig config,
      StupidPool<ByteBuffer> intermediateResultsBufferPool
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

    return new BaseSequence<Row, Iterator<Row>>(
        new BaseSequence.IteratorMaker<Row, Iterator<Row>>()
        {
          @Override
          public Iterator<Row> make()
          {
            return FunctionalIterator
                .create(cursors.iterator())
                .transformCat(
                    new Function<Cursor, Iterator<Row>>()
                    {
                      @Override
                      public Iterator<Row> apply(@Nullable final Cursor cursor)
                      {
                        return new RowIterator(query, cursor, bufferHolder.get());
                      }
                    }
                );
          }

          @Override
          public void cleanup(Iterator<Row> iterFromMake)
          {
            Closeables.closeQuietly(bufferHolder);
          }
        }
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
        for (Integer dimValue : dims.get(0).getRow()) {
          ByteBuffer newKey = key.duplicate();
          newKey.putInt(dimValue);
          final List<ByteBuffer> unaggregatedBuffers = updateValues(newKey, dims.subList(1, dims.size()));
          if (unaggregatedBuffers != null) {
            if (retVal == null) {
              retVal = Lists.newArrayList();
            }
            retVal.addAll(unaggregatedBuffers);
          }
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

  private class RowIterator implements Iterator<Row>
  {
    private final GroupByQuery query;
    private final Cursor cursor;
    private final ByteBuffer metricsBuffer;

    private final List<DimensionSpec> dimensionSpecs;
    private final List<DimensionSelector> dimensions;
    private final String[] dimNames;
    private final List<AggregatorFactory> aggregatorSpecs;
    private final BufferAggregator[] aggregators;
    private final String[] metricNames;
    private final int[] sizesRequired;

    private List<ByteBuffer> unprocessedKeys;
    private Iterator<Row> delegate;

    public RowIterator(GroupByQuery query, Cursor cursor, ByteBuffer metricsBuffer)
    {
      this.query = query;
      this.cursor = cursor;
      this.metricsBuffer = metricsBuffer;

      unprocessedKeys = null;
      delegate = Iterators.emptyIterator();
      dimensionSpecs = query.getDimensions();
      dimensions = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());
      dimNames = new String[dimensionSpecs.size()];
      for (int i = 0; i < dimensionSpecs.size(); ++i) {
        final DimensionSpec dimSpec = dimensionSpecs.get(i);
        final DimensionSelector selector = cursor.makeDimensionSelector(dimSpec.getDimension());
        if (selector != null) {
          dimensions.add(selector);
        }
        dimNames[i] = dimSpec.getOutputName();
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
                    theEvent.put(dimNames[i], dimensions.get(i).lookupName(keyBuffer.getInt()));
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
  }
}
