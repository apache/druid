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

package io.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.FunctionalIterator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.parsers.CloseableIterator;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.common.utils.SocketUtil;
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
import io.druid.segment.UnencodedDimensionSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 */
public class GroupByQueryEngine
{
  private final Supplier<GroupByQueryConfig> config;
  private final StupidPool<ByteBuffer> intermediateResultsBufferPool;

  @Inject
  public GroupByQueryEngine(
      Supplier<GroupByQueryConfig> config,
      @Global StupidPool<ByteBuffer> intermediateResultsBufferPool
  )
  {
    this.config = config;
    this.intermediateResultsBufferPool = intermediateResultsBufferPool;
  }

  public Sequence<Row> process(final GroupByQuery query, final StorageAdapter storageAdapter)
  {
    if (storageAdapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
    }

    final Sequence<Cursor> cursors = storageAdapter.makeCursors(
        Filters.convertDimensionFilters(query.getDimFilter()),
        intervals.get(0),
        query.getGranularity(),
        false
    );

    final ResourceHolder<ByteBuffer> bufferHolder = intermediateResultsBufferPool.take();

    return Sequences.concat(
        Sequences.withBaggage(
            Sequences.map(
                cursors,
                new Function<Cursor, Sequence<Row>>()
                {
                  @Override
                  public Sequence<Row> apply(final Cursor cursor)
                  {
                    return new BaseSequence<>(
                        new BaseSequence.IteratorMaker<Row, RowIterator>()
                        {
                          @Override
                          public RowIterator make()
                          {
                            return new RowIterator(query, cursor, bufferHolder.get(), config.get(), storageAdapter);
                          }

                          @Override
                          public void cleanup(RowIterator iterFromMake)
                          {
                            CloseQuietly.close(iterFromMake);
                          }
                        }
                    );
                  }
                }
            ),
            new Closeable()
            {
              @Override
              public void close() throws IOException
              {
                CloseQuietly.close(bufferHolder);
              }
            }
        )
    );
  }

  private static final Comparator<ByteBuffer> KEY_COMPARATOR = new Comparator<ByteBuffer>()
  {
    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
      int pos = 0;
      int limit = o1.limit();
      int ret = 0;

      while (pos < limit) {
        byte typeMarker1 = o1.get(pos);
        byte typeMarker2 = o2.get(pos);

        pos++;

        // Buffer is allocated assuming Long values, so it's sometimes larger than needed, leaving 0 values at the end
        if(typeMarker1 == 0) {
          return ret;
        }

        ret = Byte.compare(typeMarker1, typeMarker2);
        if (ret != 0) {
          return ret;
        }

        switch (typeMarker1) {
          case RowUpdater.NULL_KEY_MARKER:
            ret = 0;
            break;
          case RowUpdater.LONG_KEY_MARKER:
            long long1 = o1.getLong(pos);
            long long2 = o2.getLong(pos);
            pos += Longs.BYTES;
            ret = Long.compare(long1, long2);
            break;
          case RowUpdater.FLOAT_KEY_MARKER:
            float float1 = o1.getFloat(pos);
            float float2 = o2.getFloat(pos);
            pos += Floats.BYTES;
            ret = Float.compare(float1, float2);
            break;
          case RowUpdater.DICT_ENCODED_KEY_MARKER:
            int int1 = o1.getInt(pos);
            int int2 = o2.getInt(pos);
            pos += Ints.BYTES;
            ret = Integer.compare(int1, int2);
            break;
          default:
            throw new IAE("Invalid type: " + typeMarker1);
        }

        if (ret != 0) {
          return ret;
        }
      }
      return ret;
    }
  };

  private static class RowUpdater
  {
    private static final byte DICT_ENCODED_KEY_MARKER = (byte) 0x01;
    private static final byte LONG_KEY_MARKER = (byte) 0x02;
    private static final byte FLOAT_KEY_MARKER = (byte) 0x03;
    private static final byte NULL_KEY_MARKER = (byte) 0xFF;

    private final ByteBuffer metricValues;
    private final BufferAggregator[] aggregators;
    private final PositionMaintainer positionMaintainer;
    private final StorageAdapter adapter;

    private final Map<ByteBuffer, Integer> positions = Maps.newTreeMap(KEY_COMPARATOR);
    // GroupBy queries tend to do a lot of reads from this. We co-store a hash map to make those reads go faster.
    private final Map<ByteBuffer, Integer> positionsHash = Maps.newHashMap();

    public RowUpdater(
        ByteBuffer metricValues,
        BufferAggregator[] aggregators,
        PositionMaintainer positionMaintainer,
        StorageAdapter adapter
    )
    {
      this.metricValues = metricValues;
      this.aggregators = aggregators;
      this.positionMaintainer = positionMaintainer;
      this.adapter = adapter;
    }

    public int getNumRows()
    {
      return positions.size();
    }

    public Map<ByteBuffer, Integer> getPositions()
    {
      return positions;
    }

    private List<ByteBuffer> updateValues(
        ByteBuffer key,
        List<DimensionSelector> dims,
        List<String> dimNames
    )
    {
      if (dims.size() > 0) {
        List<ByteBuffer> retVal = null;
        List<ByteBuffer> unaggregatedBuffers = null;

        final DimensionSelector dimSelector = dims.get(0);
        final ColumnCapabilities capabilities = adapter.getColumnCapabilities(dimNames.get(0));
        if (capabilities == null || capabilities.isDictionaryEncoded() || (dimNames.get(0).equals(Column.TIME_COLUMN_NAME))) {
          final IndexedInts row = dimSelector.getRow();
          if (row == null || row.size() == 0) {
            ByteBuffer newKey = key.duplicate();
            newKey.put(DICT_ENCODED_KEY_MARKER);
            newKey.putInt(dimSelector.getValueCardinality());
            unaggregatedBuffers = updateValues(
                newKey,
                dims.subList(1, dims.size()),
                dimNames.subList(1, dimNames.size())
            );
          } else {
            for (Integer dimValue : row) {
              ByteBuffer newKey = key.duplicate();
              newKey.put(DICT_ENCODED_KEY_MARKER);
              newKey.putInt(dimValue);
              unaggregatedBuffers = updateValues(
                  newKey,
                  dims.subList(1, dims.size()),
                  dimNames.subList(1, dimNames.size())
              );
            }
          }
        } else {
          final List<Comparable> unencodedRow = dimSelector.getUnencodedRow();
          if (unencodedRow == null || unencodedRow.size() == 0) {
            ByteBuffer newKey = key.duplicate();
            newKey.put(NULL_KEY_MARKER);
            unaggregatedBuffers = updateValues(
                newKey,
                dims.subList(1, dims.size()),
                dimNames.subList(1, dimNames.size())
            );
          } else {
            for (Comparable dimValue : unencodedRow) {
              ByteBuffer newKey = key.duplicate();
              switch (capabilities.getType()) {
                case LONG:
                  newKey.put(LONG_KEY_MARKER);
                  newKey.putLong((Long) dimValue);
                  break;
                case FLOAT:
                  newKey.put(FLOAT_KEY_MARKER);
                  newKey.putFloat((Float) dimValue);
                  break;
                default:
                  throw new IAE("Invalid type: " + capabilities.getType());
              }
              unaggregatedBuffers = updateValues(
                  newKey,
                  dims.subList(1, dims.size()),
                  dimNames.subList(1, dimNames.size())
              );
            }
          }
        }
        if (unaggregatedBuffers != null) {
          if (retVal == null) {
            retVal = Lists.newArrayList();
          }
          retVal.addAll(unaggregatedBuffers);
        }
        return retVal;
      } else {
        key.clear();
        Integer position = positionsHash.get(key);
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
          positionsHash.put(keyCopy, position);
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

  private static class PositionMaintainer
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
      } else {
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

  private static class RowIterator implements CloseableIterator<Row>
  {
    private final GroupByQuery query;
    private final Cursor cursor;
    private final ByteBuffer metricsBuffer;
    private final GroupByQueryConfig config;

    private final List<DimensionSpec> dimensionSpecs;
    private final List<DimensionSelector> dimensions;
    private final ArrayList<String> dimNames;
    private final ArrayList<String> originalDimNames;
    private final List<AggregatorFactory> aggregatorSpecs;
    private final BufferAggregator[] aggregators;
    private final String[] metricNames;
    private final int[] sizesRequired;
    private final StorageAdapter adapter;

    private List<ByteBuffer> unprocessedKeys;
    private Iterator<Row> delegate;

    public RowIterator(
        GroupByQuery query,
        final Cursor cursor,
        ByteBuffer metricsBuffer,
        GroupByQueryConfig config,
        StorageAdapter adapter
    )
    {
      this.query = query;
      this.cursor = cursor;
      this.metricsBuffer = metricsBuffer;
      this.config = config;
      this.adapter = adapter;

      unprocessedKeys = null;
      delegate = Iterators.emptyIterator();
      dimensionSpecs = query.getDimensions();
      dimensions = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());
      dimNames = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());
      originalDimNames = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());


      for (int i = 0; i < dimensionSpecs.size(); ++i) {
        final DimensionSpec dimSpec = dimensionSpecs.get(i);
        final DimensionSelector selector = cursor.makeDimensionSelector(dimSpec);
        if (selector != null) {
          dimensions.add(selector);
          dimNames.add(dimSpec.getOutputName());
          originalDimNames.add(dimSpec.getDimension());
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
      final RowUpdater rowUpdater = new RowUpdater(metricsBuffer, aggregators, positionMaintainer, adapter);
      if (unprocessedKeys != null) {
        for (ByteBuffer key : unprocessedKeys) {
          final List<ByteBuffer> unprocUnproc = rowUpdater.updateValues(key,
                                                                        ImmutableList.<DimensionSelector>of(),
                                                                        ImmutableList.<String>of());
          if (unprocUnproc != null) {
            throw new ISE("Not enough memory to process the request.");
          }
        }
        cursor.advance();
      }
      while (!cursor.isDone() && rowUpdater.getNumRows() < config.getMaxIntermediateRows()) {
        // Per dimension, allocate one byte for key marker and Longs.BYTES for value
        // TODO: Could allocate a smaller buffer than dimensions.size() * Longs.BYTES based on dim types
        ByteBuffer key = ByteBuffer.allocate(dimensions.size() + dimensions.size() * Longs.BYTES);
        unprocessedKeys = rowUpdater.updateValues(key, dimensions, originalDimNames);
        if (unprocessedKeys != null) {
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
                    final ColumnCapabilities capabilities = adapter.getColumnCapabilities(originalDimNames.get(i));
                    if (capabilities == null || capabilities.isDictionaryEncoded() || (originalDimNames.get(i).equals(Column.TIME_COLUMN_NAME))) {
                      final DimensionSelector dimSelector = dimensions.get(i);
                      keyBuffer.get(); // read past the DICT_ENCODED_KEY_MARKER
                      final int dimVal = keyBuffer.getInt();
                      if (dimSelector.getValueCardinality() != dimVal) {
                        theEvent.put(dimNames.get(i), dimSelector.lookupName(dimVal));
                      }
                    } else {

                      final byte typeMarker = keyBuffer.get();
                      Comparable val;
                      switch(typeMarker) {
                        case RowUpdater.NULL_KEY_MARKER:
                          val = null;
                          break;
                        case RowUpdater.LONG_KEY_MARKER:
                          val = keyBuffer.getLong();
                          break;
                        case RowUpdater.FLOAT_KEY_MARKER:
                          val = keyBuffer.getFloat();
                          break;
                        default:
                          throw new UnsupportedOperationException("Invalid type for numeric dim");
                      }
                      val = dimensions.get(i).getExtractedValueFromUnencoded(val);
                      theEvent.put(dimNames.get(i), val);
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

    public void close()
    {
      // cleanup
      for (BufferAggregator agg : aggregators) {
        agg.close();
      }
    }
  }
}
