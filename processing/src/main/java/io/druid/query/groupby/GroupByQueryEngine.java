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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Global;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedLongs;
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
  private static final String CTX_KEY_MAX_INTERMEDIATE_ROWS = "maxIntermediateRows";

  private final Supplier<GroupByQueryConfig> config;
  private final StupidPool<ByteBuffer> intermediateResultsBufferPool;

  private static void getBytesFromBuffer(ByteBuffer src, byte[] dst, int srcOffset, int readLen)
  {
    for (int i = 0; i < readLen; i++) {
      dst[i] = src.get(srcOffset + i);
    }
  }

  private static final Comparator<ByteBuffer> makeKeyComparator(final List<DimensionHandler> dimHandlers)
  {
    Comparator comp = new Comparator<ByteBuffer>()
    {
      @Override
      public int compare(ByteBuffer o1, ByteBuffer o2)
      {
        int pos = 0;
        int limit = o1.limit();
        int ret = 0;
        int dimIndex = 0;
        int maxDimIndex = dimHandlers.size();

        while (pos < limit && dimIndex < maxDimIndex) {
          DimensionHandler handler = dimHandlers.get(dimIndex);
          if (handler == null) {
            // Special time column handling
            int int1 = o1.getInt(pos);
            int int2 = o2.getInt(pos);
            pos += Ints.BYTES;
            ret = Integer.compare(int1, int2);
          } else {
            int valLen = handler.getEncodedValueSize();
            byte[] bytes1 = new byte[valLen];
            byte[] bytes2 = new byte[valLen];

            getBytesFromBuffer(o1, bytes1, pos, valLen);
            getBytesFromBuffer(o2, bytes2, pos, valLen);

            pos += valLen;

            Comparator encodedComparator = handler.getEncodedComparator();
            Comparable val1 = handler.getRowValueFromBytes(bytes1);
            Comparable val2 = handler.getRowValueFromBytes(bytes2);
            ret = encodedComparator.compare(val1, val2);
          }

          if (ret != 0) {
            return ret;
          }

          dimIndex++;
        }
        return ret;
      }
    };
    return comp;
  }

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
        Filters.toFilter(query.getDimFilter()),
        intervals.get(0),
        query.getGranularity(),
        false
    );

    final Map<String, DimensionHandler> dimHandlers = storageAdapter.getDimensionHandlers();

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

  private static class RowUpdater
  {
    private final ByteBuffer metricValues;
    private final BufferAggregator[] aggregators;
    private final PositionMaintainer positionMaintainer;
    private final StorageAdapter adapter;

    private final Map<ByteBuffer, Integer> positions;
    // GroupBy queries tend to do a lot of reads from this. We co-store a hash map to make those reads go faster.
    private final Map<ByteBuffer, Integer> positionsHash = Maps.newHashMap();

    public RowUpdater(
        ByteBuffer metricValues,
        BufferAggregator[] aggregators,
        PositionMaintainer positionMaintainer,
        StorageAdapter adapter,
        List<DimensionHandler> dimHandlers
    )
    {
      this.metricValues = metricValues;
      this.aggregators = aggregators;
      this.positionMaintainer = positionMaintainer;
      this.adapter = adapter;
      this.positions = Maps.newTreeMap(makeKeyComparator(dimHandlers));
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
        final List<String> dimNames,
        final List<DimensionHandler> dimHandlers
    )
    {
      if (dims.size() > 0) {
        List<ByteBuffer> retVal = null;
        List<ByteBuffer> unaggregatedBuffers = null;

        final DimensionSelector dimSelector = dims.get(0);
        final DimensionHandler dimHandler = dimHandlers.get(0);
        final ColumnCapabilities capabilities = adapter.getColumnCapabilities(dimNames.get(0));
        int rowSize = 0;
        IndexedInts intVals = null;
        IndexedLongs longVals = null;
        IndexedFloats floatVals = null;
        Comparable objVal = null;
        ValueType type = null;

        if (dimHandler != null) {
          type = capabilities.getType();
          switch (type) {
            case STRING:
              intVals = dimSelector.getRow();
              rowSize = intVals == null ? 0 : intVals.size();
              break;
            case LONG:
              longVals = dimSelector.getLongRow();
              rowSize = longVals.size();
              break;
            case FLOAT:
              floatVals = dimSelector.getFloatRow();
              rowSize = floatVals.size();
              break;
            case COMPLEX:
              objVal = dimSelector.getComparableRow();
              rowSize = 1;
              break;
            default:
              break;
          }
        } else {
          // Dimension does not exist, dimSelector is a NullDimensionSelector.
          // The null value WILL appear in the result rows.
          // tests in GroupByQueryRunnerTest expect this behavior. (e.g., testGroupByWithMetricColumnDisappears())
          type = ValueType.STRING;
          intVals = dimSelector.getRow();
          rowSize = intVals.size();
        }

        if(dimNames.get(0).equals(Column.TIME_COLUMN_NAME)) {
          type = ValueType.STRING;
          intVals = dimSelector.getRow();
          rowSize = intVals.size();
        }

        switch (type) {
          case STRING:
            if (rowSize == 0) {
              ByteBuffer newKey = key.duplicate();
              newKey.putInt(dimSelector.getValueCardinality());
              unaggregatedBuffers = updateValues(newKey,
                                                 dims.subList(1, dims.size()),
                                                 dimNames.subList(1, dimNames.size()),
                                                 dimHandlers.subList(1, dimHandlers.size())
              );
            }

            for (int i = 0; i < rowSize; i++) {
              int dimValue = intVals.get(i);
              ByteBuffer newKey = key.duplicate();
              byte[] valBytes = Ints.toByteArray(dimValue);
              for (byte byt : valBytes) {
                newKey.put(byt);
              }
              unaggregatedBuffers = updateValues(
                  newKey,
                  dims.subList(1, dims.size()),
                  dimNames.subList(1, dimNames.size()),
                  dimHandlers.subList(1, dimHandlers.size())
              );
            }
            break;
          case LONG:
            for (int i = 0; i < rowSize; i++) {
              long longVal = longVals.get(i);
              ByteBuffer newKey = key.duplicate();
              byte[] valBytes = Longs.toByteArray(longVal);
              for (byte byt : valBytes) {
                newKey.put(byt);
              }
              unaggregatedBuffers = updateValues(
                  newKey,
                  dims.subList(1, dims.size()),
                  dimNames.subList(1, dimNames.size()),
                  dimHandlers.subList(1, dimHandlers.size())
              );
            }
            break;
          case FLOAT:
            for (int i = 0; i < rowSize; i++) {
              float floatVal = floatVals.get(i);
              ByteBuffer newKey = key.duplicate();
              byte[] valBytes = Ints.toByteArray(Float.floatToIntBits(floatVal));
              for (byte byt : valBytes) {
                newKey.put(byt);
              }
              unaggregatedBuffers = updateValues(
                  newKey,
                  dims.subList(1, dims.size()),
                  dimNames.subList(1, dimNames.size()),
                  dimHandlers.subList(1, dimHandlers.size())
              );
            }
            break;
          case COMPLEX:
            ByteBuffer newKey = key.duplicate();
            byte[] valBytes = dimHandler.getBytesFromRowValue(objVal);
            for (byte byt : valBytes) {
              newKey.put(byt);
            }
            unaggregatedBuffers = updateValues(
                newKey,
                dims.subList(1, dims.size()),
                dimNames.subList(1, dimNames.size()),
                dimHandlers.subList(1, dimHandlers.size())
            );
            break;
          default:
            throw new IAE("Invalid type: " + type);
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
    private final int maxIntermediateRows;

    private final List<DimensionSpec> dimensionSpecs;
    private final List<DimensionSelector> dimensions;
    private final ArrayList<String> dimNames;
    private final ArrayList<String> originalDimNames;
    private final List<AggregatorFactory> aggregatorSpecs;
    private final BufferAggregator[] aggregators;
    private final String[] metricNames;
    private final int[] sizesRequired;
    private final StorageAdapter adapter;
    private final List<DimensionHandler> dimHandlers;

    private List<ByteBuffer> unprocessedKeys;
    private Iterator<Row> delegate;
    private int keyBufferSize;

    public RowIterator(GroupByQuery query, final Cursor cursor, ByteBuffer metricsBuffer, GroupByQueryConfig config, StorageAdapter adapter)
    {
      this.query = query;
      this.cursor = cursor;
      this.metricsBuffer = metricsBuffer;
      this.adapter = adapter;

      this.maxIntermediateRows = Math.min(
          query.getContextValue(
              CTX_KEY_MAX_INTERMEDIATE_ROWS,
              config.getMaxIntermediateRows()
          ), config.getMaxIntermediateRows()
      );

      unprocessedKeys = null;
      delegate = Iterators.emptyIterator();
      dimensionSpecs = query.getDimensions();
      dimensions = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());
      dimNames = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());
      originalDimNames = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());
      dimHandlers = Lists.newArrayListWithExpectedSize(dimensionSpecs.size());

      Map<String, DimensionHandler> handlerMap = adapter.getDimensionHandlers();

      for (int i = 0; i < dimensionSpecs.size(); ++i) {
        final DimensionSpec dimSpec = dimensionSpecs.get(i);
        final DimensionSelector selector = cursor.makeDimensionSelector(dimSpec);
        if (selector != null) {
          String dimName = dimSpec.getDimension();
          DimensionHandler handler = handlerMap.get(dimName);
          dimensions.add(selector);
          dimNames.add(dimSpec.getOutputName());
          originalDimNames.add(dimName);
          dimHandlers.add(handlerMap.get(dimName));
          if (dimName.equals(Column.TIME_COLUMN_NAME) || handler == null) {
            keyBufferSize += Ints.BYTES;
          } else {
            keyBufferSize += handler.getEncodedValueSize();
          }
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
      final RowUpdater rowUpdater = new RowUpdater(metricsBuffer, aggregators, positionMaintainer, adapter, dimHandlers);
      if (unprocessedKeys != null) {
        for (ByteBuffer key : unprocessedKeys) {
          final List<ByteBuffer> unprocUnproc = rowUpdater.updateValues(key,
                                                                        ImmutableList.<DimensionSelector>of(),
                                                                        ImmutableList.<String>of(),
                                                                        ImmutableList.<DimensionHandler>of()
                                                                        );
          if (unprocUnproc != null) {
            throw new ISE("Not enough memory to process the request.");
          }
        }
        cursor.advance();
      }
      while (!cursor.isDone() && rowUpdater.getNumRows() < maxIntermediateRows) {
        ByteBuffer key = ByteBuffer.allocate(keyBufferSize);
        unprocessedKeys = rowUpdater.updateValues(key, dimensions, originalDimNames, dimHandlers);
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
                  int pos = 0;
                  for (int i = 0; i < dimensions.size(); ++i) {
                    final ColumnCapabilities capabilities = adapter.getColumnCapabilities(originalDimNames.get(i));
                    final DimensionHandler dimHandler = dimHandlers.get(i);
                    final DimensionSelector dimSelector = dimensions.get(i);

                    ValueType type;
                    int valLen;
                    if (originalDimNames.get(i).equals(Column.TIME_COLUMN_NAME) || dimHandler == null) {
                      type = ValueType.STRING;
                      valLen = Ints.BYTES;
                    } else {
                      type = capabilities.getType();
                      valLen = dimHandler.getEncodedValueSize();
                    }

                    byte[] valBytes = new byte[valLen];
                    getBytesFromBuffer(keyBuffer, valBytes, pos, valLen);
                    pos += valLen;

                    switch (type) {
                      case STRING:
                        int dimVal = Ints.fromByteArray(valBytes);
                        if (dimVal != dimSelector.getValueCardinality()) {
                          theEvent.put(dimNames.get(i), dimSelector.lookupName(dimVal));
                        }
                        break;
                      case LONG:
                        long longVal = Longs.fromByteArray(valBytes);
                        theEvent.put(dimNames.get(i), dimSelector.getExtractedValueLong(longVal));
                        break;
                      case FLOAT:
                        float floatVal = Float.intBitsToFloat(Ints.fromByteArray(valBytes));
                        theEvent.put(dimNames.get(i), dimSelector.getExtractedValueFloat(floatVal));
                        break;
                      case COMPLEX:
                        Comparable objVal = dimHandler.getRowValueFromBytes(valBytes);
                        theEvent.put(dimNames.get(i), dimSelector.getExtractedValueComparable(objVal));
                        break;
                      default:
                        throw new IAE("Invalid type: " + dimSelector.getDimCapabilities().getType());
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
