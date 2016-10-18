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

package io.druid.query.groupby.epinephelinae;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.guava.ResourceClosingSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class GroupByQueryEngineV2
{
  private GroupByQueryEngineV2()
  {
    // No instantiation
  }

  public static Sequence<Row> process(
      final GroupByQuery query,
      final StorageAdapter storageAdapter,
      final StupidPool<ByteBuffer> intermediateResultsBufferPool,
      final GroupByQueryConfig config
  )
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

    final Grouper.KeySerde<ByteBuffer> keySerde = new GroupByEngineKeySerde(query.getDimensions().size());
    final ResourceHolder<ByteBuffer> bufferHolder = intermediateResultsBufferPool.take();

    final String fudgeTimestampString = Strings.emptyToNull(
        query.getContextValue(GroupByStrategyV2.CTX_KEY_FUDGE_TIMESTAMP, "")
    );

    final DateTime fudgeTimestamp = fudgeTimestampString == null
                                    ? null
                                    : new DateTime(Long.parseLong(fudgeTimestampString));

    return Sequences.concat(
        new ResourceClosingSequence<>(
            Sequences.map(
                cursors,
                new Function<Cursor, Sequence<Row>>()
                {
                  @Override
                  public Sequence<Row> apply(final Cursor cursor)
                  {
                    return new BaseSequence<>(
                        new BaseSequence.IteratorMaker<Row, GroupByEngineIterator>()
                        {
                          @Override
                          public GroupByEngineIterator make()
                          {
                            return new GroupByEngineIterator(
                                query,
                                config,
                                cursor,
                                bufferHolder.get(),
                                keySerde,
                                fudgeTimestamp
                            );
                          }

                          @Override
                          public void cleanup(GroupByEngineIterator iterFromMake)
                          {
                            iterFromMake.close();
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

  private static class GroupByEngineIterator implements Iterator<Row>, Closeable
  {
    private final GroupByQuery query;
    private final GroupByQueryConfig querySpecificConfig;
    private final Cursor cursor;
    private final ByteBuffer buffer;
    private final Grouper.KeySerde<ByteBuffer> keySerde;
    private final DateTime timestamp;
    private final DimensionSelector[] selectors;
    private final ByteBuffer keyBuffer;
    private final int[] stack;
    private final IndexedInts[] valuess;

    private int stackp = Integer.MIN_VALUE;
    private boolean currentRowWasPartiallyAggregated = false;
    private CloseableGrouperIterator<ByteBuffer, Row> delegate = null;

    public GroupByEngineIterator(
        final GroupByQuery query,
        final GroupByQueryConfig config,
        final Cursor cursor,
        final ByteBuffer buffer,
        final Grouper.KeySerde<ByteBuffer> keySerde,
        final DateTime fudgeTimestamp
    )
    {
      final int dimCount = query.getDimensions().size();

      this.query = query;
      this.querySpecificConfig = config.withOverrides(query);
      this.cursor = cursor;
      this.buffer = buffer;
      this.keySerde = keySerde;
      this.keyBuffer = ByteBuffer.allocate(keySerde.keySize());
      this.selectors = new DimensionSelector[dimCount];
      for (int i = 0; i < dimCount; i++) {
        this.selectors[i] = cursor.makeDimensionSelector(query.getDimensions().get(i));
      }
      this.stack = new int[dimCount];
      this.valuess = new IndexedInts[dimCount];

      // Time is the same for every row in the cursor
      this.timestamp = fudgeTimestamp != null ? fudgeTimestamp : cursor.getTime();
    }

    @Override
    public Row next()
    {
      if (delegate != null && delegate.hasNext()) {
        return delegate.next();
      }

      if (cursor.isDone()) {
        throw new NoSuchElementException();
      }

      // Make a new delegate iterator
      if (delegate != null) {
        delegate.close();
        delegate = null;
      }

      final Grouper<ByteBuffer> grouper = new BufferGrouper<>(
          buffer,
          keySerde,
          cursor,
          query.getAggregatorSpecs()
               .toArray(new AggregatorFactory[query.getAggregatorSpecs().size()]),
          querySpecificConfig.getBufferGrouperMaxSize(),
          querySpecificConfig.getBufferGrouperMaxLoadFactor(),
          querySpecificConfig.getBufferGrouperInitialBuckets()
      );

outer:
      while (!cursor.isDone()) {
        if (!currentRowWasPartiallyAggregated) {
          // Set up stack, valuess, and first grouping in keyBuffer for this row
          stackp = stack.length - 1;

          for (int i = 0; i < selectors.length; i++) {
            final DimensionSelector selector = selectors[i];

            valuess[i] = selector == null ? EmptyIndexedInts.EMPTY_INDEXED_INTS : selector.getRow();

            final int position = Ints.BYTES * i;
            if (valuess[i].size() == 0) {
              stack[i] = 0;
              keyBuffer.putInt(position, -1);
            } else {
              stack[i] = 1;
              keyBuffer.putInt(position, valuess[i].get(0));
            }
          }
        }

        // Aggregate groupings for this row
        boolean doAggregate = true;
        while (stackp >= -1) {
          // Aggregate additional grouping for this row
          if (doAggregate) {
            keyBuffer.rewind();
            if (!grouper.aggregate(keyBuffer)) {
              // Buffer full while aggregating; break out and resume later
              currentRowWasPartiallyAggregated = true;
              break outer;
            }
            doAggregate = false;
          }

          if (stackp >= 0 && stack[stackp] < valuess[stackp].size()) {
            // Load next value for current slot
            keyBuffer.putInt(
                Ints.BYTES * stackp,
                valuess[stackp].get(stack[stackp])
            );
            stack[stackp]++;

            // Reset later slots
            for (int i = stackp + 1; i < stack.length; i++) {
              final int position = Ints.BYTES * i;
              if (valuess[i].size() == 0) {
                stack[i] = 0;
                keyBuffer.putInt(position, -1);
              } else {
                stack[i] = 1;
                keyBuffer.putInt(position, valuess[i].get(0));
              }
            }

            stackp = stack.length - 1;
            doAggregate = true;
          } else {
            stackp--;
          }
        }

        // Advance to next row
        cursor.advance();
        currentRowWasPartiallyAggregated = false;
      }

      delegate = new CloseableGrouperIterator<>(
          grouper,
          false,
          new Function<Grouper.Entry<ByteBuffer>, Row>()
          {
            @Override
            public Row apply(final Grouper.Entry<ByteBuffer> entry)
            {
              Map<String, Object> theMap = Maps.newLinkedHashMap();

              // Add dimensions.
              for (int i = 0; i < selectors.length; i++) {
                final int id = entry.getKey().getInt(Ints.BYTES * i);

                if (id >= 0) {
                  theMap.put(
                      query.getDimensions().get(i).getOutputName(),
                      selectors[i].lookupName(id)
                  );
                }
              }

              // Add aggregations.
              for (int i = 0; i < entry.getValues().length; i++) {
                theMap.put(query.getAggregatorSpecs().get(i).getName(), entry.getValues()[i]);
              }

              return new MapBasedRow(timestamp, theMap);
            }
          },
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              grouper.close();
            }
          }
      );

      return delegate.next();
    }

    @Override
    public boolean hasNext()
    {
      return (delegate != null && delegate.hasNext()) || !cursor.isDone();
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
      if (delegate != null) {
        delegate.close();
      }
    }
  }

  private static class GroupByEngineKeySerde implements Grouper.KeySerde<ByteBuffer>
  {
    private final int keySize;

    public GroupByEngineKeySerde(final int dimCount)
    {
      this.keySize = dimCount * Ints.BYTES;
    }

    @Override
    public int keySize()
    {
      return keySize;
    }

    @Override
    public Class<ByteBuffer> keyClazz()
    {
      return ByteBuffer.class;
    }

    @Override
    public ByteBuffer toByteBuffer(ByteBuffer key)
    {
      return key;
    }

    @Override
    public ByteBuffer fromByteBuffer(ByteBuffer buffer, int position)
    {
      final ByteBuffer dup = buffer.duplicate();
      dup.position(position).limit(position + keySize);
      return dup.slice();
    }

    @Override
    public Grouper.KeyComparator comparator()
    {
      // No sorting, let mergeRunners handle that
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset()
    {
      // No state, nothing to reset
    }
  }
}
