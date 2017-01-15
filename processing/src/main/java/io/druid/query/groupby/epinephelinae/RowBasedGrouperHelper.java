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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Chars;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.granularity.AllGranularity;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.query.QueryInterruptedException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.groupby.RowBasedColumnSelectorFactory;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

// this class contains shared code between GroupByMergingQueryRunnerV2 and GroupByRowProcessor
public class RowBasedGrouperHelper
{

  public static Pair<Grouper<RowBasedKey>, Accumulator<Grouper<RowBasedKey>, Row>> createGrouperAccumulatorPair(
      final GroupByQuery query,
      final boolean isInputRaw,
      final GroupByQueryConfig config,
      final ByteBuffer buffer,
      final int concurrencyHint,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final AggregatorFactory[] aggregatorFactories
  )
  {
    // concurrencyHint >= 1 for concurrent groupers, -1 for single-threaded
    Preconditions.checkArgument(concurrencyHint >= 1 || concurrencyHint == -1, "invalid concurrencyHint");

    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);
    final boolean includeTimestamp = GroupByStrategyV2.getUniversalTimestamp(query) == null;
    final Grouper.KeySerdeFactory<RowBasedKey> keySerdeFactory = new RowBasedKeySerdeFactory(
        includeTimestamp,
        query.getContextSortByDimsFirst(),
        query.getDimensions().size(),
        querySpecificConfig.getMaxMergingDictionarySize() / (concurrencyHint == -1 ? 1 : concurrencyHint)
    );
    final ThreadLocal<Row> columnSelectorRow = new ThreadLocal<>();
    final ColumnSelectorFactory columnSelectorFactory = RowBasedColumnSelectorFactory.create(
        columnSelectorRow,
        GroupByQueryHelper.rowSignatureFor(query)
    );
    final Grouper<RowBasedKey> grouper;
    if (concurrencyHint == -1) {
      grouper = new SpillingGrouper<>(
          buffer,
          keySerdeFactory,
          columnSelectorFactory,
          aggregatorFactories,
          querySpecificConfig.getBufferGrouperMaxSize(),
          querySpecificConfig.getBufferGrouperMaxLoadFactor(),
          querySpecificConfig.getBufferGrouperInitialBuckets(),
          temporaryStorage,
          spillMapper,
          true
      );
    } else {
      grouper = new ConcurrentGrouper<>(
          buffer,
          keySerdeFactory,
          columnSelectorFactory,
          aggregatorFactories,
          querySpecificConfig.getBufferGrouperMaxSize(),
          querySpecificConfig.getBufferGrouperMaxLoadFactor(),
          querySpecificConfig.getBufferGrouperInitialBuckets(),
          temporaryStorage,
          spillMapper,
          concurrencyHint
      );
    }

    final DimensionSelector[] dimensionSelectors;
    if (isInputRaw) {
      dimensionSelectors = new DimensionSelector[query.getDimensions().size()];
      for (int i = 0; i < dimensionSelectors.length; i++) {
        dimensionSelectors[i] = columnSelectorFactory.makeDimensionSelector(query.getDimensions().get(i));
      }
    } else {
      dimensionSelectors = null;
    }

    final Accumulator<Grouper<RowBasedKey>, Row> accumulator = new Accumulator<Grouper<RowBasedKey>, Row>()
    {
      @Override
      public Grouper<RowBasedKey> accumulate(
          final Grouper<RowBasedKey> theGrouper,
          final Row row
      )
      {
        if (Thread.interrupted()) {
          throw new QueryInterruptedException(new InterruptedException());
        }

        if (theGrouper == null) {
          // Pass-through null returns without doing more work.
          return null;
        }

        columnSelectorRow.set(row);

        final int dimStart;
        final Comparable[] key;

        if (includeTimestamp) {
          key = new Comparable[query.getDimensions().size() + 1];

          final long timestamp;
          if (isInputRaw) {
            if (query.getGranularity() instanceof AllGranularity) {
              timestamp = query.getIntervals().get(0).getStartMillis();
            } else {
              timestamp = query.getGranularity().truncate(row.getTimestamp()).getMillis();
            }
          } else {
            timestamp = row.getTimestampFromEpoch();
          }

          key[0] = timestamp;
          dimStart = 1;
        } else {
          key = new Comparable[query.getDimensions().size()];
          dimStart = 0;
        }

        for (int i = dimStart; i < key.length; i++) {
          final String value;
          if (isInputRaw) {
            IndexedInts index = dimensionSelectors[i - dimStart].getRow();
            value = index.size() == 0 ? "" : dimensionSelectors[i - dimStart].lookupName(index.get(0));
          } else {
            value = (String) row.getRaw(query.getDimensions().get(i - dimStart).getOutputName());
          }
          key[i] = Strings.nullToEmpty(value);
        }

        final boolean didAggregate = theGrouper.aggregate(new RowBasedKey(key));
        if (!didAggregate) {
          // null return means grouping resources were exhausted.
          return null;
        }
        columnSelectorRow.set(null);

        return theGrouper;
      }
    };

    return new Pair<>(grouper, accumulator);
  }

  public static CloseableGrouperIterator<RowBasedKey, Row> makeGrouperIterator(
      final Grouper<RowBasedKey> grouper,
      final GroupByQuery query,
      final Closeable closeable
  )
  {
    final boolean includeTimestamp = GroupByStrategyV2.getUniversalTimestamp(query) == null;

    return new CloseableGrouperIterator<>(
        grouper,
        true,
        new Function<Grouper.Entry<RowBasedKey>, Row>()
        {
          @Override
          public Row apply(Grouper.Entry<RowBasedKey> entry)
          {
            Map<String, Object> theMap = Maps.newLinkedHashMap();

            // Get timestamp, maybe.
            final DateTime timestamp;
            final int dimStart;

            if (includeTimestamp) {
              timestamp = query.getGranularity().toDateTime(((long) (entry.getKey().getKey()[0])));
              dimStart = 1;
            } else {
              timestamp = null;
              dimStart = 0;
            }

            // Add dimensions.
            for (int i = dimStart; i < entry.getKey().getKey().length; i++) {
              theMap.put(
                  query.getDimensions().get(i - dimStart).getOutputName(),
                  Strings.emptyToNull((String) entry.getKey().getKey()[i])
              );
            }

            // Add aggregations.
            for (int i = 0; i < entry.getValues().length; i++) {
              theMap.put(query.getAggregatorSpecs().get(i).getName(), entry.getValues()[i]);
            }

            return new MapBasedRow(timestamp, theMap);
          }
        },
        closeable
    );
  }

  static class RowBasedKey
  {
    private final Object[] key;

    RowBasedKey(final Object[] key)
    {
      this.key = key;
    }

    @JsonCreator
    public static RowBasedKey fromJsonArray(final Object[] key)
    {
      // Type info is lost during serde. We know we don't want ints as timestamps, so adjust.
      if (key.length > 0 && key[0] instanceof Integer) {
        key[0] = ((Integer) key[0]).longValue();
      }

      return new RowBasedKey(key);
    }

    @JsonValue
    public Object[] getKey()
    {
      return key;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      RowBasedKey that = (RowBasedKey) o;

      return Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode()
    {
      return Arrays.hashCode(key);
    }

    @Override
    public String toString()
    {
      return Arrays.toString(key);
    }
  }

  private static class RowBasedKeySerdeFactory implements Grouper.KeySerdeFactory<RowBasedKey>
  {
    private final boolean includeTimestamp;
    private final boolean sortByDimsFirst;
    private final int dimCount;
    private final long maxDictionarySize;

    RowBasedKeySerdeFactory(boolean includeTimestamp, boolean sortByDimsFirst, int dimCount, long maxDictionarySize)
    {
      this.includeTimestamp = includeTimestamp;
      this.sortByDimsFirst = sortByDimsFirst;
      this.dimCount = dimCount;
      this.maxDictionarySize = maxDictionarySize;
    }

    @Override
    public Grouper.KeySerde<RowBasedKey> factorize()
    {
      return new RowBasedKeySerde(includeTimestamp, sortByDimsFirst, dimCount, maxDictionarySize);
    }

    @Override
    public Comparator<RowBasedKey> objectComparator()
    {
      if (includeTimestamp) {
        if (sortByDimsFirst) {
          return new Comparator<RowBasedKey>()
          {
            @Override
            public int compare(RowBasedKey key1, RowBasedKey key2)
            {
              final int cmp = compareDimsInRows(key1, key2, 1);
              if (cmp != 0) {
                return cmp;
              }

              return Longs.compare((long) key1.getKey()[0], (long) key2.getKey()[0]);
            }
          };
        } else {
          return new Comparator<RowBasedKey>()
          {
            @Override
            public int compare(RowBasedKey key1, RowBasedKey key2)
            {
              final int timeCompare = Longs.compare((long) key1.getKey()[0], (long) key2.getKey()[0]);

              if (timeCompare != 0) {
                return timeCompare;
              }

              return compareDimsInRows(key1, key2, 1);
            }
          };
        }
      } else {
        return new Comparator<RowBasedKey>()
        {
          @Override
          public int compare(RowBasedKey key1, RowBasedKey key2)
          {
            return compareDimsInRows(key1, key2, 0);
          }
        };
      }
    }

    private static int compareDimsInRows(RowBasedKey key1, RowBasedKey key2, int dimStart)
    {
      for (int i = dimStart; i < key1.getKey().length; i++) {
        final int cmp = ((String) key1.getKey()[i]).compareTo((String) key2.getKey()[i]);
        if (cmp != 0) {
          return cmp;
        }
      }

      return 0;
    };
  }

  private static class RowBasedKeySerde implements Grouper.KeySerde<RowBasedKey>
  {
    // Entry in dictionary, node pointer in reverseDictionary, hash + k/v/next pointer in reverseDictionary nodes
    private static final int ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY = Longs.BYTES * 5 + Ints.BYTES;

    private final boolean includeTimestamp;
    private final boolean sortByDimsFirst;
    private final int dimCount;
    private final int keySize;
    private final ByteBuffer keyBuffer;
    private final List<String> dictionary = Lists.newArrayList();
    private final Map<String, Integer> reverseDictionary = Maps.newHashMap();

    // Size limiting for the dictionary, in (roughly estimated) bytes.
    private final long maxDictionarySize;
    private long currentEstimatedSize = 0;

    // dictionary id -> its position if it were sorted by dictionary value
    private int[] sortableIds = null;

    RowBasedKeySerde(
        final boolean includeTimestamp,
        final boolean sortByDimsFirst,
        final int dimCount,
        final long maxDictionarySize
    )
    {
      this.includeTimestamp = includeTimestamp;
      this.sortByDimsFirst = sortByDimsFirst;
      this.dimCount = dimCount;
      this.maxDictionarySize = maxDictionarySize;
      this.keySize = (includeTimestamp ? Longs.BYTES : 0) + dimCount * Ints.BYTES;
      this.keyBuffer = ByteBuffer.allocate(keySize);
    }

    @Override
    public int keySize()
    {
      return keySize;
    }

    @Override
    public Class<RowBasedKey> keyClazz()
    {
      return RowBasedKey.class;
    }

    @Override
    public ByteBuffer toByteBuffer(RowBasedKey key)
    {
      keyBuffer.rewind();

      final int dimStart;
      if (includeTimestamp) {
        keyBuffer.putLong((long) key.getKey()[0]);
        dimStart = 1;
      } else {
        dimStart = 0;
      }

      for (int i = dimStart; i < key.getKey().length; i++) {
        final int id = addToDictionary((String) key.getKey()[i]);
        if (id < 0) {
          return null;
        }
        keyBuffer.putInt(id);
      }

      keyBuffer.flip();
      return keyBuffer;
    }

    @Override
    public RowBasedKey fromByteBuffer(ByteBuffer buffer, int position)
    {
      final int dimStart;
      final Comparable[] key;
      final int dimsPosition;

      if (includeTimestamp) {
        key = new Comparable[dimCount + 1];
        key[0] = buffer.getLong(position);
        dimsPosition = position + Longs.BYTES;
        dimStart = 1;
      } else {
        key = new Comparable[dimCount];
        dimsPosition = position;
        dimStart = 0;
      }

      for (int i = dimStart; i < key.length; i++) {
        key[i] = dictionary.get(buffer.getInt(dimsPosition + (Ints.BYTES * (i - dimStart))));
      }

      return new RowBasedKey(key);
    }

    @Override
    public Grouper.KeyComparator bufferComparator()
    {
      if (sortableIds == null) {
        Map<String, Integer> sortedMap = Maps.newTreeMap();
        for (int id = 0; id < dictionary.size(); id++) {
          sortedMap.put(dictionary.get(id), id);
        }
        sortableIds = new int[dictionary.size()];
        int index = 0;
        for (final Integer id : sortedMap.values()) {
          sortableIds[id] = index++;
        }
      }

      if (includeTimestamp) {
        if (sortByDimsFirst) {
          return new Grouper.KeyComparator()
          {
            @Override
            public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
            {
              final int cmp = compareDimsInBuffersForNullFudgeTimestamp(
                  sortableIds,
                  dimCount,
                  lhsBuffer,
                  rhsBuffer,
                  lhsPosition,
                  rhsPosition
              );
              if (cmp != 0) {
                return cmp;
              }

              return Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));
            }
          };
        } else {
          return new Grouper.KeyComparator()
          {
            @Override
            public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
            {
              final int timeCompare = Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));

              if (timeCompare != 0) {
                return timeCompare;
              }

              return compareDimsInBuffersForNullFudgeTimestamp(
                  sortableIds,
                  dimCount,
                  lhsBuffer,
                  rhsBuffer,
                  lhsPosition,
                  rhsPosition
              );
            }
          };
        }
      } else {
        return new Grouper.KeyComparator()
        {
          @Override
          public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
          {
            for (int i = 0; i < dimCount; i++) {
              final int cmp = Ints.compare(
                  sortableIds[lhsBuffer.getInt(lhsPosition + (Ints.BYTES * i))],
                  sortableIds[rhsBuffer.getInt(rhsPosition + (Ints.BYTES * i))]
              );

              if (cmp != 0) {
                return cmp;
              }
            }

            return 0;
          }
        };
      }
    }

    private static int compareDimsInBuffersForNullFudgeTimestamp(
        int[] sortableIds,
        int dimCount,
        ByteBuffer lhsBuffer,
        ByteBuffer rhsBuffer,
        int lhsPosition,
        int rhsPosition
    )
    {
      for (int i = 0; i < dimCount; i++) {
        final int cmp = Ints.compare(
            sortableIds[lhsBuffer.getInt(lhsPosition + Longs.BYTES + (Ints.BYTES * i))],
            sortableIds[rhsBuffer.getInt(rhsPosition + Longs.BYTES + (Ints.BYTES * i))]
        );

        if (cmp != 0) {
          return cmp;
        }
      }

      return 0;
    }

    @Override
    public void reset()
    {
      dictionary.clear();
      reverseDictionary.clear();
      sortableIds = null;
      currentEstimatedSize = 0;
    }

    /**
     * Adds s to the dictionary. If the dictionary's size limit would be exceeded by adding this key, then
     * this returns -1.
     *
     * @param s a string
     *
     * @return id for this string, or -1
     */
    private int addToDictionary(final String s)
    {
      Integer idx = reverseDictionary.get(s);
      if (idx == null) {
        final long additionalEstimatedSize = (long) s.length() * Chars.BYTES + ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY;
        if (currentEstimatedSize + additionalEstimatedSize > maxDictionarySize) {
          return -1;
        }

        idx = dictionary.size();
        reverseDictionary.put(s, idx);
        dictionary.add(s);
        currentEstimatedSize += additionalEstimatedSize;
      }
      return idx;
    }
  }
}
