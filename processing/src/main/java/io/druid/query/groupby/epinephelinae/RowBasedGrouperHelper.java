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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Chars;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.AllGranularity;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.QueryInterruptedException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.NumericColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedInts;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
    final DateTime fudgeTimestamp = GroupByStrategyV2.getUniversalTimestamp(query);
    final Grouper.KeySerdeFactory<RowBasedKey> keySerdeFactory = new RowBasedKeySerdeFactory(
        fudgeTimestamp,
        query.getDimensions().size(),
        querySpecificConfig.getMaxMergingDictionarySize() / (concurrencyHint == -1 ? 1 : concurrencyHint)
    );
    final RowBasedColumnSelectorFactory columnSelectorFactory = new RowBasedColumnSelectorFactory();
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

        long timestamp = row.getTimestampFromEpoch();
        if (isInputRaw) {
          if (query.getGranularity() instanceof AllGranularity) {
            timestamp = query.getIntervals().get(0).getStartMillis();
          } else {
            timestamp = query.getGranularity().truncate(timestamp);
          }
        }

        columnSelectorFactory.setRow(row);
        final String[] dimensions = new String[query.getDimensions().size()];
        for (int i = 0; i < dimensions.length; i++) {
          final String value;
          if (isInputRaw) {
            IndexedInts index = dimensionSelectors[i].getRow();
            value = index.size() == 0 ? "" : dimensionSelectors[i].lookupName(index.get(0));
          } else {
            value = (String) row.getRaw(query.getDimensions().get(i).getOutputName());
          }
          dimensions[i] = Strings.nullToEmpty(value);
        }

        final boolean didAggregate = theGrouper.aggregate(new RowBasedKey(timestamp, dimensions));
        if (!didAggregate) {
          // null return means grouping resources were exhausted.
          return null;
        }
        columnSelectorFactory.setRow(null);

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
    return new CloseableGrouperIterator<>(
        grouper,
        true,
        new Function<Grouper.Entry<RowBasedKey>, Row>()
        {
          @Override
          public Row apply(Grouper.Entry<RowBasedKey> entry)
          {
            Map<String, Object> theMap = Maps.newLinkedHashMap();

            // Add dimensions.
            for (int i = 0; i < entry.getKey().getDimensions().length; i++) {
              theMap.put(
                  query.getDimensions().get(i).getOutputName(),
                  Strings.emptyToNull(entry.getKey().getDimensions()[i])
              );
            }

            // Add aggregations.
            for (int i = 0; i < entry.getValues().length; i++) {
              theMap.put(query.getAggregatorSpecs().get(i).getName(), entry.getValues()[i]);
            }

            return new MapBasedRow(
                query.getGranularity().toDateTime(entry.getKey().getTimestamp()),
                theMap
            );
          }
        },
        closeable
    );
  }

  static class RowBasedKey implements Comparable<RowBasedKey>
  {
    private final long timestamp;
    private final String[] dimensions;

    @JsonCreator
    public RowBasedKey(
        // Using short key names to reduce serialized size when spilling to disk.
        @JsonProperty("t") long timestamp,
        @JsonProperty("d") String[] dimensions
    )
    {
      this.timestamp = timestamp;
      this.dimensions = dimensions;
    }

    @JsonProperty("t")
    public long getTimestamp()
    {
      return timestamp;
    }

    @JsonProperty("d")
    public String[] getDimensions()
    {
      return dimensions;
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

      if (timestamp != that.timestamp) {
        return false;
      }
      // Probably incorrect - comparing Object[] arrays with Arrays.equals
      return Arrays.equals(dimensions, that.dimensions);

    }

    @Override
    public int hashCode()
    {
      int result = (int) (timestamp ^ (timestamp >>> 32));
      result = 31 * result + Arrays.hashCode(dimensions);
      return result;
    }

    @Override
    public int compareTo(RowBasedKey other)
    {
      final int timeCompare = Longs.compare(timestamp, other.getTimestamp());
      if (timeCompare != 0) {
        return timeCompare;
      }

      for (int i = 0; i < dimensions.length; i++) {
        final int cmp = dimensions[i].compareTo(other.getDimensions()[i]);
        if (cmp != 0) {
          return cmp;
        }
      }

      return 0;
    }

    @Override
    public String toString()
    {
      return "RowBasedKey{" +
             "timestamp=" + timestamp +
             ", dimensions=" + Arrays.toString(dimensions) +
             '}';
    }
  }

  private static class RowBasedKeySerdeFactory implements Grouper.KeySerdeFactory<RowBasedKey>
  {
    private final DateTime fudgeTimestamp;
    private final int dimCount;
    private final long maxDictionarySize;

    public RowBasedKeySerdeFactory(DateTime fudgeTimestamp, int dimCount, long maxDictionarySize)
    {
      this.fudgeTimestamp = fudgeTimestamp;
      this.dimCount = dimCount;
      this.maxDictionarySize = maxDictionarySize;
    }

    @Override
    public Grouper.KeySerde<RowBasedKey> factorize()
    {
      return new RowBasedKeySerde(fudgeTimestamp, dimCount, maxDictionarySize);
    }
  }

  private static class RowBasedKeySerde implements Grouper.KeySerde<RowBasedKey>
  {
    // Entry in dictionary, node pointer in reverseDictionary, hash + k/v/next pointer in reverseDictionary nodes
    private static final int ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY = Longs.BYTES * 5 + Ints.BYTES;

    private final DateTime fudgeTimestamp;
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

    public RowBasedKeySerde(
        final DateTime fudgeTimestamp,
        final int dimCount,
        final long maxDictionarySize
    )
    {
      this.fudgeTimestamp = fudgeTimestamp;
      this.dimCount = dimCount;
      this.maxDictionarySize = maxDictionarySize;
      this.keySize = (fudgeTimestamp == null ? Longs.BYTES : 0) + dimCount * Ints.BYTES;
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

      if (fudgeTimestamp == null) {
        keyBuffer.putLong(key.getTimestamp());
      }

      for (int i = 0; i < key.getDimensions().length; i++) {
        final int id = addToDictionary(key.getDimensions()[i]);
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
      final long timestamp = fudgeTimestamp == null ? buffer.getLong(position) : fudgeTimestamp.getMillis();
      final String[] dimensions = new String[dimCount];
      final int dimsPosition = fudgeTimestamp == null ? position + Longs.BYTES : position;
      for (int i = 0; i < dimensions.length; i++) {
        dimensions[i] = dictionary.get(buffer.getInt(dimsPosition + (Ints.BYTES * i)));
      }
      return new RowBasedKey(timestamp, dimensions);
    }

    @Override
    public Grouper.KeyComparator comparator()
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

      if (fudgeTimestamp == null) {
        return new Grouper.KeyComparator()
        {
          @Override
          public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
          {
            final int timeCompare = Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));
            if (timeCompare != 0) {
              return timeCompare;
            }

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
        };
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

  private static class RowBasedColumnSelectorFactory implements ColumnSelectorFactory
  {
    private ThreadLocal<Row> row = new ThreadLocal<>();

    public void setRow(Row row)
    {
      this.row.set(row);
    }

    // This dimension selector does not have an associated lookup dictionary, which means lookup can only be done
    // on the same row. This dimension selector is used for applying the extraction function on dimension, which
    // requires a DimensionSelector implementation
    @Override
    public DimensionSelector makeDimensionSelector(
        DimensionSpec dimensionSpec
    )
    {
      return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
    }

    private DimensionSelector makeDimensionSelectorUndecorated(
        DimensionSpec dimensionSpec
    )
    {
      final String dimension = dimensionSpec.getDimension();
      final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

      return new DimensionSelector()
      {
        @Override
        public IndexedInts getRow()
        {
          final List<String> dimensionValues = row.get().getDimension(dimension);

          final int dimensionValuesSize = dimensionValues != null ? dimensionValues.size() : 0;

          return new IndexedInts()
          {
            @Override
            public int size()
            {
              return dimensionValuesSize;
            }

            @Override
            public int get(int index)
            {
              if (index < 0 || index >= dimensionValuesSize) {
                throw new IndexOutOfBoundsException("index: " + index);
              }
              return index;
            }

            @Override
            public IntIterator iterator()
            {
              return IntIterators.fromTo(0, dimensionValuesSize);
            }

            @Override
            public void close() throws IOException
            {

            }

            @Override
            public void fill(int index, int[] toFill)
            {
              throw new UnsupportedOperationException("fill not supported");
            }
          };
        }

        @Override
        public int getValueCardinality()
        {
          return DimensionSelector.CARDINALITY_UNKNOWN;
        }

        @Override
        public String lookupName(int id)
        {
          final String value = row.get().getDimension(dimension).get(id);
          return extractionFn == null ? value : extractionFn.apply(value);
        }

        @Override
        public int lookupId(String name)
        {
          if (extractionFn != null) {
            throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
          }
          return row.get().getDimension(dimension).indexOf(name);
        }
      };
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(final String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          return row.get().getFloatMetric(columnName);
        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(final String columnName)
    {
      if (columnName.equals(Column.TIME_COLUMN_NAME)) {
        return new LongColumnSelector()
        {
          @Override
          public long get()
          {
            return row.get().getTimestampFromEpoch();
          }
        };
      }
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          return row.get().getLongMetric(columnName);
        }
      };
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(final String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public Object get()
        {
          return row.get().getRaw(columnName);
        }
      };
    }

    @Override
    public NumericColumnSelector makeMathExpressionSelector(String expression)
    {
      final Expr parsed = Parser.parse(expression);

      final List<String> required = Parser.findRequiredBindings(parsed);
      final Map<String, Supplier<Number>> values = Maps.newHashMapWithExpectedSize(required.size());

      for (final String columnName : required) {
        values.put(
            columnName, new Supplier<Number>()
            {
              @Override
              public Number get()
              {
                return Evals.toNumber(row.get().getRaw(columnName));
              }
            }
        );
      }
      final Expr.ObjectBinding binding = Parser.withSuppliers(values);

      return new NumericColumnSelector()
      {
        @Override
        public Number get()
        {
          return parsed.eval(binding);
        }
      };
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      // We don't have any information on the column value type, returning null defaults type to string
      return null;
    }
  }

}
