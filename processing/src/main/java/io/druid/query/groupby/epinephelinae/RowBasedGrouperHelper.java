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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Chars;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.druid.collections.ResourceHolder;
import io.druid.common.utils.IntArrayUtils;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.granularity.AllGranularity;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.query.BaseQuery;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.ColumnSelectorStrategy;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.RowBasedColumnSelectorFactory;
import io.druid.query.groupby.epinephelinae.Grouper.BufferComparator;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.BaseDoubleColumnValueSelector;
import io.druid.segment.BaseFloatColumnValueSelector;
import io.druid.segment.BaseLongColumnValueSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.DimensionSelector;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

// this class contains shared code between GroupByMergingQueryRunnerV2 and GroupByRowProcessor
public class RowBasedGrouperHelper
{
  // Entry in dictionary, node pointer in reverseDictionary, hash + k/v/next pointer in reverseDictionary nodes
  private static final int ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY = Longs.BYTES * 5 + Ints.BYTES;

  private static final int SINGLE_THREAD_CONCURRENCY_HINT = -1;
  private static final int UNKNOWN_THREAD_PRIORITY = -1;
  private static final long UNKNOWN_TIMEOUT = -1L;

  /**
   * Create a single-threaded grouper and accumulator.
   */
  public static Pair<Grouper<RowBasedKey>, Accumulator<AggregateResult, Row>> createGrouperAccumulatorPair(
      final GroupByQuery query,
      final boolean isInputRaw,
      final Map<String, ValueType> rawInputRowSignature,
      final GroupByQueryConfig config,
      final Supplier<ByteBuffer> bufferSupplier,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final AggregatorFactory[] aggregatorFactories,
      final int mergeBufferSize
  )
  {
    return createGrouperAccumulatorPair(
        query,
        isInputRaw,
        rawInputRowSignature,
        config,
        bufferSupplier,
        null,
        SINGLE_THREAD_CONCURRENCY_HINT,
        temporaryStorage,
        spillMapper,
        aggregatorFactories,
        null,
        UNKNOWN_THREAD_PRIORITY,
        false,
        UNKNOWN_TIMEOUT,
        mergeBufferSize
    );
  }

  /**
   * If isInputRaw is true, transformations such as timestamp truncation and extraction functions have not
   * been applied to the input rows yet, for example, in a nested query, if an extraction function is being
   * applied in the outer query to a field of the inner query. This method must apply those transformations.
   */
  public static Pair<Grouper<RowBasedKey>, Accumulator<AggregateResult, Row>> createGrouperAccumulatorPair(
      final GroupByQuery query,
      final boolean isInputRaw,
      final Map<String, ValueType> rawInputRowSignature,
      final GroupByQueryConfig config,
      final Supplier<ByteBuffer> bufferSupplier,
      final Supplier<ResourceHolder<ByteBuffer>> combineBufferSupplier,
      final int concurrencyHint,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final AggregatorFactory[] aggregatorFactories,
      @Nullable final ListeningExecutorService grouperSorter,
      final int priority,
      final boolean hasQueryTimeout,
      final long queryTimeoutAt,
      final int mergeBufferSize
  )
  {
    // concurrencyHint >= 1 for concurrent groupers, -1 for single-threaded
    Preconditions.checkArgument(concurrencyHint >= 1 || concurrencyHint == -1, "invalid concurrencyHint");

    final List<ValueType> valueTypes = DimensionHandlerUtils.getValueTypesFromDimensionSpecs(query.getDimensions());

    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);
    final boolean includeTimestamp = GroupByStrategyV2.getUniversalTimestamp(query) == null;

    final ThreadLocal<Row> columnSelectorRow = new ThreadLocal<>();
    final ColumnSelectorFactory columnSelectorFactory = query.getVirtualColumns().wrap(
        RowBasedColumnSelectorFactory.create(
            columnSelectorRow,
            rawInputRowSignature
        )
    );

    final boolean willApplyLimitPushDown = query.isApplyLimitPushDown();
    final DefaultLimitSpec limitSpec = willApplyLimitPushDown ? (DefaultLimitSpec) query.getLimitSpec() : null;
    boolean sortHasNonGroupingFields = false;
    if (willApplyLimitPushDown) {
      sortHasNonGroupingFields = DefaultLimitSpec.sortingOrderHasNonGroupingFields(
          limitSpec,
          query.getDimensions()
      );
    }

    final Grouper.KeySerdeFactory<RowBasedKey> keySerdeFactory = new RowBasedKeySerdeFactory(
        includeTimestamp,
        query.getContextSortByDimsFirst(),
        query.getDimensions(),
        querySpecificConfig.getMaxMergingDictionarySize() / (concurrencyHint == -1 ? 1 : concurrencyHint),
        valueTypes,
        aggregatorFactories,
        limitSpec
    );

    final Grouper<RowBasedKey> grouper;
    if (concurrencyHint == -1) {
      grouper = new SpillingGrouper<>(
          bufferSupplier,
          keySerdeFactory,
          columnSelectorFactory,
          aggregatorFactories,
          querySpecificConfig.getBufferGrouperMaxSize(),
          querySpecificConfig.getBufferGrouperMaxLoadFactor(),
          querySpecificConfig.getBufferGrouperInitialBuckets(),
          temporaryStorage,
          spillMapper,
          true,
          limitSpec,
          sortHasNonGroupingFields,
          mergeBufferSize
      );
    } else {
      final Grouper.KeySerdeFactory<RowBasedKey> combineKeySerdeFactory = new RowBasedKeySerdeFactory(
          includeTimestamp,
          query.getContextSortByDimsFirst(),
          query.getDimensions(),
          querySpecificConfig.getMaxMergingDictionarySize(), // use entire dictionary space for combining key serde
          valueTypes,
          aggregatorFactories,
          limitSpec
      );

      grouper = new ConcurrentGrouper<>(
          querySpecificConfig,
          bufferSupplier,
          combineBufferSupplier,
          keySerdeFactory,
          combineKeySerdeFactory,
          columnSelectorFactory,
          aggregatorFactories,
          temporaryStorage,
          spillMapper,
          concurrencyHint,
          limitSpec,
          sortHasNonGroupingFields,
          grouperSorter,
          priority,
          hasQueryTimeout,
          queryTimeoutAt
      );
    }

    final int keySize = includeTimestamp ? query.getDimensions().size() + 1 : query.getDimensions().size();
    final ValueExtractFunction valueExtractFn = makeValueExtractFunction(
        query,
        isInputRaw,
        includeTimestamp,
        columnSelectorFactory,
        valueTypes
    );

    final Accumulator<AggregateResult, Row> accumulator = new Accumulator<AggregateResult, Row>()
    {
      @Override
      public AggregateResult accumulate(
          final AggregateResult priorResult,
          final Row row
      )
      {
        BaseQuery.checkInterrupted();

        if (priorResult != null && !priorResult.isOk()) {
          // Pass-through error returns without doing more work.
          return priorResult;
        }

        if (!grouper.isInitialized()) {
          grouper.init();
        }

        columnSelectorRow.set(row);

        final Comparable[] key = new Comparable[keySize];
        valueExtractFn.apply(row, key);

        final AggregateResult aggregateResult = grouper.aggregate(new RowBasedKey(key));
        columnSelectorRow.set(null);

        return aggregateResult;
      }
    };

    return new Pair<>(grouper, accumulator);
  }

  private interface TimestampExtractFunction
  {
    long apply(Row row);
  }

  private static TimestampExtractFunction makeTimestampExtractFunction(
      final GroupByQuery query,
      final boolean isInputRaw
  )
  {
    if (isInputRaw) {
      if (query.getGranularity() instanceof AllGranularity) {
        return new TimestampExtractFunction()
        {
          @Override
          public long apply(Row row)
          {
            return query.getIntervals().get(0).getStartMillis();
          }
        };
      } else {
        return new TimestampExtractFunction()
        {
          @Override
          public long apply(Row row)
          {
            return query.getGranularity().bucketStart(row.getTimestamp()).getMillis();
          }
        };
      }
    } else {
      return new TimestampExtractFunction()
      {
        @Override
        public long apply(Row row)
        {
          return row.getTimestampFromEpoch();
        }
      };
    }
  }

  private interface ValueExtractFunction
  {
    Comparable[] apply(Row row, Comparable[] key);
  }

  private static ValueExtractFunction makeValueExtractFunction(
      final GroupByQuery query,
      final boolean isInputRaw,
      final boolean includeTimestamp,
      final ColumnSelectorFactory columnSelectorFactory,
      final List<ValueType> valueTypes
  )
  {
    final TimestampExtractFunction timestampExtractFn = includeTimestamp ?
                                                        makeTimestampExtractFunction(query, isInputRaw) :
                                                        null;

    final Function<Comparable, Comparable>[] valueConvertFns = makeValueConvertFunctions(valueTypes);

    if (isInputRaw) {
      final Supplier<Comparable>[] inputRawSuppliers = getValueSuppliersForDimensions(
          columnSelectorFactory,
          query.getDimensions()
      );

      if (includeTimestamp) {
        return new ValueExtractFunction()
        {
          @Override
          public Comparable[] apply(Row row, Comparable[] key)
          {
            key[0] = timestampExtractFn.apply(row);
            for (int i = 1; i < key.length; i++) {
              final Comparable val = inputRawSuppliers[i - 1].get();
              key[i] = valueConvertFns[i - 1].apply(val);
            }
            return key;
          }
        };
      } else {
        return new ValueExtractFunction()
        {
          @Override
          public Comparable[] apply(Row row, Comparable[] key)
          {
            for (int i = 0; i < key.length; i++) {
              final Comparable val = inputRawSuppliers[i].get();
              key[i] = valueConvertFns[i].apply(val);
            }
            return key;
          }
        };
      }
    } else {
      if (includeTimestamp) {
        return new ValueExtractFunction()
        {
          @Override
          public Comparable[] apply(Row row, Comparable[] key)
          {
            key[0] = timestampExtractFn.apply(row);
            for (int i = 1; i < key.length; i++) {
              final Comparable val = (Comparable) row.getRaw(query.getDimensions().get(i - 1).getOutputName());
              key[i] = valueConvertFns[i - 1].apply(val);
            }
            return key;
          }
        };
      } else {
        return new ValueExtractFunction()
        {
          @Override
          public Comparable[] apply(Row row, Comparable[] key)
          {
            for (int i = 0; i < key.length; i++) {
              final Comparable val = (Comparable) row.getRaw(query.getDimensions().get(i).getOutputName());
              key[i] = valueConvertFns[i].apply(val);
            }
            return key;
          }
        };
      }
    }
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
              Object dimVal = entry.getKey().getKey()[i];
              theMap.put(
                  query.getDimensions().get(i - dimStart).getOutputName(),
                  dimVal instanceof String ? Strings.emptyToNull((String) dimVal) : dimVal
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
      // Type info is lost during serde:
      // Floats may be deserialized as doubles, Longs may be deserialized as integers, convert them back
      for (int i = 0; i < key.length; i++) {
        if (key[i] instanceof Integer) {
          key[i] = ((Integer) key[i]).longValue();
        } else if (key[i] instanceof Double) {
          key[i] = ((Double) key[i]).floatValue();
        }
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

  private static final InputRawSupplierColumnSelectorStrategyFactory STRATEGY_FACTORY =
      new InputRawSupplierColumnSelectorStrategyFactory();

  private interface InputRawSupplierColumnSelectorStrategy<ValueSelectorType> extends ColumnSelectorStrategy
  {
    Supplier<Comparable> makeInputRawSupplier(ValueSelectorType selector);
  }

  private static class StringInputRawSupplierColumnSelectorStrategy
      implements InputRawSupplierColumnSelectorStrategy<DimensionSelector>
  {
    @Override
    public Supplier<Comparable> makeInputRawSupplier(DimensionSelector selector)
    {
      return new Supplier<Comparable>()
      {
        @Override
        public Comparable get()
        {
          final String value;
          IndexedInts index = selector.getRow();
          value = index.size() == 0
                  ? ""
                  : selector.lookupName(index.get(0));
          return Strings.nullToEmpty(value);
        }
      };
    }
  }

  private static class InputRawSupplierColumnSelectorStrategyFactory
      implements ColumnSelectorStrategyFactory<InputRawSupplierColumnSelectorStrategy>
  {
    @Override
    public InputRawSupplierColumnSelectorStrategy makeColumnSelectorStrategy(
        ColumnCapabilities capabilities, ColumnValueSelector selector
    )
    {
      ValueType type = capabilities.getType();
      switch (type) {
        case STRING:
          return new StringInputRawSupplierColumnSelectorStrategy();
        case LONG:
          return (InputRawSupplierColumnSelectorStrategy<BaseLongColumnValueSelector>)
              columnSelector -> columnSelector::getLong;
        case FLOAT:
          return (InputRawSupplierColumnSelectorStrategy<BaseFloatColumnValueSelector>)
              columnSelector -> columnSelector::getFloat;
        case DOUBLE:
          return (InputRawSupplierColumnSelectorStrategy<BaseDoubleColumnValueSelector>)
              columnSelector -> columnSelector::getDouble;
        default:
          throw new IAE("Cannot create query type helper from invalid type [%s]", type);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Supplier<Comparable>[] getValueSuppliersForDimensions(
      final ColumnSelectorFactory columnSelectorFactory,
      final List<DimensionSpec> dimensions
  )
  {
    final Supplier[] inputRawSuppliers = new Supplier[dimensions.size()];
    final ColumnSelectorPlus[] selectorPluses = DimensionHandlerUtils.createColumnSelectorPluses(
        STRATEGY_FACTORY,
        dimensions,
        columnSelectorFactory
    );

    for (int i = 0; i < selectorPluses.length; i++) {
      final ColumnSelectorPlus<InputRawSupplierColumnSelectorStrategy> selectorPlus = selectorPluses[i];
      final InputRawSupplierColumnSelectorStrategy strategy = selectorPlus.getColumnSelectorStrategy();
      inputRawSuppliers[i] = strategy.makeInputRawSupplier(selectorPlus.getSelector());
    }

    return inputRawSuppliers;
  }

  @SuppressWarnings("unchecked")
  private static Function<Comparable, Comparable>[] makeValueConvertFunctions(
      final List<ValueType> valueTypes
  )
  {
    final Function<Comparable, Comparable>[] functions = new Function[valueTypes.size()];
    for (int i = 0; i < functions.length; i++) {
      // Subquery post-aggs aren't added to the rowSignature (see rowSignatureFor() in GroupByQueryHelper) because
      // their types aren't known, so default to String handling.
      final ValueType type = valueTypes.get(i) == null ? ValueType.STRING : valueTypes.get(i);
      functions[i] = input -> DimensionHandlerUtils.convertObjectToTypeNonNull(input, type);
    }
    return functions;
  }

  private static class RowBasedKeySerdeFactory implements Grouper.KeySerdeFactory<RowBasedKey>
  {
    private final boolean includeTimestamp;
    private final boolean sortByDimsFirst;
    private final int dimCount;
    private final long maxDictionarySize;
    private final DefaultLimitSpec limitSpec;
    private final List<DimensionSpec> dimensions;
    final AggregatorFactory[] aggregatorFactories;
    private final List<ValueType> valueTypes;

    RowBasedKeySerdeFactory(
        boolean includeTimestamp,
        boolean sortByDimsFirst,
        List<DimensionSpec> dimensions,
        long maxDictionarySize,
        List<ValueType> valueTypes,
        final AggregatorFactory[] aggregatorFactories,
        DefaultLimitSpec limitSpec
    )
    {
      this.includeTimestamp = includeTimestamp;
      this.sortByDimsFirst = sortByDimsFirst;
      this.dimensions = dimensions;
      this.dimCount = dimensions.size();
      this.maxDictionarySize = maxDictionarySize;
      this.limitSpec = limitSpec;
      this.aggregatorFactories = aggregatorFactories;
      this.valueTypes = valueTypes;
    }

    @Override
    public long getMaxDictionarySize()
    {
      return maxDictionarySize;
    }

    @Override
    public Grouper.KeySerde<RowBasedKey> factorize()
    {
      return new RowBasedKeySerde(
          includeTimestamp,
          sortByDimsFirst,
          dimensions,
          maxDictionarySize,
          limitSpec,
          valueTypes,
          null
      );
    }

    @Override
    public Grouper.KeySerde<RowBasedKey> factorizeWithDictionary(List<String> dictionary)
    {
      return new RowBasedKeySerde(
          includeTimestamp,
          sortByDimsFirst,
          dimensions,
          maxDictionarySize,
          limitSpec,
          valueTypes,
          dictionary
      );
    }

    @Override
    public Comparator<Grouper.Entry<RowBasedKey>> objectComparator(boolean forceDefaultOrder)
    {
      if (limitSpec != null && !forceDefaultOrder) {
        return objectComparatorWithAggs();
      }

      if (includeTimestamp) {
        if (sortByDimsFirst) {
          return new Comparator<Grouper.Entry<RowBasedKey>>()
          {
            @Override
            public int compare(Grouper.Entry<RowBasedKey> entry1, Grouper.Entry<RowBasedKey> entry2)
            {
              final int cmp = compareDimsInRows(entry1.getKey(), entry2.getKey(), 1);
              if (cmp != 0) {
                return cmp;
              }

              return Longs.compare((long) entry1.getKey().getKey()[0], (long) entry2.getKey().getKey()[0]);
            }
          };
        } else {
          return new Comparator<Grouper.Entry<RowBasedKey>>()
          {
            @Override
            public int compare(Grouper.Entry<RowBasedKey> entry1, Grouper.Entry<RowBasedKey> entry2)
            {
              final int timeCompare = Longs.compare(
                  (long) entry1.getKey().getKey()[0],
                  (long) entry2.getKey().getKey()[0]
              );

              if (timeCompare != 0) {
                return timeCompare;
              }

              return compareDimsInRows(entry1.getKey(), entry2.getKey(), 1);
            }
          };
        }
      } else {
        return new Comparator<Grouper.Entry<RowBasedKey>>()
        {
          @Override
          public int compare(Grouper.Entry<RowBasedKey> entry1, Grouper.Entry<RowBasedKey> entry2)
          {
            return compareDimsInRows(entry1.getKey(), entry2.getKey(), 0);
          }
        };
      }
    }

    private Comparator<Grouper.Entry<RowBasedKey>> objectComparatorWithAggs()
    {
      // use the actual sort order from the limitspec if pushing down to merge partial results correctly
      final List<Boolean> needsReverses = Lists.newArrayList();
      final List<Boolean> aggFlags = Lists.newArrayList();
      final List<Boolean> isNumericField = Lists.newArrayList();
      final List<StringComparator> comparators = Lists.newArrayList();
      final List<Integer> fieldIndices = Lists.newArrayList();
      final Set<Integer> orderByIndices = new HashSet<>();

      for (OrderByColumnSpec orderSpec : limitSpec.getColumns()) {
        final boolean needsReverse = orderSpec.getDirection() != OrderByColumnSpec.Direction.ASCENDING;
        int dimIndex = OrderByColumnSpec.getDimIndexForOrderBy(orderSpec, dimensions);
        if (dimIndex >= 0) {
          fieldIndices.add(dimIndex);
          orderByIndices.add(dimIndex);
          needsReverses.add(needsReverse);
          aggFlags.add(false);
          final ValueType type = dimensions.get(dimIndex).getOutputType();
          isNumericField.add(ValueType.isNumeric(type));
          comparators.add(orderSpec.getDimensionComparator());
        } else {
          int aggIndex = OrderByColumnSpec.getAggIndexForOrderBy(orderSpec, Arrays.asList(aggregatorFactories));
          if (aggIndex >= 0) {
            fieldIndices.add(aggIndex);
            needsReverses.add(needsReverse);
            aggFlags.add(true);
            final String typeName = aggregatorFactories[aggIndex].getTypeName();
            isNumericField.add(ValueType.isNumeric(ValueType.fromString(typeName)));
            comparators.add(orderSpec.getDimensionComparator());
          }
        }
      }

      for (int i = 0; i < dimCount; i++) {
        if (!orderByIndices.contains(i)) {
          fieldIndices.add(i);
          aggFlags.add(false);
          needsReverses.add(false);
          final ValueType type = dimensions.get(i).getOutputType();
          isNumericField.add(ValueType.isNumeric(type));
          comparators.add(StringComparators.LEXICOGRAPHIC);
        }
      }

      if (includeTimestamp) {
        if (sortByDimsFirst) {
          return new Comparator<Grouper.Entry<RowBasedKey>>()
          {
            @Override
            public int compare(Grouper.Entry<RowBasedKey> entry1, Grouper.Entry<RowBasedKey> entry2)
            {
              final int cmp = compareDimsInRowsWithAggs(
                  entry1,
                  entry2,
                  1,
                  needsReverses,
                  aggFlags,
                  fieldIndices,
                  isNumericField,
                  comparators
              );
              if (cmp != 0) {
                return cmp;
              }

              return Longs.compare((long) entry1.getKey().getKey()[0], (long) entry2.getKey().getKey()[0]);
            }
          };
        } else {
          return new Comparator<Grouper.Entry<RowBasedKey>>()
          {
            @Override
            public int compare(Grouper.Entry<RowBasedKey> entry1, Grouper.Entry<RowBasedKey> entry2)
            {
              final int timeCompare = Longs.compare((long) entry1.getKey().getKey()[0], (long) entry2.getKey().getKey()[0]);

              if (timeCompare != 0) {
                return timeCompare;
              }

              return compareDimsInRowsWithAggs(
                  entry1,
                  entry2,
                  1,
                  needsReverses,
                  aggFlags,
                  fieldIndices,
                  isNumericField,
                  comparators
              );
            }
          };
        }
      } else {
        return new Comparator<Grouper.Entry<RowBasedKey>>()
        {
          @Override
          public int compare(Grouper.Entry<RowBasedKey> entry1, Grouper.Entry<RowBasedKey> entry2)
          {
            return compareDimsInRowsWithAggs(
                entry1,
                entry2,
                0,
                needsReverses,
                aggFlags,
                fieldIndices,
                isNumericField,
                comparators
            );
          }
        };
      }
    }

    private static int compareDimsInRows(RowBasedKey key1, RowBasedKey key2, int dimStart)
    {
      for (int i = dimStart; i < key1.getKey().length; i++) {
        final int cmp = ((Comparable) key1.getKey()[i]).compareTo(key2.getKey()[i]);
        if (cmp != 0) {
          return cmp;
        }
      }

      return 0;
    }

    private static int compareDimsInRowsWithAggs(
        Grouper.Entry<RowBasedKey> entry1,
        Grouper.Entry<RowBasedKey> entry2,
        int dimStart,
        final List<Boolean> needsReverses,
        final List<Boolean> aggFlags,
        final List<Integer> fieldIndices,
        final List<Boolean> isNumericField,
        final List<StringComparator> comparators
    )
    {
      for (int i = 0; i < fieldIndices.size(); i++) {
        final int fieldIndex = fieldIndices.get(i);
        final boolean needsReverse = needsReverses.get(i);
        final int cmp;
        final Comparable lhs;
        final Comparable rhs;

        if (aggFlags.get(i)) {
          if (needsReverse) {
            lhs = (Comparable) entry2.getValues()[fieldIndex];
            rhs = (Comparable) entry1.getValues()[fieldIndex];
          } else {
            lhs = (Comparable) entry1.getValues()[fieldIndex];
            rhs = (Comparable) entry2.getValues()[fieldIndex];
          }
        } else {
          if (needsReverse) {
            lhs = (Comparable) entry2.getKey().getKey()[fieldIndex + dimStart];
            rhs = (Comparable) entry1.getKey().getKey()[fieldIndex + dimStart];
          } else {
            lhs = (Comparable) entry1.getKey().getKey()[fieldIndex + dimStart];
            rhs = (Comparable) entry2.getKey().getKey()[fieldIndex + dimStart];
          }
        }

        final StringComparator comparator = comparators.get(i);

        if (isNumericField.get(i) && comparator.equals(StringComparators.NUMERIC)) {
          // use natural comparison
          cmp = lhs.compareTo(rhs);
        } else {
          cmp = comparator.compare(lhs.toString(), rhs.toString());
        }

        if (cmp != 0) {
          return cmp;
        }
      }

      return 0;
    }
  }

  static long estimateStringKeySize(String key)
  {
    return (long) key.length() * Chars.BYTES + ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY;
  }

  private static class RowBasedKeySerde implements Grouper.KeySerde<RowBasedGrouperHelper.RowBasedKey>
  {
    private static final int UNKNOWN_DICTIONARY_ID = -1;

    private final boolean includeTimestamp;
    private final boolean sortByDimsFirst;
    private final List<DimensionSpec> dimensions;
    private final int dimCount;
    private final int keySize;
    private final ByteBuffer keyBuffer;
    private final RowBasedKeySerdeHelper[] serdeHelpers;
    private final BufferComparator[] serdeHelperComparators;
    private final DefaultLimitSpec limitSpec;
    private final List<ValueType> valueTypes;

    private final boolean enableRuntimeDictionaryGeneration;

    private final List<String> dictionary;
    private final Object2IntMap<String> reverseDictionary;

    // Size limiting for the dictionary, in (roughly estimated) bytes.
    private final long maxDictionarySize;

    private long currentEstimatedSize = 0;

    // dictionary id -> rank of the sorted dictionary
    // This is initialized in the constructor and bufferComparator() with static dictionary and dynamic dictionary,
    // respectively.
    private int[] rankOfDictionaryIds = null;

    RowBasedKeySerde(
        final boolean includeTimestamp,
        final boolean sortByDimsFirst,
        final List<DimensionSpec> dimensions,
        final long maxDictionarySize,
        final DefaultLimitSpec limitSpec,
        final List<ValueType> valueTypes,
        @Nullable final List<String> dictionary
    )
    {
      this.includeTimestamp = includeTimestamp;
      this.sortByDimsFirst = sortByDimsFirst;
      this.dimensions = dimensions;
      this.dimCount = dimensions.size();
      this.valueTypes = valueTypes;
      this.limitSpec = limitSpec;
      this.enableRuntimeDictionaryGeneration = dictionary == null;
      this.dictionary = enableRuntimeDictionaryGeneration ? new ArrayList<>() : dictionary;
      this.reverseDictionary = enableRuntimeDictionaryGeneration ?
                               new Object2IntOpenHashMap<>() :
                               new Object2IntOpenHashMap<>(dictionary.size());
      this.reverseDictionary.defaultReturnValue(UNKNOWN_DICTIONARY_ID);
      this.maxDictionarySize = maxDictionarySize;
      this.serdeHelpers = makeSerdeHelpers(limitSpec != null, enableRuntimeDictionaryGeneration);
      this.serdeHelperComparators = new BufferComparator[serdeHelpers.length];
      Arrays.setAll(serdeHelperComparators, i -> serdeHelpers[i].getBufferComparator());
      this.keySize = (includeTimestamp ? Longs.BYTES : 0) + getTotalKeySize();
      this.keyBuffer = ByteBuffer.allocate(keySize);

      if (!enableRuntimeDictionaryGeneration) {
        final long initialDictionarySize = dictionary.stream()
                                                     .mapToLong(RowBasedGrouperHelper::estimateStringKeySize)
                                                     .sum();
        Preconditions.checkState(
            maxDictionarySize >= initialDictionarySize,
            "Dictionary size[%s] exceeds threshold[%s]",
            initialDictionarySize,
            maxDictionarySize
        );

        for (int i = 0; i < dictionary.size(); i++) {
          reverseDictionary.put(dictionary.get(i), i);
        }

        initializeRankOfDictionaryIds();
      }
    }

    private void initializeRankOfDictionaryIds()
    {
      final int dictionarySize = dictionary.size();
      rankOfDictionaryIds = IntStream.range(0, dictionarySize).toArray();
      IntArrays.quickSort(
          rankOfDictionaryIds,
          (i1, i2) -> dictionary.get(i1).compareTo(dictionary.get(i2))
      );

      IntArrayUtils.inverse(rankOfDictionaryIds);
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
    public List<String> getDictionary()
    {
      return dictionary;
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
        if (!serdeHelpers[i - dimStart].putToKeyBuffer(key, i)) {
          return null;
        }
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
        // Writes value from buffer to key[i]
        serdeHelpers[i - dimStart].getFromByteBuffer(buffer, dimsPosition, i, key);
      }

      return new RowBasedKey(key);
    }

    @Override
    public Grouper.BufferComparator bufferComparator()
    {
      if (rankOfDictionaryIds == null) {
        initializeRankOfDictionaryIds();
      }

      if (includeTimestamp) {
        if (sortByDimsFirst) {
          return new Grouper.BufferComparator()
          {
            @Override
            public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
            {
              final int cmp = compareDimsInBuffersForNullFudgeTimestamp(
                  serdeHelperComparators,
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
          return new Grouper.BufferComparator()
          {
            @Override
            public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
            {
              final int timeCompare = Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));

              if (timeCompare != 0) {
                return timeCompare;
              }

              return compareDimsInBuffersForNullFudgeTimestamp(
                  serdeHelperComparators,
                  lhsBuffer,
                  rhsBuffer,
                  lhsPosition,
                  rhsPosition
              );
            }
          };
        }
      } else {
        return new Grouper.BufferComparator()
        {
          @Override
          public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
          {
            for (int i = 0; i < dimCount; i++) {
              final int cmp = serdeHelperComparators[i].compare(
                  lhsBuffer,
                  rhsBuffer,
                  lhsPosition,
                  rhsPosition
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
    public Grouper.BufferComparator bufferComparatorWithAggregators(
        AggregatorFactory[] aggregatorFactories,
        int[] aggregatorOffsets
    )
    {
      final List<RowBasedKeySerdeHelper> adjustedSerdeHelpers;
      final List<Boolean> needsReverses = Lists.newArrayList();
      List<RowBasedKeySerdeHelper> orderByHelpers = new ArrayList<>();
      List<RowBasedKeySerdeHelper> otherDimHelpers = new ArrayList<>();
      Set<Integer> orderByIndices = new HashSet<>();

      int aggCount = 0;
      boolean needsReverse;
      for (OrderByColumnSpec orderSpec : limitSpec.getColumns()) {
        needsReverse = orderSpec.getDirection() != OrderByColumnSpec.Direction.ASCENDING;
        int dimIndex = OrderByColumnSpec.getDimIndexForOrderBy(orderSpec, dimensions);
        if (dimIndex >= 0) {
          RowBasedKeySerdeHelper serdeHelper = serdeHelpers[dimIndex];
          orderByHelpers.add(serdeHelper);
          orderByIndices.add(dimIndex);
          needsReverses.add(needsReverse);
        } else {
          int aggIndex = OrderByColumnSpec.getAggIndexForOrderBy(orderSpec, Arrays.asList(aggregatorFactories));
          if (aggIndex >= 0) {
            final RowBasedKeySerdeHelper serdeHelper;
            final StringComparator stringComparator = orderSpec.getDimensionComparator();
            final String typeName = aggregatorFactories[aggIndex].getTypeName();
            final int aggOffset = aggregatorOffsets[aggIndex] - Ints.BYTES;

            aggCount++;

            final ValueType valueType = ValueType.fromString(typeName);
            if (!ValueType.isNumeric(valueType)) {
              throw new IAE("Cannot order by a non-numeric aggregator[%s]", orderSpec);
            }

            serdeHelper = makeNumericSerdeHelper(valueType, aggOffset, true, stringComparator);

            orderByHelpers.add(serdeHelper);
            needsReverses.add(needsReverse);
          }
        }
      }

      for (int i = 0; i < dimCount; i++) {
        if (!orderByIndices.contains(i)) {
          otherDimHelpers.add(serdeHelpers[i]);
          needsReverses.add(false); // default to Ascending order if dim is not in an orderby spec
        }
      }

      adjustedSerdeHelpers = orderByHelpers;
      adjustedSerdeHelpers.addAll(otherDimHelpers);

      final BufferComparator[] adjustedSerdeHelperComparators = new BufferComparator[adjustedSerdeHelpers.size()];
      Arrays.setAll(adjustedSerdeHelperComparators, i -> adjustedSerdeHelpers.get(i).getBufferComparator());

      final int fieldCount = dimCount + aggCount;

      if (includeTimestamp) {
        if (sortByDimsFirst) {
          return new Grouper.BufferComparator()
          {
            @Override
            public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
            {
              final int cmp = compareDimsInBuffersForNullFudgeTimestampForPushDown(
                  adjustedSerdeHelperComparators,
                  needsReverses,
                  fieldCount,
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
          return new Grouper.BufferComparator()
          {
            @Override
            public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
            {
              final int timeCompare = Longs.compare(lhsBuffer.getLong(lhsPosition), rhsBuffer.getLong(rhsPosition));

              if (timeCompare != 0) {
                return timeCompare;
              }

              int cmp = compareDimsInBuffersForNullFudgeTimestampForPushDown(
                  adjustedSerdeHelperComparators,
                  needsReverses,
                  fieldCount,
                  lhsBuffer,
                  rhsBuffer,
                  lhsPosition,
                  rhsPosition
              );

              return cmp;
            }
          };
        }
      } else {
        return new Grouper.BufferComparator()
        {
          @Override
          public int compare(ByteBuffer lhsBuffer, ByteBuffer rhsBuffer, int lhsPosition, int rhsPosition)
          {
            for (int i = 0; i < fieldCount; i++) {
              final int cmp;
              if (needsReverses.get(i)) {
                cmp = adjustedSerdeHelperComparators[i].compare(
                    rhsBuffer,
                    lhsBuffer,
                    rhsPosition,
                    lhsPosition
                );
              } else {
                cmp = adjustedSerdeHelperComparators[i].compare(
                    lhsBuffer,
                    rhsBuffer,
                    lhsPosition,
                    rhsPosition
                );
              }

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
      if (enableRuntimeDictionaryGeneration) {
        dictionary.clear();
        reverseDictionary.clear();
        rankOfDictionaryIds = null;
        currentEstimatedSize = 0;
      }
    }

    private int getTotalKeySize()
    {
      int size = 0;
      for (RowBasedKeySerdeHelper helper : serdeHelpers) {
        size += helper.getKeyBufferValueSize();
      }
      return size;
    }

    private RowBasedKeySerdeHelper[] makeSerdeHelpers(
        boolean pushLimitDown,
        boolean enableRuntimeDictionaryGeneration
    )
    {
      final List<RowBasedKeySerdeHelper> helpers = new ArrayList<>();
      int keyBufferPosition = 0;

      for (int i = 0; i < dimCount; i++) {
        final StringComparator stringComparator;
        if (limitSpec != null) {
          final String dimName = dimensions.get(i).getOutputName();
          stringComparator = DefaultLimitSpec.getComparatorForDimName(limitSpec, dimName);
        } else {
          stringComparator = null;
        }

        RowBasedKeySerdeHelper helper = makeSerdeHelper(
            valueTypes.get(i),
            keyBufferPosition,
            pushLimitDown,
            stringComparator,
            enableRuntimeDictionaryGeneration
        );

        keyBufferPosition += helper.getKeyBufferValueSize();
        helpers.add(helper);
      }

      return helpers.toArray(new RowBasedKeySerdeHelper[helpers.size()]);
    }

    private RowBasedKeySerdeHelper makeSerdeHelper(
        ValueType valueType,
        int keyBufferPosition,
        boolean pushLimitDown,
        @Nullable StringComparator stringComparator,
        boolean enableRuntimeDictionaryGeneration
    )
    {
      switch (valueType) {
        case STRING:
          if (enableRuntimeDictionaryGeneration) {
            return new DynamicDictionaryStringRowBasedKeySerdeHelper(
                keyBufferPosition,
                pushLimitDown,
                stringComparator
            );
          } else {
            return new StaticDictionaryStringRowBasedKeySerdeHelper(
                keyBufferPosition,
                pushLimitDown,
                stringComparator
            );
          }
        case LONG:
        case FLOAT:
        case DOUBLE:
          return makeNumericSerdeHelper(valueType, keyBufferPosition, pushLimitDown, stringComparator);
        default:
          throw new IAE("invalid type: %s", valueType);
      }
    }

    private RowBasedKeySerdeHelper makeNumericSerdeHelper(
        ValueType valueType,
        int keyBufferPosition,
        boolean pushLimitDown,
        @Nullable StringComparator stringComparator
    )
    {
      switch (valueType) {
        case LONG:
          return new LongRowBasedKeySerdeHelper(keyBufferPosition, pushLimitDown, stringComparator);
        case FLOAT:
          return new FloatRowBasedKeySerdeHelper(keyBufferPosition, pushLimitDown, stringComparator);
        case DOUBLE:
          return new DoubleRowBasedKeySerdeHelper(keyBufferPosition, pushLimitDown, stringComparator);
        default:
          throw new IAE("invalid type: %s", valueType);
      }
    }

    private static boolean isPrimitiveComparable(boolean pushLimitDown, @Nullable StringComparator stringComparator)
    {
      return !pushLimitDown || stringComparator == null || stringComparator.equals(StringComparators.NUMERIC);
    }

    private abstract class AbstractStringRowBasedKeySerdeHelper implements RowBasedKeySerdeHelper
    {
      final int keyBufferPosition;

      final BufferComparator bufferComparator;

      AbstractStringRowBasedKeySerdeHelper(
          int keyBufferPosition,
          boolean pushLimitDown,
          @Nullable StringComparator stringComparator
      )
      {
        this.keyBufferPosition = keyBufferPosition;
        if (!pushLimitDown) {
          bufferComparator = (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Ints.compare(
              rankOfDictionaryIds[lhsBuffer.getInt(lhsPosition + keyBufferPosition)],
              rankOfDictionaryIds[rhsBuffer.getInt(rhsPosition + keyBufferPosition)]
          );
        } else {
          final StringComparator realComparator = stringComparator == null ?
                                                  StringComparators.LEXICOGRAPHIC :
                                                  stringComparator;
          bufferComparator = (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
            String lhsStr = dictionary.get(lhsBuffer.getInt(lhsPosition + keyBufferPosition));
            String rhsStr = dictionary.get(rhsBuffer.getInt(rhsPosition + keyBufferPosition));
            return realComparator.compare(lhsStr, rhsStr);
          };
        }
      }

      @Override
      public int getKeyBufferValueSize()
      {
        return Ints.BYTES;
      }

      @Override
      public void getFromByteBuffer(ByteBuffer buffer, int initialOffset, int dimValIdx, Comparable[] dimValues)
      {
        dimValues[dimValIdx] = dictionary.get(buffer.getInt(initialOffset + keyBufferPosition));
      }

      @Override
      public BufferComparator getBufferComparator()
      {
        return bufferComparator;
      }
    }

    private class DynamicDictionaryStringRowBasedKeySerdeHelper extends AbstractStringRowBasedKeySerdeHelper
    {
      DynamicDictionaryStringRowBasedKeySerdeHelper(
          int keyBufferPosition,
          boolean pushLimitDown,
          @Nullable StringComparator stringComparator
      )
      {
        super(keyBufferPosition, pushLimitDown, stringComparator);
      }

      @Override
      public boolean putToKeyBuffer(RowBasedKey key, int idx)
      {
        final int id = addToDictionary((String) key.getKey()[idx]);
        if (id < 0) {
          return false;
        }
        keyBuffer.putInt(id);
        return true;
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
        int idx = reverseDictionary.getInt(s);
        if (idx == UNKNOWN_DICTIONARY_ID) {
          final long additionalEstimatedSize = estimateStringKeySize(s);
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

    private class StaticDictionaryStringRowBasedKeySerdeHelper extends AbstractStringRowBasedKeySerdeHelper
    {
      StaticDictionaryStringRowBasedKeySerdeHelper(
          int keyBufferPosition,
          boolean pushLimitDown,
          @Nullable StringComparator stringComparator
      )
      {
        super(keyBufferPosition, pushLimitDown, stringComparator);
      }

      @Override
      public boolean putToKeyBuffer(RowBasedKey key, int idx)
      {
        final String stringKey = (String) key.getKey()[idx];

        final int dictIndex = reverseDictionary.getInt(stringKey);
        if (dictIndex == UNKNOWN_DICTIONARY_ID) {
          throw new ISE("Cannot find key[%s] from dictionary", stringKey);
        }
        keyBuffer.putInt(dictIndex);
        return true;
      }
    }

    private class LongRowBasedKeySerdeHelper implements RowBasedKeySerdeHelper
    {
      final int keyBufferPosition;
      final BufferComparator bufferComparator;

      LongRowBasedKeySerdeHelper(
          int keyBufferPosition,
          boolean pushLimitDown,
          @Nullable StringComparator stringComparator
      )
      {
        this.keyBufferPosition = keyBufferPosition;
        if (isPrimitiveComparable(pushLimitDown, stringComparator)) {
          bufferComparator = (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Longs.compare(
              lhsBuffer.getLong(lhsPosition + keyBufferPosition),
              rhsBuffer.getLong(rhsPosition + keyBufferPosition)
          );
        } else {
          bufferComparator = (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
            long lhs = lhsBuffer.getLong(lhsPosition + keyBufferPosition);
            long rhs = rhsBuffer.getLong(rhsPosition + keyBufferPosition);

            return stringComparator.compare(String.valueOf(lhs), String.valueOf(rhs));
          };
        }
      }

      @Override
      public int getKeyBufferValueSize()
      {
        return Longs.BYTES;
      }

      @Override
      public boolean putToKeyBuffer(RowBasedKey key, int idx)
      {
        keyBuffer.putLong((Long) key.getKey()[idx]);
        return true;
      }

      @Override
      public void getFromByteBuffer(ByteBuffer buffer, int initialOffset, int dimValIdx, Comparable[] dimValues)
      {
        dimValues[dimValIdx] = buffer.getLong(initialOffset + keyBufferPosition);
      }

      @Override
      public BufferComparator getBufferComparator()
      {
        return bufferComparator;
      }
    }

    private class FloatRowBasedKeySerdeHelper implements RowBasedKeySerdeHelper
    {
      final int keyBufferPosition;
      final BufferComparator bufferComparator;

      FloatRowBasedKeySerdeHelper(
          int keyBufferPosition,
          boolean pushLimitDown,
          @Nullable StringComparator stringComparator)
      {
        this.keyBufferPosition = keyBufferPosition;
        if (isPrimitiveComparable(pushLimitDown, stringComparator)) {
          bufferComparator = (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Float.compare(
              lhsBuffer.getFloat(lhsPosition + keyBufferPosition),
              rhsBuffer.getFloat(rhsPosition + keyBufferPosition)
          );
        } else {
          bufferComparator = (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
            float lhs = lhsBuffer.getFloat(lhsPosition + keyBufferPosition);
            float rhs = rhsBuffer.getFloat(rhsPosition + keyBufferPosition);
            return stringComparator.compare(String.valueOf(lhs), String.valueOf(rhs));
          };
        }
      }

      @Override
      public int getKeyBufferValueSize()
      {
        return Floats.BYTES;
      }

      @Override
      public boolean putToKeyBuffer(RowBasedKey key, int idx)
      {
        keyBuffer.putFloat((Float) key.getKey()[idx]);
        return true;
      }

      @Override
      public void getFromByteBuffer(ByteBuffer buffer, int initialOffset, int dimValIdx, Comparable[] dimValues)
      {
        dimValues[dimValIdx] = buffer.getFloat(initialOffset + keyBufferPosition);
      }

      @Override
      public BufferComparator getBufferComparator()
      {
        return bufferComparator;
      }
    }

    private class DoubleRowBasedKeySerdeHelper implements RowBasedKeySerdeHelper
    {
      final int keyBufferPosition;
      final BufferComparator bufferComparator;

      DoubleRowBasedKeySerdeHelper(
          int keyBufferPosition,
          boolean pushLimitDown,
          @Nullable StringComparator stringComparator
      )
      {
        this.keyBufferPosition = keyBufferPosition;
        if (isPrimitiveComparable(pushLimitDown, stringComparator)) {
          bufferComparator = (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Double.compare(
              lhsBuffer.getDouble(lhsPosition + keyBufferPosition),
              rhsBuffer.getDouble(rhsPosition + keyBufferPosition)
          );
        } else {
          bufferComparator = (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
            double lhs = lhsBuffer.getDouble(lhsPosition + keyBufferPosition);
            double rhs = rhsBuffer.getDouble(rhsPosition + keyBufferPosition);
            return stringComparator.compare(String.valueOf(lhs), String.valueOf(rhs));
          };
        }
      }

      @Override
      public int getKeyBufferValueSize()
      {
        return Doubles.BYTES;
      }

      @Override
      public boolean putToKeyBuffer(RowBasedKey key, int idx)
      {
        keyBuffer.putDouble((Double) key.getKey()[idx]);
        return true;
      }

      @Override
      public void getFromByteBuffer(ByteBuffer buffer, int initialOffset, int dimValIdx, Comparable[] dimValues)
      {
        dimValues[dimValIdx] = buffer.getDouble(initialOffset + keyBufferPosition);
      }

      @Override
      public BufferComparator getBufferComparator()
      {
        return bufferComparator;
      }
    }
  }

  private static int compareDimsInBuffersForNullFudgeTimestamp(
      BufferComparator[] serdeHelperComparators,
      ByteBuffer lhsBuffer,
      ByteBuffer rhsBuffer,
      int lhsPosition,
      int rhsPosition
  )
  {
    for (BufferComparator comparator : serdeHelperComparators) {
      final int cmp = comparator.compare(
          lhsBuffer,
          rhsBuffer,
          lhsPosition + Longs.BYTES,
          rhsPosition + Longs.BYTES
      );
      if (cmp != 0) {
        return cmp;
      }
    }

    return 0;
  }

  private static int compareDimsInBuffersForNullFudgeTimestampForPushDown(
      BufferComparator[] serdeHelperComparators,
      List<Boolean> needsReverses,
      int dimCount,
      ByteBuffer lhsBuffer,
      ByteBuffer rhsBuffer,
      int lhsPosition,
      int rhsPosition
  )
  {
    for (int i = 0; i < dimCount; i++) {
      final int cmp;
      if (needsReverses.get(i)) {
        cmp = serdeHelperComparators[i].compare(
            rhsBuffer,
            lhsBuffer,
            rhsPosition + Longs.BYTES,
            lhsPosition + Longs.BYTES
        );
      } else {
        cmp = serdeHelperComparators[i].compare(
            lhsBuffer,
            rhsBuffer,
            lhsPosition + Longs.BYTES,
            rhsPosition + Longs.BYTES
        );
      }
      if (cmp != 0) {
        return cmp;
      }
    }

    return 0;
  }
}
