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

package org.apache.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListeningExecutorService;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.common.utils.IntArrayUtils;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.ColumnSelectorStrategy;
import org.apache.druid.query.dimension.ColumnSelectorStrategyFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryHelper;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.RowBasedColumnSelectorFactory;
import org.apache.druid.query.groupby.epinephelinae.Grouper.BufferComparator;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;

/**
 * This class contains shared code between {@link GroupByMergingQueryRunnerV2} and {@link GroupByRowProcessor}.
 */
public class RowBasedGrouperHelper
{
  // Entry in dictionary, node pointer in reverseDictionary, hash + k/v/next pointer in reverseDictionary nodes
  private static final int ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY = Long.BYTES * 5 + Integer.BYTES;

  private static final int SINGLE_THREAD_CONCURRENCY_HINT = -1;
  private static final int UNKNOWN_THREAD_PRIORITY = -1;
  private static final long UNKNOWN_TIMEOUT = -1L;

  private RowBasedGrouperHelper()
  {
    // No instantiation.
  }

  /**
   * Create a single-threaded grouper and accumulator.
   */
  public static Pair<Grouper<RowBasedKey>, Accumulator<AggregateResult, ResultRow>> createGrouperAccumulatorPair(
      final GroupByQuery query,
      @Nullable final GroupByQuery subquery,
      final GroupByQueryConfig config,
      final Supplier<ByteBuffer> bufferSupplier,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final int mergeBufferSize
  )
  {
    return createGrouperAccumulatorPair(
        query,
        subquery,
        config,
        bufferSupplier,
        null,
        SINGLE_THREAD_CONCURRENCY_HINT,
        temporaryStorage,
        spillMapper,
        null,
        UNKNOWN_THREAD_PRIORITY,
        false,
        UNKNOWN_TIMEOUT,
        mergeBufferSize
    );
  }

  /**
   * Create a {@link Grouper} that groups according to the dimensions and aggregators in "query", along with
   * an {@link Accumulator} that accepts ResultRows and forwards them to the grouper.
   *
   * The pair will operate in one of two modes:
   *
   * 1) Combining mode (used if "subquery" is null). In this mode, filters from the "query" are ignored, and
   * its aggregators are converted into combining form. The input ResultRows are assumed to be partially-grouped
   * results originating from the provided "query".
   *
   * 2) Subquery mode (used if "subquery" is nonnull). In this mode, filters from the "query" (both intervals
   * and dim filters) are respected, and its aggregators are used in standard (not combining) form. The input
   * ResultRows are assumed to be results originating from the provided "subquery".
   *
   * @param query               query that we are grouping for
   * @param subquery            optional subquery that we are receiving results from (see combining vs. subquery
   *                            mode above)
   * @param config              groupBy query config
   * @param bufferSupplier      supplier of merge buffers
   * @param combineBufferHolder holder of combine buffers. Unused if concurrencyHint = -1, and may be null in that case
   * @param concurrencyHint     -1 for single-threaded Grouper, >=1 for concurrent Grouper
   * @param temporaryStorage    temporary storage used for spilling from the Grouper
   * @param spillMapper         object mapper used for spilling from the Grouper
   * @param grouperSorter       executor service used for parallel combining. Unused if concurrencyHint = -1, and may
   *                            be null in that case
   * @param priority            query priority
   * @param hasQueryTimeout     whether or not this query has a timeout
   * @param queryTimeoutAt      when this query times out, in milliseconds since the epoch
   * @param mergeBufferSize     size of the merge buffers from "bufferSupplier"
   */
  public static Pair<Grouper<RowBasedKey>, Accumulator<AggregateResult, ResultRow>> createGrouperAccumulatorPair(
      final GroupByQuery query,
      @Nullable final GroupByQuery subquery,
      final GroupByQueryConfig config,
      final Supplier<ByteBuffer> bufferSupplier,
      @Nullable final ReferenceCountingResourceHolder<ByteBuffer> combineBufferHolder,
      final int concurrencyHint,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      @Nullable final ListeningExecutorService grouperSorter,
      final int priority,
      final boolean hasQueryTimeout,
      final long queryTimeoutAt,
      final int mergeBufferSize
  )
  {
    // concurrencyHint >= 1 for concurrent groupers, -1 for single-threaded
    Preconditions.checkArgument(concurrencyHint >= 1 || concurrencyHint == -1, "invalid concurrencyHint");

    if (concurrencyHint >= 1) {
      Preconditions.checkNotNull(grouperSorter, "grouperSorter executor must be provided");
    }

    // See method-level javadoc; we go into combining mode if there is no subquery.
    final boolean combining = subquery == null;

    final List<ValueType> valueTypes = DimensionHandlerUtils.getValueTypesFromDimensionSpecs(query.getDimensions());

    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);
    final boolean includeTimestamp = query.getResultRowHasTimestamp();

    final ThreadLocal<ResultRow> columnSelectorRow = new ThreadLocal<>();

    ColumnSelectorFactory columnSelectorFactory = createResultRowBasedColumnSelectorFactory(
        combining ? query : subquery,
        columnSelectorRow::get
    );

    // Apply virtual columns if we are in subquery (non-combining) mode.
    if (!combining) {
      columnSelectorFactory = query.getVirtualColumns().wrap(columnSelectorFactory);
    }

    final boolean willApplyLimitPushDown = query.isApplyLimitPushDown();
    final DefaultLimitSpec limitSpec = willApplyLimitPushDown ? (DefaultLimitSpec) query.getLimitSpec() : null;
    boolean sortHasNonGroupingFields = false;
    if (willApplyLimitPushDown) {
      sortHasNonGroupingFields = DefaultLimitSpec.sortingOrderHasNonGroupingFields(
          limitSpec,
          query.getDimensions()
      );
    }

    final AggregatorFactory[] aggregatorFactories;

    if (combining) {
      aggregatorFactories = query.getAggregatorSpecs()
                                 .stream()
                                 .map(AggregatorFactory::getCombiningFactory)
                                 .toArray(AggregatorFactory[]::new);
    } else {
      aggregatorFactories = query.getAggregatorSpecs().toArray(new AggregatorFactory[0]);
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
          combineBufferHolder,
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
        combining,
        includeTimestamp,
        columnSelectorFactory,
        valueTypes
    );

    final Predicate<ResultRow> rowPredicate;

    if (combining) {
      // Filters are not applied in combining mode.
      rowPredicate = row -> true;
    } else {
      rowPredicate = getResultRowPredicate(query, subquery);
    }

    final Accumulator<AggregateResult, ResultRow> accumulator = (priorResult, row) -> {
      BaseQuery.checkInterrupted();

      if (priorResult != null && !priorResult.isOk()) {
        // Pass-through error returns without doing more work.
        return priorResult;
      }

      if (!grouper.isInitialized()) {
        grouper.init();
      }

      if (!rowPredicate.test(row)) {
        return AggregateResult.ok();
      }

      columnSelectorRow.set(row);

      final Comparable[] key = new Comparable[keySize];
      valueExtractFn.apply(row, key);

      final AggregateResult aggregateResult = grouper.aggregate(new RowBasedKey(key));
      columnSelectorRow.set(null);

      return aggregateResult;
    };

    return new Pair<>(grouper, accumulator);
  }

  /**
   * Creates a {@link ColumnSelectorFactory} that can read rows which originate as results of the provided "query".
   *
   * @param query    a groupBy query
   * @param supplier supplier of result rows from the query
   */
  public static ColumnSelectorFactory createResultRowBasedColumnSelectorFactory(
      final GroupByQuery query,
      final Supplier<ResultRow> supplier
  )
  {
    final RowBasedColumnSelectorFactory.RowAdapter<ResultRow> adapter =
        new RowBasedColumnSelectorFactory.RowAdapter<ResultRow>()
        {
          @Override
          public ToLongFunction<ResultRow> timestampFunction()
          {
            if (query.getResultRowHasTimestamp()) {
              return row -> row.getLong(0);
            } else {
              final long timestamp = query.getUniversalTimestamp().getMillis();
              return row -> timestamp;
            }
          }

          @Override
          public Function<ResultRow, Object> rawFunction(final String columnName)
          {
            final int columnIndex = query.getResultRowPositionLookup().getInt(columnName);
            if (columnIndex < 0) {
              return row -> null;
            } else {
              return row -> row.get(columnIndex);
            }
          }
        };

    return RowBasedColumnSelectorFactory.create(adapter, supplier::get, GroupByQueryHelper.rowSignatureFor(query));
  }

  /**
   * Returns a predicate that filters result rows from a particular "subquery" based on the intervals and dim filters
   * from "query".
   *
   * @param query    outer query
   * @param subquery inner query
   */
  private static Predicate<ResultRow> getResultRowPredicate(final GroupByQuery query, final GroupByQuery subquery)
  {
    final List<Interval> queryIntervals = query.getIntervals();
    final Filter filter = Filters.convertToCNFFromQueryContext(
        query,
        Filters.toFilter(query.getDimFilter())
    );

    final SettableSupplier<ResultRow> rowSupplier = new SettableSupplier<>();
    final ColumnSelectorFactory columnSelectorFactory =
        RowBasedGrouperHelper.createResultRowBasedColumnSelectorFactory(subquery, rowSupplier);

    final ValueMatcher filterMatcher = filter == null
                                       ? BooleanValueMatcher.of(true)
                                       : filter.makeMatcher(columnSelectorFactory);

    if (subquery.getUniversalTimestamp() != null
        && queryIntervals.stream().noneMatch(itvl -> itvl.contains(subquery.getUniversalTimestamp()))) {
      // There's a universal timestamp, and it doesn't match our query intervals, so no row should match.
      // By the way, if there's a universal timestamp that _does_ match the query intervals, we do nothing special here.
      return row -> false;
    }

    return row -> {
      if (subquery.getResultRowHasTimestamp()) {
        boolean inInterval = false;
        for (Interval queryInterval : queryIntervals) {
          if (queryInterval.contains(row.getLong(0))) {
            inInterval = true;
            break;
          }
        }
        if (!inInterval) {
          return false;
        }
      }
      rowSupplier.set(row);
      return filterMatcher.matches();
    };
  }

  private interface TimestampExtractFunction
  {
    long apply(ResultRow row);
  }

  private static TimestampExtractFunction makeTimestampExtractFunction(
      final GroupByQuery query,
      final boolean combining
  )
  {
    if (query.getResultRowHasTimestamp()) {
      if (combining) {
        return row -> row.getLong(0);
      } else {
        if (query.getGranularity() instanceof AllGranularity) {
          return row -> query.getIntervals().get(0).getStartMillis();
        } else {
          return row -> query.getGranularity().bucketStart(DateTimes.utc(row.getLong(0))).getMillis();
        }
      }
    } else {
      final long timestamp = query.getUniversalTimestamp().getMillis();
      return row -> timestamp;
    }
  }

  private interface ValueExtractFunction
  {
    Comparable[] apply(ResultRow row, Comparable[] key);
  }

  private static ValueExtractFunction makeValueExtractFunction(
      final GroupByQuery query,
      final boolean combining,
      final boolean includeTimestamp,
      final ColumnSelectorFactory columnSelectorFactory,
      final List<ValueType> valueTypes
  )
  {
    final TimestampExtractFunction timestampExtractFn = includeTimestamp ?
                                                        makeTimestampExtractFunction(query, combining) :
                                                        null;

    final Function<Comparable, Comparable>[] valueConvertFns = makeValueConvertFunctions(valueTypes);

    if (!combining) {
      final Supplier<Comparable>[] inputRawSuppliers = getValueSuppliersForDimensions(
          columnSelectorFactory,
          query.getDimensions()
      );

      if (includeTimestamp) {
        return (row, key) -> {
          key[0] = timestampExtractFn.apply(row);
          for (int i = 1; i < key.length; i++) {
            final Comparable val = inputRawSuppliers[i - 1].get();
            key[i] = valueConvertFns[i - 1].apply(val);
          }
          return key;
        };
      } else {
        return (row, key) -> {
          for (int i = 0; i < key.length; i++) {
            final Comparable val = inputRawSuppliers[i].get();
            key[i] = valueConvertFns[i].apply(val);
          }
          return key;
        };
      }
    } else {
      final int dimensionStartPosition = query.getResultRowDimensionStart();

      if (includeTimestamp) {
        return (row, key) -> {
          key[0] = timestampExtractFn.apply(row);
          for (int i = 1; i < key.length; i++) {
            final Comparable val = (Comparable) row.get(dimensionStartPosition + i - 1);
            key[i] = valueConvertFns[i - 1].apply(val);
          }
          return key;
        };
      } else {
        return (row, key) -> {
          for (int i = 0; i < key.length; i++) {
            final Comparable val = (Comparable) row.get(dimensionStartPosition + i);
            key[i] = valueConvertFns[i].apply(val);
          }
          return key;
        };
      }
    }
  }

  public static CloseableGrouperIterator<RowBasedKey, ResultRow> makeGrouperIterator(
      final Grouper<RowBasedKey> grouper,
      final GroupByQuery query,
      final Closeable closeable
  )
  {
    return makeGrouperIterator(grouper, query, null, closeable);
  }

  public static CloseableGrouperIterator<RowBasedKey, ResultRow> makeGrouperIterator(
      final Grouper<RowBasedKey> grouper,
      final GroupByQuery query,
      @Nullable final List<String> dimsToInclude,
      final Closeable closeable
  )
  {
    final boolean includeTimestamp = query.getResultRowHasTimestamp();
    final BitSet dimsToIncludeBitSet = new BitSet(query.getDimensions().size());
    final int resultRowDimensionStart = query.getResultRowDimensionStart();

    if (dimsToInclude != null) {
      for (String dimension : dimsToInclude) {
        final int dimIndex = query.getResultRowPositionLookup().getInt(dimension);
        if (dimIndex >= 0) {
          dimsToIncludeBitSet.set(dimIndex - resultRowDimensionStart);
        }
      }
    }

    return new CloseableGrouperIterator<>(
        grouper.iterator(true),
        entry -> {
          final ResultRow resultRow = ResultRow.create(query.getResultRowSizeWithoutPostAggregators());

          // Add timestamp, maybe.
          if (includeTimestamp) {
            final DateTime timestamp = query.getGranularity().toDateTime(((long) (entry.getKey().getKey()[0])));
            resultRow.set(0, timestamp.getMillis());
          }

          // Add dimensions.
          for (int i = resultRowDimensionStart; i < entry.getKey().getKey().length; i++) {
            if (dimsToInclude == null || dimsToIncludeBitSet.get(i - resultRowDimensionStart)) {
              final Object dimVal = entry.getKey().getKey()[i];
              resultRow.set(
                  i,
                  dimVal instanceof String ? NullHandling.emptyToNullIfNeeded((String) dimVal) : dimVal
              );
            }
          }

          // Add aggregations.
          final int resultRowAggregatorStart = query.getResultRowAggregatorStart();
          for (int i = 0; i < entry.getValues().length; i++) {
            resultRow.set(resultRowAggregatorStart + i, entry.getValues()[i]);
          }

          return resultRow;
        },
        closeable
    );
  }

  public static class RowBasedKey
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
      return () -> {
        IndexedInts index = selector.getRow();
        return index.size() == 0
               ? null
               : selector.lookupName(index.get(0));
      };
    }
  }

  private static class InputRawSupplierColumnSelectorStrategyFactory
      implements ColumnSelectorStrategyFactory<InputRawSupplierColumnSelectorStrategy>
  {
    @Override
    public InputRawSupplierColumnSelectorStrategy makeColumnSelectorStrategy(
        ColumnCapabilities capabilities,
        ColumnValueSelector selector
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
      functions[i] = input -> DimensionHandlerUtils.convertObjectToType(input, type);
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
          return (entry1, entry2) -> {
            final int cmp = compareDimsInRows(entry1.getKey(), entry2.getKey(), 1);
            if (cmp != 0) {
              return cmp;
            }

            return Longs.compare((long) entry1.getKey().getKey()[0], (long) entry2.getKey().getKey()[0]);
          };
        } else {
          return (entry1, entry2) -> {
            final int timeCompare = Longs.compare(
                (long) entry1.getKey().getKey()[0],
                (long) entry2.getKey().getKey()[0]
            );

            if (timeCompare != 0) {
              return timeCompare;
            }

            return compareDimsInRows(entry1.getKey(), entry2.getKey(), 1);
          };
        }
      } else {
        return (entry1, entry2) -> compareDimsInRows(entry1.getKey(), entry2.getKey(), 0);
      }
    }

    private Comparator<Grouper.Entry<RowBasedKey>> objectComparatorWithAggs()
    {
      // use the actual sort order from the limitspec if pushing down to merge partial results correctly
      final List<Boolean> needsReverses = new ArrayList<>();
      final List<Boolean> aggFlags = new ArrayList<>();
      final List<Boolean> isNumericField = new ArrayList<>();
      final List<StringComparator> comparators = new ArrayList<>();
      final List<Integer> fieldIndices = new ArrayList<>();
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
          boolean isNumeric = ValueType.isNumeric(dimensions.get(i).getOutputType());
          isNumericField.add(isNumeric);
          if (isNumeric) {
            comparators.add(StringComparators.NUMERIC);
          } else {
            comparators.add(StringComparators.LEXICOGRAPHIC);
          }
        }
      }

      if (includeTimestamp) {
        if (sortByDimsFirst) {
          return (entry1, entry2) -> {
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
          };
        } else {
          return (entry1, entry2) -> {
            final int timeCompare = Longs.compare(
                (long) entry1.getKey().getKey()[0],
                (long) entry2.getKey().getKey()[0]
            );

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
          };
        }
      } else {
        return (entry1, entry2) -> compareDimsInRowsWithAggs(
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
    }

    private static int compareDimsInRows(RowBasedKey key1, RowBasedKey key2, int dimStart)
    {
      for (int i = dimStart; i < key1.getKey().length; i++) {
        final int cmp = Comparators.<Comparable>naturalNullsFirst().compare(
            (Comparable) key1.getKey()[i],
            (Comparable) key2.getKey()[i]
        );
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
          cmp = Comparators.<Comparable>naturalNullsFirst().compare(lhs, rhs);
        } else {
          cmp = comparator.compare(
              DimensionHandlerUtils.convertObjectToString(lhs),
              DimensionHandlerUtils.convertObjectToString(rhs)
          );
        }

        if (cmp != 0) {
          return cmp;
        }
      }

      return 0;
    }
  }

  static long estimateStringKeySize(@Nullable String key)
  {
    long length = key == null ? 0 : key.length();
    return length * Character.BYTES + ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY;
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
    @Nullable
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
      this.keySize = (includeTimestamp ? Long.BYTES : 0) + getTotalKeySize();
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
          (i1, i2) -> Comparators.<String>naturalNullsFirst().compare(dictionary.get(i1), dictionary.get(i2))
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
        dimsPosition = position + Long.BYTES;
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

      return GrouperBufferComparatorUtils.bufferComparator(
          includeTimestamp,
          sortByDimsFirst,
          dimCount,
          serdeHelperComparators
      );
    }

    @Override
    public Grouper.BufferComparator bufferComparatorWithAggregators(
        AggregatorFactory[] aggregatorFactories,
        int[] aggregatorOffsets
    )
    {
      return GrouperBufferComparatorUtils.bufferComparatorWithAggregators(
          aggregatorFactories,
          aggregatorOffsets,
          limitSpec,
          dimensions,
          serdeHelperComparators,
          includeTimestamp,
          sortByDimsFirst
      );
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

      return helpers.toArray(new RowBasedKeySerdeHelper[0]);
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
          return makeNullHandlingNumericserdeHelper(valueType, keyBufferPosition, pushLimitDown, stringComparator);
        default:
          throw new IAE("invalid type: %s", valueType);
      }
    }

    private RowBasedKeySerdeHelper makeNullHandlingNumericserdeHelper(
        ValueType valueType,
        int keyBufferPosition,
        boolean pushLimitDown,
        @Nullable StringComparator stringComparator
    )
    {
      if (NullHandling.sqlCompatible()) {
        return new NullableRowBasedKeySerdeHelper(
            makeNumericSerdeHelper(
                valueType,
                keyBufferPosition + Byte.BYTES,
                pushLimitDown,
                stringComparator
            ),
            keyBufferPosition
        );
      } else {
        return makeNumericSerdeHelper(valueType, keyBufferPosition, pushLimitDown, stringComparator);
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
        return Integer.BYTES;
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
        bufferComparator = GrouperBufferComparatorUtils.makeBufferComparatorForLong(
            keyBufferPosition,
            pushLimitDown,
            stringComparator
        );
      }

      @Override
      public int getKeyBufferValueSize()
      {
        return Long.BYTES;
      }

      @Override
      public boolean putToKeyBuffer(RowBasedKey key, int idx)
      {
        keyBuffer.putLong(DimensionHandlerUtils.nullToZero((Long) key.getKey()[idx]));
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
          @Nullable StringComparator stringComparator
      )
      {
        this.keyBufferPosition = keyBufferPosition;
        bufferComparator = GrouperBufferComparatorUtils.makeBufferComparatorForFloat(
            keyBufferPosition,
            pushLimitDown,
            stringComparator
        );
      }

      @Override
      public int getKeyBufferValueSize()
      {
        return Float.BYTES;
      }

      @Override
      public boolean putToKeyBuffer(RowBasedKey key, int idx)
      {
        keyBuffer.putFloat(DimensionHandlerUtils.nullToZero((Float) key.getKey()[idx]));
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
        bufferComparator = GrouperBufferComparatorUtils.makeBufferComparatorForDouble(
            keyBufferPosition,
            pushLimitDown,
            stringComparator
        );
      }

      @Override
      public int getKeyBufferValueSize()
      {
        return Double.BYTES;
      }

      @Override
      public boolean putToKeyBuffer(RowBasedKey key, int idx)
      {
        keyBuffer.putDouble(DimensionHandlerUtils.nullToZero((Double) key.getKey()[idx]));
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

    // This class is only used when SQL compatible null handling is enabled.
    // When serializing the key, it will add a byte to store the nullability of the serialized object before
    // serializing the key using delegate RowBasedKeySerdeHelper.
    // Buffer Layout - 1 byte for storing nullability + bytes from delegate RowBasedKeySerdeHelper.
    private class NullableRowBasedKeySerdeHelper implements RowBasedKeySerdeHelper
    {
      private final RowBasedKeySerdeHelper delegate;
      private final int keyBufferPosition;
      private final BufferComparator comparator;

      NullableRowBasedKeySerdeHelper(RowBasedKeySerdeHelper delegate, int keyBufferPosition)
      {
        this.delegate = delegate;
        this.keyBufferPosition = keyBufferPosition;
        this.comparator = GrouperBufferComparatorUtils.makeNullHandlingBufferComparatorForNumericData(
            keyBufferPosition,
            this.delegate.getBufferComparator()
        );
      }

      @Override
      public int getKeyBufferValueSize()
      {
        return delegate.getKeyBufferValueSize() + Byte.BYTES;
      }

      @Override
      public boolean putToKeyBuffer(RowBasedKey key, int idx)
      {
        Object val = key.getKey()[idx];
        if (val == null) {
          keyBuffer.put(NullHandling.IS_NULL_BYTE);
        } else {
          keyBuffer.put(NullHandling.IS_NOT_NULL_BYTE);
        }
        delegate.putToKeyBuffer(key, idx);
        return true;
      }

      @Override
      public void getFromByteBuffer(ByteBuffer buffer, int initialOffset, int dimValIdx, Comparable[] dimValues)
      {
        if (buffer.get(initialOffset + keyBufferPosition) == NullHandling.IS_NULL_BYTE) {
          dimValues[dimValIdx] = null;
        } else {
          delegate.getFromByteBuffer(buffer, initialOffset, dimValIdx, dimValues);
        }
      }

      @Override
      public BufferComparator getBufferComparator()
      {
        return comparator;
      }
    }
  }
}
