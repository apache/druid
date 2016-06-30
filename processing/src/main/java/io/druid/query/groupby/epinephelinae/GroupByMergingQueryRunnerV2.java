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
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Chars;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.collections.BlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.BaseQuery;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunner;
import io.druid.query.QueryWatcher;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionHandlerUtil;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ValueType;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class GroupByMergingQueryRunnerV2 implements QueryRunner
{
  private static final Logger log = new Logger(GroupByMergingQueryRunnerV2.class);
  private static final String CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION = "mergeRunnersUsingChainedExecution";

  private final GroupByQueryConfig config;
  private final Iterable<QueryRunner<Row>> queryables;
  private final ListeningExecutorService exec;
  private final QueryWatcher queryWatcher;
  private final int concurrencyHint;
  private final BlockingPool<ByteBuffer> mergeBufferPool;
  private final ObjectMapper spillMapper;

  public GroupByMergingQueryRunnerV2(
      GroupByQueryConfig config,
      ExecutorService exec,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<Row>> queryables,
      int concurrencyHint,
      BlockingPool<ByteBuffer> mergeBufferPool,
      ObjectMapper spillMapper
  )
  {
    this.config = config;
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.queryWatcher = queryWatcher;
    this.queryables = Iterables.unmodifiableIterable(Iterables.filter(queryables, Predicates.notNull()));
    this.concurrencyHint = concurrencyHint;
    this.mergeBufferPool = mergeBufferPool;
    this.spillMapper = spillMapper;
  }

  @Override
  public Sequence<Row> run(final Query queryParam, final Map responseContext)
  {
    final GroupByQuery query = (GroupByQuery) queryParam;

    // CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION is here because realtime servers use nested mergeRunners calls
    // (one for the entire query and one for each sink). We only want the outer call to actually do merging with a
    // merge buffer, otherwise the query will allocate too many merge buffers. This is potentially sub-optimal as it
    // will involve materializing the results for each sink before starting to feed them into the outer merge buffer.
    // I'm not sure of a better way to do this without tweaking how realtime servers do queries.
    final boolean forceChainedExecution = query.getContextBoolean(
        CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION,
        false
    );
    final GroupByQuery queryForRunners = query.withOverriddenContext(
        ImmutableMap.<String, Object>of(CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION, true)
    );

    if (BaseQuery.getContextBySegment(query, false) || forceChainedExecution) {
      return new ChainedExecutionQueryRunner(exec, queryWatcher, queryables).run(query, responseContext);
    }

    final AggregatorFactory[] combiningAggregatorFactories = new AggregatorFactory[query.getAggregatorSpecs().size()];
    for (int i = 0; i < query.getAggregatorSpecs().size(); i++) {
      combiningAggregatorFactories[i] = query.getAggregatorSpecs().get(i).getCombiningFactory();
    }

    Map<String, ValueType> typeHints = BaseQuery.getContextTypeHints(query);
    final GroupByMergingKeySerdeFactory keySerdeFactory = new GroupByMergingKeySerdeFactory(
        query.getDimensions(),
        config.getMaxMergingDictionarySize() / concurrencyHint,
        typeHints
    );
    final GroupByMergingColumnSelectorFactory columnSelectorFactory = new GroupByMergingColumnSelectorFactory();

    final File temporaryStorageDirectory = new File(
        System.getProperty("java.io.tmpdir"),
        String.format("druid-groupBy-%s_%s", UUID.randomUUID(), query.getId())
    );

    final LimitedTemporaryStorage temporaryStorage = new LimitedTemporaryStorage(
        temporaryStorageDirectory,
        config.getMaxOnDiskStorage()
    );

    // Figure out timeoutAt time now, so we can apply the timeout to both the mergeBufferPool.take and the actual
    // query processing together.
    final long startTime = System.currentTimeMillis();
    final Number timeout = query.getContextValue(QueryContextKeys.TIMEOUT, null);
    final long timeoutAt = timeout == null ? -1L : startTime + timeout.longValue();

    final ResourceHolder<ByteBuffer> mergeBufferHolder;

    try {
      mergeBufferHolder = mergeBufferPool.take(timeout != null && timeout.longValue() > 0 ? timeout.longValue() : -1);
    }
    catch (InterruptedException e) {
      CloseQuietly.close(temporaryStorage);
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }

    final long processingStartTime = System.currentTimeMillis();

    try {
      return new ResourceClosingSequence<>(
          new BaseSequence<>(
              new BaseSequence.IteratorMaker<Row, CloseableGrouperIterator<GroupByMergingKey, Row>>()
              {
                @Override
                public CloseableGrouperIterator<GroupByMergingKey, Row> make()
                {
                  final Grouper<GroupByMergingKey> grouper = new ConcurrentGrouper<>(
                      mergeBufferHolder.get(),
                      concurrencyHint,
                      temporaryStorage,
                      spillMapper,
                      config.getBufferGrouperMaxSize(),
                      GroupByStrategyV2.getBufferGrouperInitialBuckets(config, query),
                      keySerdeFactory,
                      columnSelectorFactory,
                      combiningAggregatorFactories
                  );

                  final Accumulator<Grouper<GroupByMergingKey>, Row> accumulator = new Accumulator<Grouper<GroupByMergingKey>, Row>()
                  {
                    @Override
                    public Grouper<GroupByMergingKey> accumulate(
                        final Grouper<GroupByMergingKey> theGrouper,
                        final Row row
                    )
                    {
                      final long timestamp = row.getTimestampFromEpoch();

                      final String[] dimensions = new String[query.getDimensions().size()];
                      for (int i = 0; i < dimensions.length; i++) {
                        final Object dimValue = row.getRaw(query.getDimensions().get(i).getOutputName());
                        dimensions[i] = Strings.nullToEmpty((String) dimValue);
                      }

                      columnSelectorFactory.setRow(row);
                      final boolean didAggregate = theGrouper.aggregate(new GroupByMergingKey(timestamp, dimensions));
                      if (!didAggregate) {
                        throw new ISE("Grouping resources exhausted");
                      }
                      columnSelectorFactory.setRow(null);

                      return theGrouper;
                    }
                  };

                  final int priority = BaseQuery.getContextPriority(query, 0);

                  ListenableFuture<List<Void>> futures = Futures.allAsList(
                      Lists.newArrayList(
                          Iterables.transform(
                              queryables,
                              new Function<QueryRunner<Row>, ListenableFuture<Void>>()
                              {
                                @Override
                                public ListenableFuture<Void> apply(final QueryRunner<Row> input)
                                {
                                  if (input == null) {
                                    throw new ISE(
                                        "Null queryRunner! Looks to be some segment unmapping action happening"
                                    );
                                  }

                                  return exec.submit(
                                      new AbstractPrioritizedCallable<Void>(priority)
                                      {
                                        @Override
                                        public Void call() throws Exception
                                        {
                                          try {
                                            input.run(queryForRunners, responseContext)
                                                 .accumulate(grouper, accumulator);
                                            return null;
                                          }
                                          catch (QueryInterruptedException e) {
                                            throw Throwables.propagate(e);
                                          }
                                          catch (Exception e) {
                                            log.error(e, "Exception with one of the sequences!");
                                            throw Throwables.propagate(e);
                                          }
                                        }
                                      }
                                  );
                                }
                              }
                          )
                      )
                  );

                  try {
                    waitForFutureCompletion(query, futures, timeoutAt - processingStartTime);
                  }
                  catch (Exception e) {
                    grouper.close();
                    throw e;
                  }

                  return new CloseableGrouperIterator<>(
                      grouper,
                      true,
                      new Function<Grouper.Entry<GroupByMergingKey>, Row>()
                      {
                        @Override
                        public Row apply(Grouper.Entry<GroupByMergingKey> entry)
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
                      }
                  );
                }

                @Override
                public void cleanup(CloseableGrouperIterator<GroupByMergingKey, Row> iterFromMake)
                {
                  iterFromMake.close();
                }
              }
          ),
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              mergeBufferHolder.close();
              CloseQuietly.close(temporaryStorage);
            }
          }
      );
    }
    catch (Exception e) {
      // Exception caught while creating the sequence; release resources.
      mergeBufferHolder.close();
      CloseQuietly.close(temporaryStorage);
      throw e;
    }
  }

  private void waitForFutureCompletion(
      GroupByQuery query,
      ListenableFuture<?> future,
      long timeout
  )
  {
    try {
      if (queryWatcher != null) {
        queryWatcher.registerQuery(query, future);
      }
      if (timeout <= 0) {
        future.get();
      } else {
        future.get(timeout, TimeUnit.MILLISECONDS);
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (CancellationException e) {
      throw new QueryInterruptedException(e);
    }
    catch (TimeoutException e) {
      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
  }

  private static class GroupByMergingKey implements Comparable<GroupByMergingKey>
  {
    private final long timestamp;
    private final Comparable[] dimensions;

    @JsonCreator
    public GroupByMergingKey(
        // Using short key names to reduce serialized size when spilling to disk.
        @JsonProperty("t") long timestamp,
        @JsonProperty("d") Comparable[] dimensions
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
    public Comparable[] getDimensions()
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

      GroupByMergingKey that = (GroupByMergingKey) o;

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
    public int compareTo(GroupByMergingKey other)
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
      return "GroupByMergingKey{" +
             "timestamp=" + timestamp +
             ", dimensions=" + Arrays.toString(dimensions) +
             '}';
    }
  }

  private static class GroupByMergingKeySerdeFactory implements Grouper.KeySerdeFactory<GroupByMergingKey>
  {
    private final long maxDictionarySize;
    private final List<DimensionSpec> dimensions;
    private final Map<String, ValueType> typeHints;

    public GroupByMergingKeySerdeFactory(List<DimensionSpec> dimensions, long maxDictionarySize, Map<String, ValueType> typeHints)
    {
      this.dimensions = dimensions;
      this.maxDictionarySize = maxDictionarySize;
      if (typeHints == null) {
        this.typeHints = new HashMap<>();
      } else {
        this.typeHints = typeHints;
      }
    }

    @Override
    public Grouper.KeySerde<GroupByMergingKey> factorize()
    {
      return new GroupByMergingKeySerde(dimensions, maxDictionarySize, typeHints);
    }
  }

  private static class GroupByMergingKeySerde implements Grouper.KeySerde<GroupByMergingKey>
  {
    // Entry in dictionary, node pointer in reverseDictionary, hash + k/v/next pointer in reverseDictionary nodes
    private static final int ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY = Longs.BYTES * 5 + Ints.BYTES;

    private final int dimCount;
    private final int keySize;
    private final ByteBuffer keyBuffer;
    private final List<String> dictionary = Lists.newArrayList();
    private final Map<String, Integer> reverseDictionary = Maps.newHashMap();
    private final List<DimensionSpec> dimensions;
    private final Map<String, ValueType> typeHints;
    private final Map<String, DimensionHandler> handlerMap;

    // Size limiting for the dictionary, in (roughly estimated) bytes.
    private final long maxDictionarySize;
    private long currentEstimatedSize = 0;

    // dictionary id -> its position if it were sorted by dictionary value
    private int[] sortableIds = null;

    public GroupByMergingKeySerde(final List<DimensionSpec> dimensions, final long maxDictionarySize, Map<String, ValueType> typeHints)
    {
      this.dimensions = dimensions;
      this.dimCount = dimensions.size();
      this.typeHints = typeHints;
      this.maxDictionarySize = maxDictionarySize;
      this.handlerMap = makeHandlerMap();
      this.keySize = getKeySize();
      this.keyBuffer = ByteBuffer.allocate(keySize);
    }

    @Override
    public int keySize()
    {
      return keySize;
    }

    @Override
    public Class<GroupByMergingKey> keyClazz()
    {
      return GroupByMergingKey.class;
    }

    @Override
    public ByteBuffer toByteBuffer(GroupByMergingKey key)
    {
      keyBuffer.rewind();
      keyBuffer.putLong(key.getTimestamp());
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
    public GroupByMergingKey fromByteBuffer(ByteBuffer buffer, int position)
    {
      final long timestamp = buffer.getLong(position);
      final String[] dimensions = new String[dimCount];
      for (int i = 0; i < dimensions.length; i++) {
        dimensions[i] = dictionary.get(buffer.getInt(position + Longs.BYTES + (Ints.BYTES * i)));
      }
      return new GroupByMergingKey(timestamp, dimensions);
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

    private Map<String, DimensionHandler> makeHandlerMap()
    {
      Map<String, DimensionHandler> handlerMap = new LinkedHashMap<>();
      for (DimensionSpec dimension : dimensions) {
        String dimName = dimension.getDimension();
        ValueType type = typeHints.get(dimName);
        if (type == null) {
          type = ValueType.STRING;
        }
        DimensionHandler handler = DimensionHandlerUtil.getHandlerFromType(dimName, type);
        handlerMap.put(dimName, handler);
      }
      return handlerMap;
    }

    private int getKeySize()
    {
      int keySize = Longs.BYTES;
      for (DimensionHandler handler : handlerMap.values()) {
        keySize += handler.getEncodedValueSize();
      }
      return keySize;
    }
  }

  private static class GroupByMergingColumnSelectorFactory implements ColumnSelectorFactory
  {
    private ThreadLocal<Row> row = new ThreadLocal<>();

    public void setRow(Row row)
    {
      this.row.set(row);
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      // Combining factories shouldn't need dimension selectors, that'd be weird.
      throw new UnsupportedOperationException();
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
  }
}
