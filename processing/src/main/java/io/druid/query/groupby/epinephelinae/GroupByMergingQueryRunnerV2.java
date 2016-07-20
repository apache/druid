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
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.collections.BlockingPool;
import io.druid.collections.ReferenceCountingResourceHolder;
import io.druid.collections.Releaser;
import io.druid.common.utils.JodaUtils;
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
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);

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

    final File temporaryStorageDirectory = new File(
        System.getProperty("java.io.tmpdir"),
        String.format("druid-groupBy-%s_%s", UUID.randomUUID(), query.getId())
    );

    // Figure out timeoutAt time now, so we can apply the timeout to both the mergeBufferPool.take and the actual
    // query processing together.
    final Number queryTimeout = query.getContextValue(QueryContextKeys.TIMEOUT, null);
    final long timeoutAt = queryTimeout == null
                           ? JodaUtils.MAX_INSTANT
                           : System.currentTimeMillis() + queryTimeout.longValue();

    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<Row, CloseableGrouperIterator<GroupByMergingKey, Row>>()
        {
          @Override
          public CloseableGrouperIterator<GroupByMergingKey, Row> make()
          {
            final List<Closeable> closeOnFailure = Lists.newArrayList();

            try {
              final ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder;
              final LimitedTemporaryStorage temporaryStorage;
              final Grouper<GroupByMergingKey> grouper;

              temporaryStorage = new LimitedTemporaryStorage(
                  temporaryStorageDirectory,
                  querySpecificConfig.getMaxOnDiskStorage()
              );
              closeOnFailure.add(temporaryStorage);

              try {
                // This will potentially block if there are no merge buffers left in the pool.
                final long timeout = timeoutAt - System.currentTimeMillis();
                if (timeout <= 0 || (mergeBufferHolder = mergeBufferPool.take(timeout)) == null) {
                  throw new QueryInterruptedException(new TimeoutException());
                }
                closeOnFailure.add(mergeBufferHolder);
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
              }

              final GroupByMergingKeySerdeFactory keySerdeFactory = new GroupByMergingKeySerdeFactory(
                  query.getDimensions().size(),
                  querySpecificConfig.getMaxMergingDictionarySize() / concurrencyHint
              );
              final GroupByMergingColumnSelectorFactory columnSelectorFactory = new GroupByMergingColumnSelectorFactory();

              grouper = new ConcurrentGrouper<>(
                  mergeBufferHolder.get(),
                  concurrencyHint,
                  temporaryStorage,
                  spillMapper,
                  querySpecificConfig.getBufferGrouperMaxSize(),
                  querySpecificConfig.getBufferGrouperInitialBuckets(),
                  keySerdeFactory,
                  columnSelectorFactory,
                  combiningAggregatorFactories
              );
              closeOnFailure.add(grouper);

              final Accumulator<Grouper<GroupByMergingKey>, Row> accumulator = new Accumulator<Grouper<GroupByMergingKey>, Row>()
              {
                @Override
                public Grouper<GroupByMergingKey> accumulate(
                    final Grouper<GroupByMergingKey> theGrouper,
                    final Row row
                )
                {
                  if (theGrouper == null) {
                    // Pass-through null returns without doing more work.
                    return null;
                  }

                  final long timestamp = row.getTimestampFromEpoch();

                  final String[] dimensions = new String[query.getDimensions().size()];
                  for (int i = 0; i < dimensions.length; i++) {
                    final Object dimValue = row.getRaw(query.getDimensions().get(i).getOutputName());
                    dimensions[i] = Strings.nullToEmpty((String) dimValue);
                  }

                  columnSelectorFactory.setRow(row);
                  final boolean didAggregate = theGrouper.aggregate(new GroupByMergingKey(timestamp, dimensions));
                  if (!didAggregate) {
                    // null return means grouping resources were exhausted.
                    return null;
                  }
                  columnSelectorFactory.setRow(null);

                  return theGrouper;
                }
              };

              final int priority = BaseQuery.getContextPriority(query, 0);

              final ReferenceCountingResourceHolder<Grouper<GroupByMergingKey>> grouperHolder = new ReferenceCountingResourceHolder<>(
                  grouper,
                  new Closeable()
                  {
                    @Override
                    public void close() throws IOException
                    {
                      grouper.close();
                    }
                  }
              );

              ListenableFuture<List<Boolean>> futures = Futures.allAsList(
                  Lists.newArrayList(
                      Iterables.transform(
                          queryables,
                          new Function<QueryRunner<Row>, ListenableFuture<Boolean>>()
                          {
                            @Override
                            public ListenableFuture<Boolean> apply(final QueryRunner<Row> input)
                            {
                              if (input == null) {
                                throw new ISE(
                                    "Null queryRunner! Looks to be some segment unmapping action happening"
                                );
                              }

                              final Releaser bufferReleaser = mergeBufferHolder.increment();
                              try {
                                final Releaser grouperReleaser = grouperHolder.increment();
                                try {
                                  return exec.submit(
                                      new AbstractPrioritizedCallable<Boolean>(priority)
                                      {
                                        @Override
                                        public Boolean call() throws Exception
                                        {
                                          try {
                                            final Object retVal = input.run(queryForRunners, responseContext)
                                                                       .accumulate(grouper, accumulator);

                                            // Return true if OK, false if resources were exhausted.
                                            return retVal == grouper;
                                          }
                                          catch (QueryInterruptedException e) {
                                            throw e;
                                          }
                                          catch (Exception e) {
                                            log.error(e, "Exception with one of the sequences!");
                                            throw Throwables.propagate(e);
                                          }
                                          finally {
                                            grouperReleaser.close();
                                            bufferReleaser.close();
                                          }
                                        }
                                      }
                                  );
                                }
                                catch (Exception e) {
                                  // Exception caught while submitting the task; release resources.
                                  grouperReleaser.close();
                                  throw e;
                                }
                              }
                              catch (Exception e) {
                                // Exception caught while submitting the task; release resources.
                                bufferReleaser.close();
                                throw e;
                              }
                            }
                          }
                      )
                  )
              );

              waitForFutureCompletion(query, futures, timeoutAt - System.currentTimeMillis());

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
                  },
                  new Closeable()
                  {
                    @Override
                    public void close() throws IOException
                    {
                      grouperHolder.close();
                      mergeBufferHolder.close();
                      CloseQuietly.close(temporaryStorage);
                    }
                  }
              );
            }
            catch (Throwable e) {
              // Exception caught while setting up the iterator; release resources.
              for (Closeable closeable : Lists.reverse(closeOnFailure)) {
                CloseQuietly.close(closeable);
              }
              throw e;
            }
          }

          @Override
          public void cleanup(CloseableGrouperIterator<GroupByMergingKey, Row> iterFromMake)
          {
            iterFromMake.close();
          }
        }
    );
  }

  private void waitForFutureCompletion(
      GroupByQuery query,
      ListenableFuture<List<Boolean>> future,
      long timeout
  )
  {
    try {
      if (queryWatcher != null) {
        queryWatcher.registerQuery(query, future);
      }

      if (timeout <= 0) {
        throw new TimeoutException();
      }

      final List<Boolean> results = future.get(timeout, TimeUnit.MILLISECONDS);

      for (Boolean result : results) {
        if (!result) {
          future.cancel(true);
          throw new ISE("Grouping resources exhausted");
        }
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

  static class GroupByMergingKey implements Comparable<GroupByMergingKey>
  {
    private final long timestamp;
    private final String[] dimensions;

    @JsonCreator
    public GroupByMergingKey(
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

  static class GroupByMergingKeySerdeFactory implements Grouper.KeySerdeFactory<GroupByMergingKey>
  {
    private final int dimCount;
    private final long maxDictionarySize;

    public GroupByMergingKeySerdeFactory(int dimCount, long maxDictionarySize)
    {
      this.dimCount = dimCount;
      this.maxDictionarySize = maxDictionarySize;
    }

    @Override
    public Grouper.KeySerde<GroupByMergingKey> factorize()
    {
      return new GroupByMergingKeySerde(dimCount, maxDictionarySize);
    }
  }

  static class GroupByMergingKeySerde implements Grouper.KeySerde<GroupByMergingKey>
  {
    // Entry in dictionary, node pointer in reverseDictionary, hash + k/v/next pointer in reverseDictionary nodes
    private static final int ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY = Longs.BYTES * 5 + Ints.BYTES;

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

    public GroupByMergingKeySerde(final int dimCount, final long maxDictionarySize)
    {
      this.dimCount = dimCount;
      this.maxDictionarySize = maxDictionarySize;
      this.keySize = Longs.BYTES + dimCount * Ints.BYTES;
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

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      // getColumnCapabilities() is only used by FilteredAggregatorFactory for determining dimension types.
      // Since FilteredAggregatorFactory only works with ColumnSelectorFactory implementations
      // that support makeDimensionSelector(), this method is also left unsupported.
      throw new UnsupportedOperationException();
    }
  }
}
