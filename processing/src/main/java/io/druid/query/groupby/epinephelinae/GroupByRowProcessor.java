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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import io.druid.collections.BlockingPool;
import io.druid.collections.ReferenceCountingResourceHolder;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.Row;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.guava.FilteredSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryInterruptedException;
import io.druid.query.ResourceLimitExceededException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;
import io.druid.segment.column.Column;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class GroupByRowProcessor
{
  public static Sequence<Row> process(
      final Query queryParam,
      final Sequence<Row> rows,
      final GroupByQueryConfig config,
      final BlockingPool<ByteBuffer> mergeBufferPool,
      final ObjectMapper spillMapper
  )
  {
    final GroupByQuery query = (GroupByQuery) queryParam;
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);

    final AggregatorFactory[] aggregatorFactories = new AggregatorFactory[query.getAggregatorSpecs().size()];
    for (int i = 0; i < query.getAggregatorSpecs().size(); i++) {
      aggregatorFactories[i] = query.getAggregatorSpecs().get(i);
    }

    final File temporaryStorageDirectory = new File(
        System.getProperty("java.io.tmpdir"),
        String.format("druid-groupBy-%s_%s", UUID.randomUUID(), query.getId())
    );

    final Number queryTimeout = query.getContextValue(QueryContextKeys.TIMEOUT, null);
    final long timeout = queryTimeout == null ? JodaUtils.MAX_INSTANT : queryTimeout.longValue();
    final List<Interval> queryIntervals = query.getIntervals();
    final Filter filter = Filters.convertToCNFFromQueryContext(
        query,
        Filters.toFilter(query.getDimFilter())
    );
    final RowBasedValueMatcherFactory filterMatcherFactory = new RowBasedValueMatcherFactory();
    final ValueMatcher filterMatcher = filter == null
                                       ? new BooleanValueMatcher(true)
                                       : filter.makeMatcher(filterMatcherFactory);

    final FilteredSequence<Row> filteredSequence = new FilteredSequence<>(
        rows,
        new Predicate<Row>()
        {
          @Override
          public boolean apply(Row input)
          {
            boolean inInterval = false;
            DateTime rowTime = input.getTimestamp();
            for (Interval queryInterval : queryIntervals) {
              if (queryInterval.contains(rowTime)) {
                inInterval = true;
              }
            }
            if (!inInterval) {
              return false;
            }
            filterMatcherFactory.setRow(input);
            return filterMatcher.matches();
          }
        }
    );

    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<Row, CloseableGrouperIterator<RowBasedKey, Row>>()
        {
          @Override
          public CloseableGrouperIterator<RowBasedKey, Row> make()
          {
            final List<Closeable> closeOnFailure = Lists.newArrayList();

            try {
              final LimitedTemporaryStorage temporaryStorage = new LimitedTemporaryStorage(
                  temporaryStorageDirectory,
                  querySpecificConfig.getMaxOnDiskStorage()
              );

              closeOnFailure.add(temporaryStorage);

              final ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder;
              try {
                // This will potentially block if there are no merge buffers left in the pool.
                if (timeout <= 0 || (mergeBufferHolder = mergeBufferPool.take(timeout)) == null) {
                  throw new QueryInterruptedException(new TimeoutException());
                }
                closeOnFailure.add(mergeBufferHolder);
              }
              catch (InterruptedException e) {
                throw new QueryInterruptedException(e);
              }

              Pair<Grouper<RowBasedKey>, Accumulator<Grouper<RowBasedKey>, Row>> pair = RowBasedGrouperHelper.createGrouperAccumulatorPair(
                  query,
                  true,
                  querySpecificConfig,
                  mergeBufferHolder.get(),
                  -1,
                  temporaryStorage,
                  spillMapper,
                  aggregatorFactories
              );
              final Grouper<RowBasedKey> grouper = pair.lhs;
              final Accumulator<Grouper<RowBasedKey>, Row> accumulator = pair.rhs;
              closeOnFailure.add(grouper);

              final Grouper<RowBasedKey> retVal = filteredSequence.accumulate(grouper, accumulator);
              if (retVal != grouper) {
                throw new ResourceLimitExceededException("Grouping resources exhausted");
              }

              return RowBasedGrouperHelper.makeGrouperIterator(
                  grouper,
                  query,
                  new Closeable()
                  {
                    @Override
                    public void close() throws IOException
                    {
                      grouper.close();
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
          public void cleanup(CloseableGrouperIterator<RowBasedKey, Row> iterFromMake)
          {
            iterFromMake.close();
          }
        }
    );
  }

  private static class RowBasedValueMatcherFactory implements ValueMatcherFactory
  {
    private Row row;

    public void setRow(Row row)
    {
      this.row = row;
    }

    @Override
    public ValueMatcher makeValueMatcher(final String dimension, final Comparable value)
    {
      if (dimension.equals(Column.TIME_COLUMN_NAME)) {
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            return value.equals(row.getTimestampFromEpoch());
          }
        };
      } else {
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            return row.getDimension(dimension).contains(value);
          }
        };
      }
    }

    // There is no easy way to determine the dimension value type from the map based row, so this defaults all
    // dimensions (except time) to string, and provide the string value matcher. This has some performance impact
    // on filtering, but should provide the same result. It should be changed to support dimension types when better
    // type hinting is implemented
    @Override
    public ValueMatcher makeValueMatcher(final String dimension, final DruidPredicateFactory predicateFactory)
    {
      if (dimension.equals(Column.TIME_COLUMN_NAME)) {
        return new ValueMatcher()
        {
          final DruidLongPredicate predicate = predicateFactory.makeLongPredicate();

          @Override
          public boolean matches()
          {
            return predicate.applyLong(row.getTimestampFromEpoch());
          }
        };
      } else {
        return new ValueMatcher()
        {
          final Predicate<String> predicate = predicateFactory.makeStringPredicate();

          @Override
          public boolean matches()
          {
            List<String> values = row.getDimension(dimension);
            for (String value : values) {
              if (predicate.apply(value)) {
                return true;
              }
            }
            return false;
          }
        };
      }
    }
  }
}
