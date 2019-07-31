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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.FilteredSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.RowBasedColumnSelectorFactory;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;
import org.apache.druid.query.groupby.resource.GroupByQueryResource;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class GroupByRowProcessor
{
  public static Grouper createGrouper(
      final Query queryParam,
      final Sequence<Row> rows,
      final Map<String, ValueType> rowSignature,
      final GroupByQueryConfig config,
      final GroupByQueryResource resource,
      final ObjectMapper spillMapper,
      final String processingTmpDir,
      final int mergeBufferSize,
      final List<Closeable> closeOnExit,
      final boolean wasQueryPushedDown,
      final boolean useVirtualizedColumnSelectorFactory
  )
  {
    final GroupByQuery query = (GroupByQuery) queryParam;
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);

    final AggregatorFactory[] aggregatorFactories = new AggregatorFactory[query.getAggregatorSpecs().size()];
    for (int i = 0; i < query.getAggregatorSpecs().size(); i++) {
      aggregatorFactories[i] = query.getAggregatorSpecs().get(i);
    }

    final File temporaryStorageDirectory = new File(
        processingTmpDir,
        StringUtils.format("druid-groupBy-%s_%s", UUID.randomUUID(), query.getId())
    );

    Sequence<Row> sequenceToGroup = rows;
    // When query is pushed down, rows have already been filtered
    if (!wasQueryPushedDown) {
      sequenceToGroup = getFilteredSequence(rows, rowSignature, query);
    }

    final LimitedTemporaryStorage temporaryStorage = new LimitedTemporaryStorage(
        temporaryStorageDirectory,
        querySpecificConfig.getMaxOnDiskStorage()
    );

    closeOnExit.add(temporaryStorage);

    Pair<Grouper<RowBasedKey>, Accumulator<AggregateResult, Row>> pair = RowBasedGrouperHelper.createGrouperAccumulatorPair(
        query,
        true,
        rowSignature,
        querySpecificConfig,
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            final ResourceHolder<ByteBuffer> mergeBufferHolder = resource.getMergeBuffer();
            closeOnExit.add(mergeBufferHolder);
            return mergeBufferHolder.get();
          }
        },
        temporaryStorage,
        spillMapper,
        aggregatorFactories,
        mergeBufferSize,
        useVirtualizedColumnSelectorFactory
    );
    final Grouper<RowBasedKey> grouper = pair.lhs;
    final Accumulator<AggregateResult, Row> accumulator = pair.rhs;
    closeOnExit.add(grouper);

    final AggregateResult retVal = sequenceToGroup.accumulate(AggregateResult.ok(), accumulator);

    if (!retVal.isOk()) {
      throw new ResourceLimitExceededException(retVal.getReason());
    }

    return grouper;
  }

  private static Sequence<Row> getFilteredSequence(
      Sequence<Row> rows,
      Map<String, ValueType> rowSignature,
      GroupByQuery query
  )
  {
    final List<Interval> queryIntervals = query.getIntervals();
    final Filter filter = Filters.convertToCNFFromQueryContext(
        query,
        Filters.toFilter(query.getDimFilter())
    );

    final SettableSupplier<Row> rowSupplier = new SettableSupplier<>();
    final RowBasedColumnSelectorFactory columnSelectorFactory = RowBasedColumnSelectorFactory.create(
        rowSupplier,
        rowSignature
    );
    final ValueMatcher filterMatcher = filter == null
                                       ? BooleanValueMatcher.of(true)
                                       : filter.makeMatcher(columnSelectorFactory);

    return new FilteredSequence<>(
        rows,
        input -> {
          boolean inInterval = false;
          DateTime rowTime = input.getTimestamp();
          for (Interval queryInterval : queryIntervals) {
            if (queryInterval.contains(rowTime)) {
              inInterval = true;
              break;
            }
          }
          if (!inInterval) {
            return false;
          }
          rowSupplier.set(input);
          return filterMatcher.matches();
        }
    );
  }

  public static Sequence<Row> getRowsFromGrouper(GroupByQuery query, @Nullable List<String> subtotalSpec, Supplier<Grouper> grouper)
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<Row, CloseableGrouperIterator<RowBasedKey, Row>>()
        {
          @Override
          public CloseableGrouperIterator<RowBasedKey, Row> make()
          {
            return RowBasedGrouperHelper.makeGrouperIterator(
                grouper.get(),
                query,
                subtotalSpec,
                () -> {}
            );
          }

          @Override
          public void cleanup(CloseableGrouperIterator<RowBasedKey, Row> iterFromMake)
          {
            iterFromMake.close();
          }
        }
    );

  }
}
