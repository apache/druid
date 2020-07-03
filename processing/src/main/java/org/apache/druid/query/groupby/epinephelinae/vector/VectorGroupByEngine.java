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

package org.apache.druid.query.groupby.epinephelinae.vector;

import com.google.common.base.Suppliers;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.QueryConfig;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.AggregateResult;
import org.apache.druid.query.groupby.epinephelinae.BufferArrayGrouper;
import org.apache.druid.query.groupby.epinephelinae.CloseableGrouperIterator;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2;
import org.apache.druid.query.groupby.epinephelinae.HashVectorGrouper;
import org.apache.druid.query.groupby.epinephelinae.VectorGrouper;
import org.apache.druid.query.vector.VectorCursorGranularizer;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class VectorGroupByEngine
{
  private VectorGroupByEngine()
  {
    // No instantiation.
  }

  public static boolean canVectorize(
      final GroupByQuery query,
      final StorageAdapter adapter,
      @Nullable final Filter filter
  )
  {
    // Multi-value dimensions are not yet supported.
    //
    // Two notes here about how we're handling this check:
    //   1) After multi-value dimensions are supported, we could alter "GroupByQueryEngineV2.isAllSingleValueDims"
    //      to accept a ColumnSelectorFactory, which makes more sense than using a StorageAdapter (see #8013).
    //   2) Technically using StorageAdapter here is bad since it only looks at real columns, but they might
    //      be shadowed by virtual columns (again, see #8013). But it's fine for now since adapter.canVectorize
    //      always returns false if there are any virtual columns.
    //
    // This situation should sort itself out pretty well once this engine supports multi-valued columns. Then we
    // won't have to worry about having this all-single-value-dims check here.

    return GroupByQueryEngineV2.isAllSingleValueDims(adapter::getColumnCapabilities, query.getDimensions(), true)
           && query.getDimensions().stream().allMatch(DimensionSpec::canVectorize)
           && query.getAggregatorSpecs().stream().allMatch(aggregatorFactory -> aggregatorFactory.canVectorize(adapter))
           && adapter.canVectorize(filter, query.getVirtualColumns(), false);
  }

  public static Sequence<ResultRow> process(
      final GroupByQuery query,
      final StorageAdapter storageAdapter,
      final ByteBuffer processingBuffer,
      @Nullable final DateTime fudgeTimestamp,
      @Nullable final Filter filter,
      final Interval interval,
      final GroupByQueryConfig config,
      final QueryConfig queryConfig
  )
  {
    if (!canVectorize(query, storageAdapter, filter)) {
      throw new ISE("Cannot vectorize");
    }

    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<ResultRow, CloseableIterator<ResultRow>>()
        {
          @Override
          public CloseableIterator<ResultRow> make()
          {
            final VectorCursor cursor = storageAdapter.makeVectorCursor(
                Filters.toFilter(query.getDimFilter()),
                interval,
                query.getVirtualColumns(),
                false,
                queryConfig.getVectorSize(),
                null
            );

            if (cursor == null) {
              // Return empty iterator.
              return new CloseableIterator<ResultRow>()
              {
                @Override
                public boolean hasNext()
                {
                  return false;
                }

                @Override
                public ResultRow next()
                {
                  throw new NoSuchElementException();
                }

                @Override
                public void close()
                {
                  // Nothing to do.
                }
              };
            }

            try {
              final VectorColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
              final List<GroupByVectorColumnSelector> dimensions = query.getDimensions().stream().map(
                  dimensionSpec ->
                      DimensionHandlerUtils.makeVectorProcessor(
                          dimensionSpec,
                          GroupByVectorColumnProcessorFactory.instance(),
                          columnSelectorFactory
                      )
              ).collect(Collectors.toList());

              return new VectorGroupByEngineIterator(
                  query,
                  config,
                  storageAdapter,
                  cursor,
                  interval,
                  dimensions,
                  processingBuffer,
                  fudgeTimestamp
              );
            }
            catch (Throwable e) {
              try {
                cursor.close();
              }
              catch (Throwable e2) {
                e.addSuppressed(e2);
              }
              throw e;
            }
          }

          @Override
          public void cleanup(CloseableIterator<ResultRow> iterFromMake)
          {
            try {
              iterFromMake.close();
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
    );
  }

  private static class VectorGroupByEngineIterator implements CloseableIterator<ResultRow>
  {
    private final GroupByQuery query;
    private final GroupByQueryConfig querySpecificConfig;
    private final StorageAdapter storageAdapter;
    private final VectorCursor cursor;
    private final List<GroupByVectorColumnSelector> selectors;
    private final ByteBuffer processingBuffer;
    private final DateTime fudgeTimestamp;
    private final int keySize;
    private final WritableMemory keySpace;
    private final VectorGrouper vectorGrouper;

    @Nullable
    private final VectorCursorGranularizer granulizer;

    // Granularity-bucket iterator and current bucket.
    private final Iterator<Interval> bucketIterator;

    @Nullable
    private Interval bucketInterval;

    private int partiallyAggregatedRows = -1;

    @Nullable
    private CloseableGrouperIterator<Memory, ResultRow> delegate = null;

    VectorGroupByEngineIterator(
        final GroupByQuery query,
        final GroupByQueryConfig config,
        final StorageAdapter storageAdapter,
        final VectorCursor cursor,
        final Interval queryInterval,
        final List<GroupByVectorColumnSelector> selectors,
        final ByteBuffer processingBuffer,
        @Nullable final DateTime fudgeTimestamp
    )
    {
      this.query = query;
      this.querySpecificConfig = config;
      this.storageAdapter = storageAdapter;
      this.cursor = cursor;
      this.selectors = selectors;
      this.processingBuffer = processingBuffer;
      this.fudgeTimestamp = fudgeTimestamp;
      this.keySize = selectors.stream().mapToInt(GroupByVectorColumnSelector::getGroupingKeySize).sum();
      this.keySpace = WritableMemory.allocate(keySize * cursor.getMaxVectorSize());
      this.vectorGrouper = makeGrouper();
      this.granulizer = VectorCursorGranularizer.create(storageAdapter, cursor, query.getGranularity(), queryInterval);

      if (granulizer != null) {
        this.bucketIterator = granulizer.getBucketIterable().iterator();
      } else {
        this.bucketIterator = Collections.emptyIterator();
      }

      this.bucketInterval = this.bucketIterator.hasNext() ? this.bucketIterator.next() : null;
    }

    @Override
    public ResultRow next()
    {
      if (delegate == null || !delegate.hasNext()) {
        throw new NoSuchElementException();
      }

      return delegate.next();
    }

    @Override
    public boolean hasNext()
    {
      if (delegate != null && delegate.hasNext()) {
        return true;
      } else {
        final boolean moreToRead = !cursor.isDone() || partiallyAggregatedRows >= 0;

        if (bucketInterval != null && moreToRead) {
          while (delegate == null || !delegate.hasNext()) {
            if (delegate != null) {
              delegate.close();
              vectorGrouper.reset();
            }

            delegate = initNewDelegate();
          }
          return true;
        } else {
          return false;
        }
      }
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
      cursor.close();

      if (delegate != null) {
        delegate.close();
      }
    }

    private VectorGrouper makeGrouper()
    {
      final VectorGrouper grouper;

      final int cardinalityForArrayAggregation = GroupByQueryEngineV2.getCardinalityForArrayAggregation(
          querySpecificConfig,
          query,
          storageAdapter,
          processingBuffer
      );

      if (cardinalityForArrayAggregation >= 0) {
        grouper = new BufferArrayGrouper(
            Suppliers.ofInstance(processingBuffer),
            AggregatorAdapters.factorizeVector(
                cursor.getColumnSelectorFactory(),
                query.getAggregatorSpecs()
            ),
            cardinalityForArrayAggregation
        );
      } else {
        grouper = new HashVectorGrouper(
            Suppliers.ofInstance(processingBuffer),
            keySize,
            AggregatorAdapters.factorizeVector(
                cursor.getColumnSelectorFactory(),
                query.getAggregatorSpecs()
            ),
            querySpecificConfig.getBufferGrouperMaxSize(),
            querySpecificConfig.getBufferGrouperMaxLoadFactor(),
            querySpecificConfig.getBufferGrouperInitialBuckets()
        );
      }

      grouper.initVectorized(cursor.getMaxVectorSize());

      return grouper;
    }

    private CloseableGrouperIterator<Memory, ResultRow> initNewDelegate()
    {
      // Method must not be called unless there's a current bucketInterval.
      assert bucketInterval != null;

      final DateTime timestamp = fudgeTimestamp != null
                                 ? fudgeTimestamp
                                 : query.getGranularity().toDateTime(bucketInterval.getStartMillis());

      while (!cursor.isDone()) {
        final int startOffset;

        if (partiallyAggregatedRows < 0) {
          granulizer.setCurrentOffsets(bucketInterval);
          startOffset = granulizer.getStartOffset();
        } else {
          startOffset = granulizer.getStartOffset() + partiallyAggregatedRows;
        }

        if (granulizer.getEndOffset() > startOffset) {
          // Write keys to the keySpace.
          int keyOffset = 0;
          for (final GroupByVectorColumnSelector selector : selectors) {
            selector.writeKeys(keySpace, keySize, keyOffset, startOffset, granulizer.getEndOffset());
            keyOffset += selector.getGroupingKeySize();
          }

          // Aggregate this vector.
          final AggregateResult result = vectorGrouper.aggregateVector(
              keySpace,
              startOffset,
              granulizer.getEndOffset()
          );

          if (result.isOk()) {
            partiallyAggregatedRows = -1;
          } else {
            if (partiallyAggregatedRows < 0) {
              partiallyAggregatedRows = result.getCount();
            } else {
              partiallyAggregatedRows += result.getCount();
            }
          }
        } else {
          partiallyAggregatedRows = -1;
        }

        if (partiallyAggregatedRows >= 0) {
          break;
        } else if (!granulizer.advanceCursorWithinBucket()) {
          // Advance bucketInterval.
          bucketInterval = bucketIterator.hasNext() ? bucketIterator.next() : null;
          break;
        }
      }

      final boolean resultRowHasTimestamp = query.getResultRowHasTimestamp();
      final int resultRowDimensionStart = query.getResultRowDimensionStart();
      final int resultRowAggregatorStart = query.getResultRowAggregatorStart();

      return new CloseableGrouperIterator<>(
          vectorGrouper.iterator(),
          entry -> {
            final ResultRow resultRow = ResultRow.create(query.getResultRowSizeWithoutPostAggregators());

            // Add timestamp, if necessary.
            if (resultRowHasTimestamp) {
              resultRow.set(0, timestamp.getMillis());
            }

            // Add dimensions.
            int keyOffset = 0;
            for (int i = 0; i < selectors.size(); i++) {
              final GroupByVectorColumnSelector selector = selectors.get(i);

              selector.writeKeyToResultRow(
                  entry.getKey(),
                  keyOffset,
                  resultRow,
                  resultRowDimensionStart + i
              );

              keyOffset += selector.getGroupingKeySize();
            }

            // Convert dimension values to desired output types, possibly.
            GroupByQueryEngineV2.convertRowTypesToOutputTypes(
                query.getDimensions(),
                resultRow,
                resultRowDimensionStart
            );

            // Add aggregations.
            for (int i = 0; i < entry.getValues().length; i++) {
              resultRow.set(resultRowAggregatorStart + i, entry.getValues()[i]);
            }

            return resultRow;
          },
          vectorGrouper
      );
    }
  }
}
