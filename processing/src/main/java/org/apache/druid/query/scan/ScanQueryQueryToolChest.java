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

package org.apache.druid.query.scan;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.inject.Inject;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.frame.segment.FrameCursorUtils;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.IterableRowsCursorHelper;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.utils.CloseableUtils;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ScanQueryQueryToolChest extends QueryToolChest<ScanResultValue, ScanQuery>
{
  private static final TypeReference<ScanResultValue> TYPE_REFERENCE = new TypeReference<ScanResultValue>()
  {
  };

  private final ScanQueryConfig scanQueryConfig;
  private final GenericQueryMetricsFactory queryMetricsFactory;

  @Inject
  public ScanQueryQueryToolChest(
      final ScanQueryConfig scanQueryConfig,
      final GenericQueryMetricsFactory queryMetricsFactory
  )
  {
    this.scanQueryConfig = scanQueryConfig;
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryRunner<ScanResultValue> mergeResults(final QueryRunner<ScanResultValue> runner)
  {
    return (queryPlus, responseContext) -> {
      final ScanQuery originalQuery = ((ScanQuery) (queryPlus.getQuery()));
      ScanQuery.verifyOrderByForNativeExecution(originalQuery);

      // Remove "offset" and add it to the "limit" (we won't push the offset down, just apply it here, at the
      // merge at the top of the stack).
      final long newLimit;
      if (!originalQuery.isLimited()) {
        // Unlimited stays unlimited.
        newLimit = Long.MAX_VALUE;
      } else if (originalQuery.getScanRowsLimit() > Long.MAX_VALUE - originalQuery.getScanRowsOffset()) {
        throw new ISE(
            "Cannot apply limit[%d] with offset[%d] due to overflow",
            originalQuery.getScanRowsLimit(),
            originalQuery.getScanRowsOffset()
        );
      } else {
        newLimit = originalQuery.getScanRowsLimit() + originalQuery.getScanRowsOffset();
      }

      // Ensure "legacy" is a non-null value, such that all other nodes this query is forwarded to will treat it
      // the same way, even if they have different default legacy values.
      final ScanQuery queryToRun = originalQuery.withNonNullLegacy(scanQueryConfig)
                                                .withOffset(0)
                                                .withLimit(newLimit);

      final Sequence<ScanResultValue> results;

      if (!queryToRun.isLimited()) {
        results = runner.run(queryPlus.withQuery(queryToRun), responseContext);
      } else {
        results = new BaseSequence<>(
            new BaseSequence.IteratorMaker<ScanResultValue, ScanQueryLimitRowIterator>()
            {
              @Override
              public ScanQueryLimitRowIterator make()
              {
                return new ScanQueryLimitRowIterator(runner, queryPlus.withQuery(queryToRun), responseContext);
              }

              @Override
              public void cleanup(ScanQueryLimitRowIterator iterFromMake)
              {
                CloseableUtils.closeAndWrapExceptions(iterFromMake);
              }
            });
      }

      if (originalQuery.getScanRowsOffset() > 0) {
        return new ScanQueryOffsetSequence(results, originalQuery.getScanRowsOffset());
      } else {
        return results;
      }
    };
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(ScanQuery query)
  {
    return queryMetricsFactory.makeMetrics(query);
  }

  @Override
  public Function<ScanResultValue, ScanResultValue> makePreComputeManipulatorFn(
      ScanQuery query,
      MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<ScanResultValue> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public QueryRunner<ScanResultValue> preMergeQueryDecoration(final QueryRunner<ScanResultValue> runner)
  {
    return (queryPlus, responseContext) -> {
      return runner.run(queryPlus, responseContext);
    };
  }

  @Override
  public RowSignature resultArraySignature(final ScanQuery query)
  {
    boolean defaultIsLegacy = scanQueryConfig.isLegacy();
    return query.getRowSignature(defaultIsLegacy);
  }

  /**
   * This batches the fetched {@link ScanResultValue}s which have similar signatures and are consecutives. In best case
   * it would return a single frame, and in the worst case, it would return as many frames as the number of {@link ScanResultValue}
   * passed.
   */
  @Override
  public Optional<Sequence<FrameSignaturePair>> resultsAsFrames(
      final ScanQuery query,
      final Sequence<ScanResultValue> resultSequence,
      MemoryAllocatorFactory memoryAllocatorFactory,
      boolean useNestedForUnknownTypes
  )
  {
    final RowSignature defaultRowSignature = resultArraySignature(query);
    ScanResultValueIterator resultSequenceIterator = new ScanResultValueIterator(resultSequence);

    Iterable<Sequence<FrameSignaturePair>> retVal = () -> new Iterator<Sequence<FrameSignaturePair>>()
    {
      PeekingIterator<ScanResultValue> scanResultValuePeekingIterator = Iterators.peekingIterator(resultSequenceIterator);

      @Override
      public boolean hasNext()
      {
        return scanResultValuePeekingIterator.hasNext();
      }

      @Override
      public Sequence<FrameSignaturePair> next()
      {
        final List<ScanResultValue> batch = new ArrayList<>();
        final ScanResultValue scanResultValue = scanResultValuePeekingIterator.next();
        batch.add(scanResultValue);
        // If the rowSignature is not provided, assume that the scanResultValue can contain any number of the columns
        // that appear in the original scan query
        final RowSignature rowSignature = scanResultValue.getRowSignature() != null
                                          ? scanResultValue.getRowSignature()
                                          : defaultRowSignature;
        while (scanResultValuePeekingIterator.hasNext()) {
          RowSignature nextRowSignature = scanResultValuePeekingIterator.peek().getRowSignature();
          if (nextRowSignature == null) {
            nextRowSignature = defaultRowSignature;
          }
          if (nextRowSignature != null && nextRowSignature.equals(rowSignature)) {
            batch.add(scanResultValuePeekingIterator.next());
          } else {
            break;
          }
        }
        return convertScanResultValuesToFrame(
            batch,
            rowSignature,
            query,
            memoryAllocatorFactory,
            useNestedForUnknownTypes
        );
      }
    };
    return Optional.of(Sequences.concat(retVal).withBaggage(resultSequenceIterator));
  }

  private Sequence<FrameSignaturePair> convertScanResultValuesToFrame(
      List<ScanResultValue> batch,
      RowSignature rowSignature,
      ScanQuery query,
      MemoryAllocatorFactory memoryAllocatorFactory,
      boolean useNestedForUnknownTypes
  )
  {
    Preconditions.checkNotNull(rowSignature, "'rowSignature' must be provided");

    List<Cursor> cursors = new ArrayList<>();
    Closer closer = Closer.create();

    for (ScanResultValue scanResultValue : batch) {
      final List rows = (List) scanResultValue.getEvents();
      final Function<?, Object[]> mapper = getResultFormatMapper(query.getResultFormat(), rowSignature.getColumnNames());
      final Iterable<Object[]> formattedRows = Lists.newArrayList(Iterables.transform(rows, (Function) mapper));

      Pair<Cursor, Closeable> cursorAndCloseable = IterableRowsCursorHelper.getCursorFromIterable(
          formattedRows,
          rowSignature
      );
      Cursor cursor = cursorAndCloseable.lhs;
      Closeable closeable = cursorAndCloseable.rhs;
      cursors.add(cursor);
      // Cursors created from iterators don't have any resources, therefore this is mostly a defensive check
      closer.register(closeable);
    }

    RowSignature modifiedRowSignature = useNestedForUnknownTypes
                                        ? FrameWriterUtils.replaceUnknownTypesWithNestedColumns(rowSignature)
                                        : rowSignature;
    FrameWriterFactory frameWriterFactory = FrameWriters.makeFrameWriterFactory(
        FrameType.COLUMNAR,
        memoryAllocatorFactory,
        modifiedRowSignature,
        new ArrayList<>()
    );


    Cursor concatCursor = new ConcatCursor(cursors);
    Sequence<Frame> frames = FrameCursorUtils.cursorToFrames(
        concatCursor,
        frameWriterFactory
    );

    return frames.map(frame -> new FrameSignaturePair(frame, modifiedRowSignature)).withBaggage(closer);
  }

  @Override
  public Sequence<Object[]> resultsAsArrays(final ScanQuery query, final Sequence<ScanResultValue> resultSequence)
  {
    final Function<?, Object[]> mapper = getResultFormatMapper(query.getResultFormat(), resultArraySignature(query).getColumnNames());

    return resultSequence.flatMap(
        result -> {
          // Generics? Where we're going, we don't need generics.
          final List rows = (List) result.getEvents();
          final Iterable arrays = Iterables.transform(rows, (Function) mapper);
          return Sequences.simple(arrays);
        }
    );
  }

  private Function<?, Object[]> getResultFormatMapper(ScanQuery.ResultFormat resultFormat, List<String> fields)
  {
    Function<?, Object[]> mapper;

    switch (resultFormat) {
      case RESULT_FORMAT_LIST:
        mapper = (Map<String, Object> row) -> {
          final Object[] rowArray = new Object[fields.size()];

          for (int i = 0; i < fields.size(); i++) {
            rowArray[i] = row.get(fields.get(i));
          }

          return rowArray;
        };
        break;
      case RESULT_FORMAT_COMPACTED_LIST:
        mapper = (List<Object> row) -> {
          if (row.size() == fields.size()) {
            return row.toArray();
          } else if (fields.isEmpty()) {
            return new Object[0];
          } else {
            // Uh oh... mismatch in expected and actual field count. I don't think this should happen, so let's
            // throw an exception. If this really does happen, and there's a good reason for it, then we should remap
            // the result row here.
            throw new ISE("Mismatch in expected[%d] vs actual[%s] field count", fields.size(), row.size());
          }
        };
        break;
      default:
        throw new UOE("Unsupported resultFormat for array-based results: %s", resultFormat);
    }
    return mapper;
  }
}
