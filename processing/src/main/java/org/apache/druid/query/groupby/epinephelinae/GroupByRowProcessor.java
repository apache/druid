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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryResources;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

/**
 * Utility class that knows how to do higher-level groupBys: i.e. group a {@link Sequence} of {@link ResultRow}
 * originating from a subquery. It uses a buffer provided by a {@link GroupByQueryResources}. The output rows may not
 * be perfectly grouped and will not have PostAggregators applied, so they should be fed into
 * {@link org.apache.druid.query.groupby.GroupingEngine#mergeResults}.
 *
 * This class has two primary uses: processing nested groupBys, and processing subtotals.
 *
 * This class has some similarity to {@link GroupByMergingQueryRunner}, but is different enough that it deserved to
 * be its own class. Some common code between the two classes is in {@link RowBasedGrouperHelper}.
 */
public class GroupByRowProcessor
{
  public interface ResultSupplier extends Closeable
  {
    /**
     * Return a result sequence. Can be called any number of times. When the results are no longer needed,
     * call {@link #close()} (but make sure any result sequences have been fully consumed first!).
     *
     * @param dimensionsToInclude list of dimensions to include, or null to include all dimensions. Used by processing
     *                            of subtotals. If specified, the results will not necessarily be fully grouped.
     */
    Sequence<ResultRow> results(@Nullable List<DimensionSpec> dimensionsToInclude);
  }

  private GroupByRowProcessor()
  {
    // No instantiation
  }

  /**
   * Process the input of sequence "rows" (output by "subquery") based on "query" and returns a {@link ResultSupplier}.
   *
   * In addition to grouping using dimensions and metrics, it will also apply filters (both DimFilter and interval
   * filters).
   *
   * The input sequence is processed synchronously with the call to this method, and result iteration happens lazy upon
   * calls to the {@link ResultSupplier}. Make sure to close it when you're done.
   */
  public static ResultSupplier process(
      final GroupByQuery query,
      final GroupByQuery subquery,
      final Sequence<ResultRow> rows,
      final GroupByQueryConfig config,
      final DruidProcessingConfig processingConfig,
      final GroupByQueryResources resource,
      final ObjectMapper spillMapper,
      final String processingTmpDir,
      final int mergeBufferSize
  )
  {
    final Closer closeOnExit = Closer.create();
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);

    final File temporaryStorageDirectory = new File(
        processingTmpDir,
        StringUtils.format("druid-groupBy-%s_%s", UUID.randomUUID(), query.getId())
    );

    final LimitedTemporaryStorage temporaryStorage = new LimitedTemporaryStorage(
        temporaryStorageDirectory,
        querySpecificConfig.getMaxOnDiskStorage().getBytes()
    );

    closeOnExit.register(temporaryStorage);

    Pair<Grouper<RowBasedKey>, Accumulator<AggregateResult, ResultRow>> pair = RowBasedGrouperHelper.createGrouperAccumulatorPair(
        query,
        subquery,
        querySpecificConfig,
        processingConfig,
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            final ResourceHolder<ByteBuffer> mergeBufferHolder = resource.getMergeBuffer();
            closeOnExit.register(mergeBufferHolder);
            return mergeBufferHolder.get();
          }
        },
        temporaryStorage,
        spillMapper,
        mergeBufferSize
    );
    final Grouper<RowBasedKey> grouper = pair.lhs;
    final Accumulator<AggregateResult, ResultRow> accumulator = pair.rhs;
    closeOnExit.register(grouper);

    final AggregateResult retVal = rows.accumulate(AggregateResult.ok(), accumulator);

    if (!retVal.isOk()) {
      throw new ResourceLimitExceededException(retVal.getReason());
    }

    return new ResultSupplier()
    {
      @Override
      public Sequence<ResultRow> results(@Nullable List<DimensionSpec> dimensionsToInclude)
      {
        return getRowsFromGrouper(query, grouper, dimensionsToInclude);
      }

      @Override
      public void close() throws IOException
      {
        closeOnExit.close();
      }
    };
  }

  private static Sequence<ResultRow> getRowsFromGrouper(
      final GroupByQuery query,
      final Grouper<RowBasedKey> grouper,
      @Nullable List<DimensionSpec> dimensionsToInclude
  )
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<ResultRow, CloseableGrouperIterator<RowBasedKey, ResultRow>>()
        {
          @Override
          public CloseableGrouperIterator<RowBasedKey, ResultRow> make()
          {
            return RowBasedGrouperHelper.makeGrouperIterator(
                grouper,
                query,
                dimensionsToInclude,
                () -> {}
            );
          }

          @Override
          public void cleanup(CloseableGrouperIterator<RowBasedKey, ResultRow> iterFromMake)
          {
            iterFromMake.close();
          }
        }
    );

  }
}
