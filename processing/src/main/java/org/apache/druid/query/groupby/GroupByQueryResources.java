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

package org.apache.druid.query.groupby;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.Segment;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class contains resources required for a groupBy query execution.
 * Currently, it contains only merge buffers, but any additional resources can be added in the future.
 *
 * It contains merge buffers for the execution of
 * a) {@link GroupByQueryQueryToolChest#mergeResults(QueryRunner)} - Required for merging the results of the subqueries
 *    and the subtotals.
 * b) {@link org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunner} - Required for merging the results
 *    of the individual runners created by {@link GroupByQueryRunnerFactory#createRunner(Segment)}
 *
 * The resources should be acquired once throughout the execution of the query (with caveats such as union being treated
 * as separate queries on the data servers) or it should release all the resources before re-acquiring them (if needed),
 * to prevent deadlocks.
 */
public class GroupByQueryResources implements Closeable
{
  private static final Logger log = new Logger(GroupByQueryResources.class);

  private static final int MAX_MERGE_BUFFER_NUM_WITHOUT_SUBTOTAL = 2;

  private static int countRequiredMergeBufferNumWithoutSubtotal(Query query, int foundNum)
  {
    // Note: A broker requires merge buffers for processing the groupBy layers beyond the inner-most one.
    // For example, the number of required merge buffers for a nested groupBy (groupBy -> groupBy -> table) is 1.
    // If the broker processes an outer groupBy which reads input from an inner groupBy,
    // it requires two merge buffers for inner and outer groupBys to keep the intermediate result of inner groupBy
    // until the outer groupBy processing completes.
    // This is same for subsequent groupBy layers, and thus the maximum number of required merge buffers becomes 2.

    final DataSource dataSource = query.getDataSource();
    if (foundNum == MAX_MERGE_BUFFER_NUM_WITHOUT_SUBTOTAL + 1 || !(dataSource instanceof QueryDataSource)) {
      return foundNum - 1;
    } else {
      return countRequiredMergeBufferNumWithoutSubtotal(((QueryDataSource) dataSource).getQuery(), foundNum + 1);
    }
  }

  private static int numMergeBuffersNeededForSubtotalsSpec(GroupByQuery query)
  {
    List<List<String>> subtotalSpecs = query.getSubtotalsSpec();
    final DataSource dataSource = query.getDataSource();
    int numMergeBuffersNeededForSubQuerySubtotal = 0;
    if (dataSource instanceof QueryDataSource) {
      Query<?> subQuery = ((QueryDataSource) dataSource).getQuery();
      if (subQuery instanceof GroupByQuery) {
        numMergeBuffersNeededForSubQuerySubtotal = numMergeBuffersNeededForSubtotalsSpec((GroupByQuery) subQuery);
      }

    }
    if (subtotalSpecs == null || subtotalSpecs.size() == 0) {
      return numMergeBuffersNeededForSubQuerySubtotal;
    }

    List<String> queryDimOutputNames = query.getDimensions().stream().map(DimensionSpec::getOutputName).collect(
        Collectors.toList());
    for (List<String> subtotalSpec : subtotalSpecs) {
      if (!Utils.isPrefix(subtotalSpec, queryDimOutputNames)) {
        return 2;
      }
    }

    return Math.max(1, numMergeBuffersNeededForSubQuerySubtotal);
  }

  /**
   * Counts the number of merge buffers required for {@link GroupByQueryQueryToolChest#mergeResults}. For a given query,
   * it is dependent on the structure of the group by query.
   */
  @VisibleForTesting
  public static int countRequiredMergeBufferNumForToolchestMerge(GroupByQuery query)
  {
    return countRequiredMergeBufferNumWithoutSubtotal(query, 1) + numMergeBuffersNeededForSubtotalsSpec(query);
  }

  /**
   * Count the number of merge buffers required for {@link org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunner}
   * It can be either 1 or 2, depending on the query's config
   */
  public static int countRequiredMergeBufferNumForMergingQueryRunner(GroupByQueryConfig config, GroupByQuery query)
  {
    GroupByQueryConfig querySpecificConfig = config.withOverrides(query);
    return querySpecificConfig.getNumParallelCombineThreads() > 1 ? 2 : 1;
  }

  @Nullable
  private final List<ReferenceCountingResourceHolder<ByteBuffer>> toolchestMergeBuffersHolders;

  private final Deque<ByteBuffer> toolchestMergeBuffers = new ArrayDeque<>();

  @Nullable
  private final List<ReferenceCountingResourceHolder<ByteBuffer>> mergingQueryRunnerMergeBuffersHolders;

  private final Deque<ByteBuffer> mergingQueryRunnerMergeBuffers = new ArrayDeque<>();

  public GroupByQueryResources(
      @Nullable List<ReferenceCountingResourceHolder<ByteBuffer>> toolchestMergeBuffersHolders,
      @Nullable List<ReferenceCountingResourceHolder<ByteBuffer>> mergingQueryRunnerMergeBuffersHolders
  )
  {
    this.toolchestMergeBuffersHolders = toolchestMergeBuffersHolders;
    if (toolchestMergeBuffersHolders != null) {
      toolchestMergeBuffersHolders.forEach(holder -> toolchestMergeBuffers.add(holder.get()));
    }
    this.mergingQueryRunnerMergeBuffersHolders = mergingQueryRunnerMergeBuffersHolders;
    if (mergingQueryRunnerMergeBuffersHolders != null) {
      mergingQueryRunnerMergeBuffersHolders.forEach(holder -> mergingQueryRunnerMergeBuffers.add(holder.get()));
    }
  }

  /**
   * Returns a merge buffer associate with the {@link GroupByQueryQueryToolChest#mergeResults}
   */
  public ResourceHolder<ByteBuffer> getToolchestMergeBuffer()
  {
    return getMergeBuffer(toolchestMergeBuffers);
  }

  /**
   * Returns a merge buffer associated with the {@link org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunner}
   */
  public ResourceHolder<ByteBuffer> getMergingQueryRunnerMergeBuffer()
  {
    return getMergeBuffer(mergingQueryRunnerMergeBuffers);
  }

  /**
   * Returns the number of the currently unused merge buffers reserved for {@link org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunner}
   */
  public int getNumMergingQueryRunnerMergeBuffers()
  {
    return mergingQueryRunnerMergeBuffers.size();
  }

  /**
   * Get a merge buffer from the pre-acquired resources.
   *
   * @return a resource holder containing a merge buffer
   */
  private static ResourceHolder<ByteBuffer> getMergeBuffer(Deque<ByteBuffer> acquiredBufferPool)
  {
    if (acquiredBufferPool.size() == 0) {
      throw DruidException.defensive("Insufficient free merge buffers present.");
    }
    final ByteBuffer buffer = acquiredBufferPool.pop();
    return new ResourceHolder<ByteBuffer>()
    {
      @Override
      public ByteBuffer get()
      {
        return buffer;
      }

      @Override
      public void close()
      {
        acquiredBufferPool.add(buffer);
      }
    };
  }

  /**
   * Closes the query resource. It must be called to release back the acquired merge buffers back into the global
   * merging pool from where all the merge buffers are acquired. The references to the merge buffers will become invalid
   * once this method is called. The user must ensure that the callers are not using the stale references to the merge
   * buffers after this method is called, as reading them would give incorrect results and writing there would interfere
   * with other users of the merge buffers
   */
  @Override
  public void close()
  {
    if (toolchestMergeBuffersHolders != null) {
      if (toolchestMergeBuffers.size() != toolchestMergeBuffersHolders.size()) {
        log.warn(
            "%d toolchest merge buffers are not returned yet",
            toolchestMergeBuffersHolders.size() - toolchestMergeBuffers.size()
        );
      }
      toolchestMergeBuffersHolders.forEach(ReferenceCountingResourceHolder::close);
    }

    if (mergingQueryRunnerMergeBuffersHolders != null) {
      if (mergingQueryRunnerMergeBuffers.size() != mergingQueryRunnerMergeBuffersHolders.size()) {
        log.warn(
            "%d merging query runner merge buffers are not returned yet",
            mergingQueryRunnerMergeBuffersHolders.size() - mergingQueryRunnerMergeBuffers.size()
        );
      }
      mergingQueryRunnerMergeBuffersHolders.forEach(ReferenceCountingResourceHolder::close);
    }
  }
}
