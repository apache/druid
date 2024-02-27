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
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.dimension.DimensionSpec;

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

  @VisibleForTesting
  public static int countRequiredMergeBufferNum(GroupByQuery query)
  {
    return countRequiredMergeBufferNumWithoutSubtotal(query, 1) + numMergeBuffersNeededForSubtotalsSpec(query);
  }

  @Nullable
  private final List<ReferenceCountingResourceHolder<ByteBuffer>> mergeBufferHolders;
  private final Deque<ByteBuffer> mergeBuffers;

  public GroupByQueryResources()
  {
    this.mergeBufferHolders = null;
    this.mergeBuffers = new ArrayDeque<>();
  }

  public GroupByQueryResources(List<ReferenceCountingResourceHolder<ByteBuffer>> mergeBufferHolders)
  {
    this.mergeBufferHolders = mergeBufferHolders;
    this.mergeBuffers = new ArrayDeque<>(mergeBufferHolders.size());
    mergeBufferHolders.forEach(holder -> mergeBuffers.add(holder.get()));
  }

  /**
   * Get a merge buffer from the pre-acquired resources.
   *
   * @return a resource holder containing a merge buffer
   *
   * @throws IllegalStateException if this resource is initialized with empty merge buffers, or
   *                               there isn't any available merge buffers
   */
  public ResourceHolder<ByteBuffer> getMergeBuffer()
  {
    final ByteBuffer buffer = mergeBuffers.pop();
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
        mergeBuffers.add(buffer);
      }
    };
  }

  @Override
  public void close()
  {
    if (mergeBufferHolders != null) {
      if (mergeBuffers.size() != mergeBufferHolders.size()) {
        log.warn("%d resources are not returned yet", mergeBufferHolders.size() - mergeBuffers.size());
      }
      mergeBufferHolders.forEach(ReferenceCountingResourceHolder::close);
    }
  }
}
