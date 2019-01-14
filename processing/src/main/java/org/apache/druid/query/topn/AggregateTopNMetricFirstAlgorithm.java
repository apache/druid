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

package org.apache.druid.query.topn;

import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 */
public class AggregateTopNMetricFirstAlgorithm implements TopNAlgorithm<int[], TopNParams>
{
  private final StorageAdapter storageAdapter;
  private final TopNQuery query;
  private final NonBlockingPool<ByteBuffer> bufferPool;

  public AggregateTopNMetricFirstAlgorithm(
      StorageAdapter storageAdapter,
      TopNQuery query,
      NonBlockingPool<ByteBuffer> bufferPool
  )
  {
    this.storageAdapter = storageAdapter;
    this.query = query;
    this.bufferPool = bufferPool;
  }

  @Override
  public TopNParams makeInitParams(ColumnSelectorPlus selectorPlus, Cursor cursor)
  {
    return new TopNParams(selectorPlus, cursor, Integer.MAX_VALUE);
  }

  @Override
  public void run(
      TopNParams params,
      TopNResultBuilder resultBuilder,
      int[] ints,
      @Nullable TopNQueryMetrics queryMetrics
  )
  {
    final String metric = query.getTopNMetricSpec().getMetricName(query.getDimensionSpec());
    Pair<List<AggregatorFactory>, List<PostAggregator>> condensedAggPostAggPair =
        AggregatorUtil.condensedAggregators(query.getAggregatorSpecs(), query.getPostAggregatorSpecs(), metric);

    if (condensedAggPostAggPair.lhs.isEmpty() && condensedAggPostAggPair.rhs.isEmpty()) {
      throw new ISE("WTF! Can't find the metric to do topN over?");
    }
    // Run topN for only a single metric
    TopNQuery singleMetricQuery = new TopNQueryBuilder(query)
        .aggregators(condensedAggPostAggPair.lhs)
        .postAggregators(condensedAggPostAggPair.rhs)
        .build();
    final TopNResultBuilder singleMetricResultBuilder = BaseTopNAlgorithm.makeResultBuilder(params, singleMetricQuery);

    PooledTopNAlgorithm singleMetricAlgo = new PooledTopNAlgorithm(storageAdapter, singleMetricQuery, bufferPool);
    PooledTopNAlgorithm.PooledTopNParams singleMetricParam = null;
    int[] dimValSelector;
    try {
      singleMetricParam = singleMetricAlgo.makeInitParams(params.getSelectorPlus(), params.getCursor());
      singleMetricAlgo.run(
          singleMetricParam,
          singleMetricResultBuilder,
          null,
          null // Don't collect metrics during the preparation run.
      );

      // Get only the topN dimension values
      dimValSelector = getDimValSelectorForTopNMetric(singleMetricParam, singleMetricResultBuilder);
    }
    finally {
      singleMetricAlgo.cleanup(singleMetricParam);
    }

    PooledTopNAlgorithm allMetricAlgo = new PooledTopNAlgorithm(storageAdapter, query, bufferPool);
    PooledTopNAlgorithm.PooledTopNParams allMetricsParam = null;
    try {
      // Run topN for all metrics for top N dimension values
      allMetricsParam = allMetricAlgo.makeInitParams(params.getSelectorPlus(), params.getCursor());
      allMetricAlgo.run(
          allMetricsParam,
          resultBuilder,
          dimValSelector,
          queryMetrics
      );
    }
    finally {
      allMetricAlgo.cleanup(allMetricsParam);
    }
  }

  @Override
  public void cleanup(TopNParams params)
  {
  }

  private int[] getDimValSelectorForTopNMetric(TopNParams params, TopNResultBuilder resultBuilder)
  {
    if (params.getCardinality() < 0) {
      throw new UnsupportedOperationException("Cannot operate on a dimension with unknown cardinality");
    }

    int[] dimValSelector = new int[params.getCardinality()];
    Arrays.fill(dimValSelector, SKIP_POSITION_VALUE);

    Iterator<DimValHolder> dimValIter = resultBuilder.getTopNIterator();
    while (dimValIter.hasNext()) {
      int dimValIndex = (Integer) dimValIter.next().getDimValIndex();
      dimValSelector[dimValIndex] = INIT_POSITION_VALUE;
    }

    return dimValSelector;
  }
}
