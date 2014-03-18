/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.topn;

import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import io.druid.collections.StupidPool;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 */
public class AggregateTopNMetricFirstAlgorithm implements TopNAlgorithm<int[], TopNParams>
{
  private final Capabilities capabilities;
  private final TopNQuery query;
  private final Comparator<?> comparator;
  private final StupidPool<ByteBuffer> bufferPool;

  public AggregateTopNMetricFirstAlgorithm(
      Capabilities capabilities,
      TopNQuery query,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    this.capabilities = capabilities;
    this.query = query;
    this.comparator = query.getTopNMetricSpec()
                           .getComparator(query.getAggregatorSpecs(), query.getPostAggregatorSpecs());
    this.bufferPool = bufferPool;
  }


  @Override
  public TopNParams makeInitParams(
      DimensionSelector dimSelector, Cursor cursor
  )
  {
    return new TopNParams(dimSelector, cursor, dimSelector.getValueCardinality(), Integer.MAX_VALUE);
  }

  @Override
  public TopNResultBuilder makeResultBuilder(TopNParams params)
  {
    return query.getTopNMetricSpec().getResultBuilder(
        params.getCursor().getTime(), query.getDimensionSpec(), query.getThreshold(), comparator
    );
  }

  @Override
  public void run(
      TopNParams params, TopNResultBuilder resultBuilder, int[] ints
  )
  {
    final TopNResultBuilder singleMetricResultBuilder = makeResultBuilder(params);
    final String metric;
    // ugly
    TopNMetricSpec spec = query.getTopNMetricSpec();
    if (spec instanceof InvertedTopNMetricSpec
        && ((InvertedTopNMetricSpec) spec).getDelegate() instanceof NumericTopNMetricSpec) {
      metric = ((NumericTopNMetricSpec) ((InvertedTopNMetricSpec) spec).getDelegate()).getMetric();
    } else if (spec instanceof NumericTopNMetricSpec) {
      metric = ((NumericTopNMetricSpec) query.getTopNMetricSpec()).getMetric();
    } else {
      throw new ISE("WTF?! We are in AggregateTopNMetricFirstAlgorithm with a [%s] spec", spec.getClass().getName());
    }

    // Find either the aggregator or post aggregator to do the topN over
    List<AggregatorFactory> condensedAggs = Lists.newArrayList();
    for (AggregatorFactory aggregatorSpec : query.getAggregatorSpecs()) {
      if (aggregatorSpec.getName().equalsIgnoreCase(metric)) {
        condensedAggs.add(aggregatorSpec);
        break;
      }
    }
    List<PostAggregator> condensedPostAggs = Lists.newArrayList();
    if (condensedAggs.isEmpty()) {
      for (PostAggregator postAggregator : query.getPostAggregatorSpecs()) {
        if (postAggregator.getName().equalsIgnoreCase(metric)) {
          condensedPostAggs.add(postAggregator);

          // Add all dependent metrics
          for (AggregatorFactory aggregatorSpec : query.getAggregatorSpecs()) {
            if (postAggregator.getDependentFields().contains(aggregatorSpec.getName())) {
              condensedAggs.add(aggregatorSpec);
            }
          }
          break;
        }
      }
    }
    if (condensedAggs.isEmpty() && condensedPostAggs.isEmpty()) {
      throw new ISE("WTF! Can't find the metric to do topN over?");
    }

    // Run topN for only a single metric
    TopNQuery singleMetricQuery = new TopNQueryBuilder().copy(query)
                                                        .aggregators(condensedAggs)
                                                        .postAggregators(condensedPostAggs)
                                                        .build();

    PooledTopNAlgorithm singleMetricAlgo = new PooledTopNAlgorithm(capabilities, singleMetricQuery, bufferPool);
    PooledTopNAlgorithm.PooledTopNParams singleMetricParam = null;
    int[] dimValSelector = null;
    try {
      singleMetricParam = singleMetricAlgo.makeInitParams(params.getDimSelector(), params.getCursor());
      singleMetricAlgo.run(
          singleMetricParam,
          singleMetricResultBuilder,
          null
      );

      // Get only the topN dimension values
      dimValSelector = getDimValSelectorForTopNMetric(singleMetricParam, singleMetricResultBuilder);
    }
    finally {
      if (singleMetricParam != null) {
        singleMetricAlgo.cleanup(singleMetricParam);
      }
    }

    PooledTopNAlgorithm allMetricAlgo = new PooledTopNAlgorithm(capabilities, query, bufferPool);
    PooledTopNAlgorithm.PooledTopNParams allMetricsParam = null;
    try {
      // Run topN for all metrics for top N dimension values
      allMetricsParam = allMetricAlgo.makeInitParams(params.getDimSelector(), params.getCursor());
      allMetricAlgo.run(
          allMetricsParam,
          resultBuilder,
          dimValSelector
      );
    }
    finally {
      if (allMetricsParam != null) {
        allMetricAlgo.cleanup(allMetricsParam);
      }
    }
  }

  @Override
  public void cleanup(TopNParams params)
  {
  }

  private int[] getDimValSelectorForTopNMetric(TopNParams params, TopNResultBuilder resultBuilder)
  {
    int[] dimValSelector = new int[params.getDimSelector().getValueCardinality()];
    Arrays.fill(dimValSelector, SKIP_POSITION_VALUE);

    Iterator<DimValHolder> dimValIter = resultBuilder.getTopNIterator();
    while (dimValIter.hasNext()) {
      int dimValIndex = (Integer) dimValIter.next().getDimValIndex();
      dimValSelector[dimValIndex] = INIT_POSITION_VALUE;
    }

    return dimValSelector;
  }
}
