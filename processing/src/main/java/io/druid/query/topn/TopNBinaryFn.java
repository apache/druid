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

import com.metamx.common.guava.nary.BinaryFn;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNBinaryFn implements BinaryFn<Result<TopNResultValue>, Result<TopNResultValue>, Result<TopNResultValue>>
{
  private final TopNResultMerger merger;
  private final DimensionSpec dimSpec;
  private final QueryGranularity gran;
  private final String dimension;
  private final TopNMetricSpec topNMetricSpec;
  private final int threshold;
  private final List<AggregatorFactory> aggregations;
  private final List<PostAggregator> postAggregations;
  private final Comparator comparator;

  public TopNBinaryFn(
      final TopNResultMerger merger,
      final QueryGranularity granularity,
      final DimensionSpec dimSpec,
      final TopNMetricSpec topNMetricSpec,
      final int threshold,
      final List<AggregatorFactory> aggregatorSpecs,
      final List<PostAggregator> postAggregatorSpecs
  )
  {
    this.merger = merger;
    this.dimSpec = dimSpec;
    this.gran = granularity;
    this.topNMetricSpec = topNMetricSpec;
    this.threshold = threshold;
    this.aggregations = aggregatorSpecs;

    this.postAggregations = AggregatorUtil.pruneDependentPostAgg(
        postAggregatorSpecs,
        topNMetricSpec.getMetricName(dimSpec)
    );

    this.dimension = dimSpec.getOutputName();
    this.comparator = topNMetricSpec.getComparator(aggregatorSpecs, postAggregatorSpecs);
  }

  @Override
  public Result<TopNResultValue> apply(Result<TopNResultValue> arg1, Result<TopNResultValue> arg2)
  {
    if (arg1 == null) {
      return merger.getResult(arg2, comparator);
    }
    if (arg2 == null) {
      return merger.getResult(arg1, comparator);
    }

    Map<String, DimensionAndMetricValueExtractor> retVals = new LinkedHashMap<>();

    TopNResultValue arg1Vals = arg1.getValue();
    TopNResultValue arg2Vals = arg2.getValue();

    for (DimensionAndMetricValueExtractor arg1Val : arg1Vals) {
      retVals.put(arg1Val.getStringDimensionValue(dimension), arg1Val);
    }
    for (DimensionAndMetricValueExtractor arg2Val : arg2Vals) {
      final String dimensionValue = arg2Val.getStringDimensionValue(dimension);
      DimensionAndMetricValueExtractor arg1Val = retVals.get(dimensionValue);

      if (arg1Val != null) {
        // size of map = aggregator + topNDim + postAgg (If sorting is done on post agg field)
        Map<String, Object> retVal = new LinkedHashMap<>(aggregations.size() + 2);

        retVal.put(dimension, dimensionValue);
        for (AggregatorFactory factory : aggregations) {
          final String metricName = factory.getName();
          retVal.put(metricName, factory.combine(arg1Val.getMetric(metricName), arg2Val.getMetric(metricName)));
        }

        for (PostAggregator pf : postAggregations) {
          retVal.put(pf.getName(), pf.compute(retVal));
        }

        retVals.put(dimensionValue, new DimensionAndMetricValueExtractor(retVal));
      } else {
        retVals.put(dimensionValue, arg2Val);
      }
    }

    final DateTime timestamp;
    if (gran instanceof AllGranularity) {
      timestamp = arg1.getTimestamp();
    } else {
      timestamp = gran.toDateTime(gran.truncate(arg1.getTimestamp().getMillis()));
    }

    TopNResultBuilder bob = topNMetricSpec.getResultBuilder(
        timestamp,
        dimSpec,
        threshold,
        comparator,
        aggregations,
        postAggregations
    );
    for (DimensionAndMetricValueExtractor extractor : retVals.values()) {
      bob.addEntry(extractor);
    }
    return bob.build();
  }
}
