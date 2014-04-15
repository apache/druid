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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

/**
 */
public class NumericTopNMetricSpec implements TopNMetricSpec
{
  private static final byte CACHE_TYPE_ID = 0x0;

  private final String metric;

  @JsonCreator
  public NumericTopNMetricSpec(
      @JsonProperty("metric") String metric
  )
  {
    this.metric = metric;
  }

  @Override
  public void verifyPreconditions(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
    Preconditions.checkNotNull(metric, "metric can't be null");
    Preconditions.checkNotNull(aggregatorSpecs, "aggregations cannot be null");
    Preconditions.checkArgument(aggregatorSpecs.size() > 0, "Must have at least one AggregatorFactory");

    final AggregatorFactory aggregator = Iterables.tryFind(
        aggregatorSpecs,
        new Predicate<AggregatorFactory>()
        {
          @Override
          public boolean apply(AggregatorFactory input)
          {
            return input.getName().equals(metric);
          }
        }
    ).orNull();

    final PostAggregator postAggregator = Iterables.tryFind(
        postAggregatorSpecs,
        new Predicate<PostAggregator>()
        {
          @Override
          public boolean apply(PostAggregator input)
          {
            return input.getName().equals(metric);
          }
        }
    ).orNull();

    Preconditions.checkArgument(
        aggregator != null || postAggregator != null,
        "Must have an AggregatorFactory or PostAggregator for metric[%s], gave[%s] and [%s]",
        metric,
        aggregatorSpecs,
        postAggregatorSpecs
    );
  }

  @JsonProperty
  public String getMetric()
  {
    return metric;
  }

  @Override
  public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
    Comparator comp = null;
    for (AggregatorFactory factory : aggregatorSpecs) {
      if (metric.equals(factory.getName())) {
        comp = factory.getComparator();
        break;
      }
    }
    for (PostAggregator pf : postAggregatorSpecs) {
      if (metric.equals(pf.getName())) {
        comp = pf.getComparator();
        break;
      }
    }

    return comp;
  }

  @Override
  public TopNResultBuilder getResultBuilder(
      DateTime timestamp,
      DimensionSpec dimSpec,
      int threshold,
      Comparator comparator,
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  )
  {
    return new TopNNumericResultBuilder(timestamp, dimSpec, metric, threshold, comparator, aggFactories, postAggs);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] metricBytes = metric.getBytes(Charsets.UTF_8);

    return ByteBuffer.allocate(1 + metricBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(metricBytes)
                     .array();
  }

  @Override
  public <T> TopNMetricSpecBuilder<T> configureOptimizer(TopNMetricSpecBuilder<T> builder)
  {
    return builder;
  }

  @Override
  public void initTopNAlgorithmSelector(TopNAlgorithmSelector selector)
  {
    selector.setAggregateTopNMetricFirst(true);
  }

  @Override
  public String getMetricName(DimensionSpec dimSpec)
  {
    return metric;
  }

  @Override
  public String toString()
  {
    return "NumericTopNMetricSpec{" +
           "metric='" + metric + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NumericTopNMetricSpec that = (NumericTopNMetricSpec) o;

    if (metric != null ? !metric.equals(that.metric) : that.metric != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    return metric != null ? metric.hashCode() : 0;
  }
}
