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
import com.metamx.common.guava.Comparators;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

/**
 */
public class InvertedTopNMetricSpec implements TopNMetricSpec
{
  private static final byte CACHE_TYPE_ID = 0x3;
  private final TopNMetricSpec delegate;

  @JsonCreator
  public InvertedTopNMetricSpec(
      @JsonProperty("metric") TopNMetricSpec delegate
  )
  {
    this.delegate = delegate;
  }

  @Override
  public void verifyPreconditions(
      List<AggregatorFactory> aggregatorSpecs,
      List<PostAggregator> postAggregatorSpecs
  )
  {
    delegate.verifyPreconditions(aggregatorSpecs, postAggregatorSpecs);
  }

  @JsonProperty("metric")
  public TopNMetricSpec getDelegate()
  {
    return delegate;
  }

  @Override
  public Comparator getComparator(
      List<AggregatorFactory> aggregatorSpecs,
      List<PostAggregator> postAggregatorSpecs
  )
  {
    return Comparators.inverse(delegate.getComparator(aggregatorSpecs, postAggregatorSpecs));
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
    return delegate.getResultBuilder(timestamp, dimSpec, threshold, comparator, aggFactories, postAggs);
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] cacheKey = delegate.getCacheKey();

    return ByteBuffer.allocate(1 + cacheKey.length).put(CACHE_TYPE_ID).put(cacheKey).array();
  }

  @Override
  public <T> TopNMetricSpecBuilder<T> configureOptimizer(TopNMetricSpecBuilder<T> builder)
  {
    return delegate.configureOptimizer(builder);
  }

  @Override
  public void initTopNAlgorithmSelector(TopNAlgorithmSelector selector)
  {
    delegate.initTopNAlgorithmSelector(selector);
  }

  @Override
  public String getMetricName(DimensionSpec dimSpec)
  {
    return delegate.getMetricName(dimSpec);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InvertedTopNMetricSpec that = (InvertedTopNMetricSpec) o;

    if (delegate != null ? !delegate.equals(that.delegate) : that.delegate != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return delegate != null ? delegate.hashCode() : 0;
  }
}
