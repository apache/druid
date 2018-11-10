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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
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
      final List<AggregatorFactory> aggregatorSpecs,
      final List<PostAggregator> postAggregatorSpecs
  )
  {
    return Comparator.nullsFirst(delegate.getComparator(aggregatorSpecs, postAggregatorSpecs).reversed());
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
    return delegate.getResultBuilder(
        timestamp,
        dimSpec,
        threshold,
        comparator,
        aggFactories,
        postAggs
    );
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
    if (!canBeOptimizedUnordered()) {
      return builder;
    }
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
  public boolean canBeOptimizedUnordered()
  {
    return delegate.canBeOptimizedUnordered();
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

  @Override
  public String toString()
  {
    return "InvertedTopNMetricSpec{" +
           "delegate=" + delegate +
           '}';
  }
}
