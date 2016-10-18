/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.ordering.StringComparators;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

/**
 */
public class LexicographicTopNMetricSpec implements TopNMetricSpec
{
  private static final byte CACHE_TYPE_ID = 0x1;

  private static Comparator<String> comparator = StringComparators.LEXICOGRAPHIC;

  private final String previousStop;

  @JsonCreator
  public LexicographicTopNMetricSpec(
      @JsonProperty("previousStop") String previousStop
  )
  {
    this.previousStop = previousStop;
  }

  @Override
  public void verifyPreconditions(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
  }

  @JsonProperty
  public String getPreviousStop()
  {
    return previousStop;
  }


  @Override
  public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
    return comparator;
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
    return new TopNLexicographicResultBuilder(
        timestamp,
        dimSpec,
        threshold,
        previousStop,
        comparator,
        aggFactories
    );
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] previousStopBytes = previousStop == null ? new byte[]{} : StringUtils.toUtf8(previousStop);

    return ByteBuffer.allocate(1 + previousStopBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(previousStopBytes)
                     .array();
  }

  @Override
  public <T> TopNMetricSpecBuilder<T> configureOptimizer(TopNMetricSpecBuilder<T> builder)
  {
    builder.skipTo(previousStop);
    builder.ignoreAfterThreshold();
    return builder;
  }

  @Override
  public void initTopNAlgorithmSelector(TopNAlgorithmSelector selector)
  {
    selector.setAggregateAllMetrics(true);
  }

  @Override
  public String getMetricName(DimensionSpec dimSpec)
  {
    return dimSpec.getOutputName();
  }

  @Override
  public boolean canBeOptimizedUnordered()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "LexicographicTopNMetricSpec{" +
           "previousStop='" + previousStop + '\'' +
           '}';
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

    LexicographicTopNMetricSpec that = (LexicographicTopNMetricSpec) o;

    if (previousStop != null ? !previousStop.equals(that.previousStop) : that.previousStop != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return previousStop != null ? previousStop.hashCode() : 0;
  }
}
