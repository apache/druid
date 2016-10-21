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
import io.druid.query.ordering.StringComparators;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class AlphaNumericTopNMetricSpec extends LexicographicTopNMetricSpec
{
  private static final byte CACHE_TYPE_ID = 0x2;

  protected static Comparator<String> comparator = StringComparators.ALPHANUMERIC;

  @JsonCreator
  public AlphaNumericTopNMetricSpec(
      @JsonProperty("previousStop") String previousStop
  )
  {
    super(previousStop);
  }

  @Override
  public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
    return comparator;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] previousStopBytes = getPreviousStop() == null ? new byte[]{} : StringUtils.toUtf8(getPreviousStop());

    return ByteBuffer.allocate(1 + previousStopBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(previousStopBytes)
                     .array();
  }

  @Override
  public <T> TopNMetricSpecBuilder<T> configureOptimizer(TopNMetricSpecBuilder<T> builder)
  {
    return builder;
  }

  @Override
  public String toString()
  {
    return "AlphaNumericTopNMetricSpec{" +
           "previousStop='" + getPreviousStop() + '\'' +
           '}';
  }
}
