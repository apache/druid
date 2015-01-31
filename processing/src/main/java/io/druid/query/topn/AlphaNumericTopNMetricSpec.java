/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.topn;

import com.davekoelle.alphanum.AlphanumComparator;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.metamx.common.StringUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class AlphaNumericTopNMetricSpec extends LexicographicTopNMetricSpec
{
  private static final byte CACHE_TYPE_ID = 0x2;

  private final String previousStop;

  @JsonCreator
  public AlphaNumericTopNMetricSpec(
      @JsonProperty("previousStop") String previousStop
  )
  {
    super(previousStop);
    this.previousStop = (previousStop == null) ? "" : previousStop;
  }

  @Override
  public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
    return new AlphanumComparator();
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] previousStopBytes = StringUtils.toUtf8(previousStop);

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
           "previousStop='" + previousStop + '\'' +
           '}';
  }
}
