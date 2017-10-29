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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;

import java.util.List;

/**
 */
public final class NoopLimitSpec implements LimitSpec
{
  private static final byte CACHE_KEY = 0x0;

  public static final NoopLimitSpec INSTANCE = new NoopLimitSpec();

  @JsonCreator
  public static NoopLimitSpec instance()
  {
    return INSTANCE;
  }

  private NoopLimitSpec()
  {
  }

  @Override
  public Function<Sequence<Row>, Sequence<Row>> build(
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggs,
      List<PostAggregator> postAggs
  )
  {
    return Functions.identity();
  }

  @Override
  public LimitSpec merge(LimitSpec other)
  {
    return other;
  }

  @Override
  public String toString()
  {
    return "NoopLimitSpec";
  }

  @Override
  public boolean equals(Object other)
  {
    return (other instanceof NoopLimitSpec);
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[]{CACHE_KEY};
  }
}
