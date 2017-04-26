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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Function;
import io.druid.data.input.Row;
import io.druid.java.util.common.Cacheable;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NoopLimitSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultLimitSpec.class)
})
public interface LimitSpec extends Cacheable
{
  static LimitSpec nullToNoopLimitSpec(@Nullable LimitSpec limitSpec)
  {
    return (limitSpec == null) ? NoopLimitSpec.instance() : limitSpec;
  }

  /**
   * Returns a function that applies a limit to an input sequence that is assumed to be sorted on dimensions.
   *
   * @param dimensions query dimensions
   * @param aggs       query aggregators
   * @param postAggs   query postAggregators
   *
   * @return limit function
   */
  Function<Sequence<Row>, Sequence<Row>> build(
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggs,
      List<PostAggregator> postAggs
  );

  LimitSpec merge(LimitSpec other);
}
