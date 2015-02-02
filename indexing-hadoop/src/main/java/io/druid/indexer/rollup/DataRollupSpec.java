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

package io.druid.indexer.rollup;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.List;

/**
 * Class uses public fields to work around http://jira.codehaus.org/browse/MSHADE-92
 *
 * Adjust to JsonCreator and final fields when resolved.
 */
public class DataRollupSpec
{
  @JsonProperty
  public List<AggregatorFactory> aggs;

  @JsonProperty
  public QueryGranularity rollupGranularity = QueryGranularity.NONE;

  @JsonProperty
  public int rowFlushBoundary = 500000;

  public DataRollupSpec() {}

  public DataRollupSpec(List<AggregatorFactory> aggs, QueryGranularity rollupGranularity)
  {
    this.aggs = aggs;
    this.rollupGranularity = rollupGranularity;
  }

  public List<AggregatorFactory> getAggs()
  {
    return aggs;
  }

  public QueryGranularity getRollupGranularity()
  {
    return rollupGranularity;
  }
}
