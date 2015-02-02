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

package io.druid.segment.realtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;

import java.util.Arrays;
import java.util.List;

/**
 */
@Deprecated
public class Schema
{
  private final String dataSource;
  private final List<SpatialDimensionSchema> spatialDimensions;
  private final AggregatorFactory[] aggregators;
  private final QueryGranularity indexGranularity;
  private final ShardSpec shardSpec;

  @JsonCreator
  public Schema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("spatialDimensions") List<SpatialDimensionSchema> spatialDimensions,
      @JsonProperty("aggregators") AggregatorFactory[] aggregators,
      @JsonProperty("indexGranularity") QueryGranularity indexGranularity,
      @JsonProperty("shardSpec") ShardSpec shardSpec
  )
  {
    this.dataSource = dataSource;
    this.spatialDimensions = (spatialDimensions == null) ? Lists.<SpatialDimensionSchema>newArrayList()
                                                                     : spatialDimensions;
    this.aggregators = aggregators;
    this.indexGranularity = indexGranularity;
    this.shardSpec = shardSpec == null ? new NoneShardSpec() : shardSpec;

    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(aggregators, "aggregators");
    Preconditions.checkNotNull(indexGranularity, "indexGranularity");
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("spatialDimensions")
  public List<SpatialDimensionSchema> getSpatialDimensions()
  {
    return spatialDimensions;
  }

  @JsonProperty
  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  @JsonProperty
  public QueryGranularity getIndexGranularity()
  {
    return indexGranularity;
  }

  @JsonProperty
  public ShardSpec getShardSpec()
  {
    return shardSpec;
  }

  @Override
  public String toString()
  {
    return "Schema{" +
           "dataSource='" + dataSource + '\'' +
           ", spatialDimensions=" + spatialDimensions +
           ", aggregators=" + (aggregators == null ? null : Arrays.asList(aggregators)) +
           ", indexGranularity=" + indexGranularity +
           ", shardSpec=" + shardSpec +
           '}';
  }
}
