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
