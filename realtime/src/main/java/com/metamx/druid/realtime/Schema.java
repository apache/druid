/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.realtime;

import com.google.common.base.Preconditions;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.index.v1.IndexGranularity;
import com.metamx.druid.shard.NoneShardSpec;
import com.metamx.druid.shard.ShardSpec;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Arrays;

/**
 */
public class Schema
{
  private final String dataSource;
  private final AggregatorFactory[] aggregators;
  private final QueryGranularity indexGranularity;
  private final ShardSpec shardSpec;

  @JsonCreator
  public Schema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("aggregators") AggregatorFactory[] aggregators,
      @JsonProperty("indexGranularity") QueryGranularity indexGranularity,
      @JsonProperty("shardSpec") ShardSpec shardSpec
  )
  {
    this.dataSource = dataSource;
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
           ", aggregators=" + (aggregators == null ? null : Arrays.asList(aggregators)) +
           ", indexGranularity=" + indexGranularity +
           '}';
  }
}
