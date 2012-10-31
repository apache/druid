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

package com.metamx.druid.merge;

import java.util.List;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;

/**
 */
public class ClientDefaultMergeQuery implements ClientMergeQuery
{
  private final String dataSource;
  private final List<DataSegment> segments;
  private final List<AggregatorFactory> aggregators;

  @JsonCreator
  public ClientDefaultMergeQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<DataSegment> segments,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregators
  )
  {
    this.dataSource = dataSource;
    this.segments = segments;
    this.aggregators = aggregators;

  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Override
  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public List<AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  @Override
  public String toString()
  {
    return "ClientDefaultMergeQuery{" +
           "dataSource='" + dataSource + '\'' +
           ", segments=" + segments +
           ", aggregators=" + aggregators +
           '}';
  }
}
