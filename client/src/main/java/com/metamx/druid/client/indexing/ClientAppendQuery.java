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

package com.metamx.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.druid.client.DataSegment;

import java.util.List;

/**
 */
public class ClientAppendQuery
{
  private final String dataSource;
  private final List<DataSegment> segments;

  @JsonCreator
  public ClientAppendQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<DataSegment> segments
  )
  {
    this.dataSource = dataSource;
    this.segments = segments;
  }

  @JsonProperty
  public String getType()
  {
    return "append";
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @Override
  public String toString()
  {
    return "ClientAppendQuery{" +
           "dataSource='" + dataSource + '\'' +
           ", segments=" + segments +
           '}';
  }
}
