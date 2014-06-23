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

package io.druid.client;

import com.google.common.collect.ImmutableMap;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

import java.util.Map;

/**
 */
public class ImmutableDruidServer
{
  private final DruidServerMetadata metadata;
  private final long currSize;
  private final ImmutableMap<String, ImmutableDruidDataSource> dataSources;
  private final ImmutableMap<String, DataSegment> segments;

  public ImmutableDruidServer(
      DruidServerMetadata metadata,
      long currSize,
      ImmutableMap<String, ImmutableDruidDataSource> dataSources,
      ImmutableMap<String, DataSegment> segments
  )
  {
    this.metadata = metadata;
    this.currSize = currSize;
    this.segments = segments;
    this.dataSources = dataSources;
  }

  public String getName()
  {
    return metadata.getName();
  }

  public DruidServerMetadata getMetadata()
  {
    return metadata;
  }

  public String getHost()
  {
    return metadata.getHost();
  }

  public long getCurrSize()
  {
    return currSize;
  }

  public long getMaxSize()
  {
    return metadata.getMaxSize();
  }

  public String getType()
  {
    return metadata.getType();
  }

  public String getTier()
  {
    return metadata.getTier();
  }

  public int getPriority()
  {
    return metadata.getPriority();
  }

  public DataSegment getSegment(String segmentName)
  {
    return segments.get(segmentName);
  }

  public Iterable<ImmutableDruidDataSource> getDataSources()
  {
    return dataSources.values();
  }

  public Map<String, DataSegment> getSegments()
  {
    return segments;
  }
}
