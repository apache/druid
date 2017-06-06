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

package io.druid.client;

import com.google.common.collect.ImmutableMap;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
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

  public ServerType getType()
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

  public ImmutableDruidDataSource getDataSource(String name)
  {
    return dataSources.get(name);
  }

  public Map<String, DataSegment> getSegments()
  {
    return segments;
  }

  @Override
  public String toString()
  {
    // segments is intentionally ignored because it is usually large
    return "ImmutableDruidServer{"
           + "meta='" + metadata
           + "', size='" + currSize
           + "', sources='" + dataSources
           + "'}";
  }
}
