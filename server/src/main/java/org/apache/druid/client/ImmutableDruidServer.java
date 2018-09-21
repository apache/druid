/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

/**
 * This class should not be subclassed, it isn't declared final only to make it possible to mock the class with EasyMock
 * in tests.
 *
 * @see DruidServer - a mutable counterpart of this class
 */
public class ImmutableDruidServer
{
  private final DruidServerMetadata metadata;
  private final long currSize;
  private final ImmutableMap<String, ImmutableDruidDataSource> dataSources;
  private final int totalSegments;

  public ImmutableDruidServer(
      DruidServerMetadata metadata,
      long currSize,
      ImmutableMap<String, ImmutableDruidDataSource> dataSources,
      int totalSegments
  )
  {
    this.metadata = Preconditions.checkNotNull(metadata);
    this.currSize = currSize;
    this.dataSources = dataSources;
    this.totalSegments = totalSegments;
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

  public DataSegment getSegment(SegmentId segmentId)
  {
    ImmutableDruidDataSource dataSource = dataSources.get(segmentId.getDataSource());
    if (dataSource == null) {
      return null;
    }
    return dataSource.getSegment(segmentId);
  }

  public Iterable<ImmutableDruidDataSource> getDataSources()
  {
    return dataSources.values();
  }

  public ImmutableDruidDataSource getDataSource(String name)
  {
    return dataSources.get(name);
  }

  public Collection<DataSegment> getSegments()
  {
    return new AbstractCollection<DataSegment>()
    {
      @Override
      public Iterator<DataSegment> iterator()
      {
        return dataSources.values().stream().flatMap(dataSource -> dataSource.getSegments().stream()).iterator();
      }

      @Override
      public int size()
      {
        return totalSegments;
      }
    };
  }

  public String getURL()
  {
    if (metadata.getHostAndTlsPort() != null) {
      return StringUtils.nonStrictFormat("https://%s", metadata.getHostAndTlsPort());
    } else {
      return StringUtils.nonStrictFormat("http://%s", metadata.getHostAndPort());
    }
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ImmutableDruidServer that = (ImmutableDruidServer) o;

    return metadata.equals(that.metadata);
  }

  @Override
  public int hashCode()
  {
    return metadata.hashCode();
  }
}
