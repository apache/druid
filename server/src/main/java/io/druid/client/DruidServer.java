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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class DruidServer implements Comparable
{
  public static final int DEFAULT_PRIORITY = 0;
  public static final int DEFAULT_NUM_REPLICANTS = 2;
  public static final String DEFAULT_TIER = "_default_tier";

  private static final Logger log = new Logger(DruidServer.class);

  private final Object lock = new Object();

  private final ConcurrentMap<String, DruidDataSource> dataSources;
  private final ConcurrentMap<String, DataSegment> segments;

  private final DruidServerMetadata metadata;

  private volatile long currSize;

  public DruidServer(
      DruidNode node,
      DruidServerConfig config,
      ServerType type
  )
  {
    this(
        node.getHostAndPortToUse(),
        node.getHostAndPort(),
        node.getHostAndTlsPort(),
        config.getMaxSize(),
        type,
        config.getTier(),
        DEFAULT_PRIORITY
    );
  }

  @JsonCreator
  public DruidServer(
      @JsonProperty("name") String name,
      @JsonProperty("host") String hostAndPort,
      @JsonProperty("hostAndTlsPort") String hostAndTlsPort,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") ServerType type,
      @JsonProperty("tier") String tier,
      @JsonProperty("priority") int priority
  )
  {
    this.metadata = new DruidServerMetadata(name, hostAndPort, hostAndTlsPort, maxSize, type, tier, priority);

    this.dataSources = new ConcurrentHashMap<String, DruidDataSource>();
    this.segments = new ConcurrentHashMap<String, DataSegment>();
  }

  @JsonProperty
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
    return getHostAndTlsPort() != null ? getHostAndTlsPort() : getHostAndPort();
  }

  @JsonProperty("host")
  public String getHostAndPort()
  {
    return metadata.getHostAndPort();
  }

  @JsonProperty
  public String getHostAndTlsPort()
  {
    return metadata.getHostAndTlsPort();
  }

  public long getCurrSize()
  {
    return currSize;
  }

  @JsonProperty
  public long getMaxSize()
  {
    return metadata.getMaxSize();
  }

  @JsonProperty
  public ServerType getType()
  {
    return metadata.getType();
  }

  @JsonProperty
  public String getTier()
  {
    return metadata.getTier();
  }

  public boolean segmentReplicatable()
  {
    return metadata.segmentReplicatable();
  }

  @JsonProperty
  public int getPriority()
  {
    return metadata.getPriority();
  }

  public String getScheme()
  {
    return metadata.getHostAndTlsPort() != null ? "https" : "http";
  }

  public Map<String, DataSegment> getSegments()
  {
    // Copying the map slows things down a lot here, don't use Immutable Map here
    return Collections.unmodifiableMap(segments);
  }

  public DataSegment getSegment(String segmentName)
  {
    return segments.get(segmentName);
  }

  public DruidServer addDataSegment(String segmentId, DataSegment segment)
  {
    synchronized (lock) {
      DataSegment shouldNotExist = segments.get(segmentId);

      if (shouldNotExist != null) {
        log.warn("Asked to add data segment that already exists!? server[%s], segment[%s]", getName(), segmentId);
        return this;
      }

      String dataSourceName = segment.getDataSource();
      DruidDataSource dataSource = dataSources.get(dataSourceName);

      if (dataSource == null) {
        dataSource = new DruidDataSource(
            dataSourceName,
            ImmutableMap.of("client", "side")
        );
        dataSources.put(dataSourceName, dataSource);
      }

      dataSource.addSegment(segmentId, segment);

      segments.put(segmentId, segment);
      currSize += segment.getSize();
    }
    return this;
  }

  public DruidServer addDataSegments(DruidServer server)
  {
    synchronized (lock) {
      for (Map.Entry<String, DataSegment> entry : server.segments.entrySet()) {
        addDataSegment(entry.getKey(), entry.getValue());
      }
    }
    return this;
  }

  public DruidServer removeDataSegment(String segmentId)
  {
    synchronized (lock) {
      DataSegment segment = segments.get(segmentId);

      if (segment == null) {
        log.warn("Asked to remove data segment that doesn't exist!? server[%s], segment[%s]", getName(), segmentId);
        return this;
      }

      DruidDataSource dataSource = dataSources.get(segment.getDataSource());

      if (dataSource == null) {
        log.warn(
            "Asked to remove data segment from dataSource[%s] that doesn't exist, but the segment[%s] exists!?!?!?! wtf?  server[%s]",
            segment.getDataSource(),
            segmentId,
            getName()
        );
        return this;
      }

      dataSource.removePartition(segmentId);

      segments.remove(segmentId);
      currSize -= segment.getSize();

      if (dataSource.isEmpty()) {
        dataSources.remove(dataSource.getName());
      }
    }

    return this;
  }

  public DruidDataSource getDataSource(String dataSource)
  {
    return dataSources.get(dataSource);
  }

  public Iterable<DruidDataSource> getDataSources()
  {
    return dataSources.values();
  }

  public void removeAllSegments()
  {
    synchronized (lock) {
      dataSources.clear();
      segments.clear();
      currSize = 0;
    }
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

    DruidServer that = (DruidServer) o;

    if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return getName() != null ? getName().hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return metadata.toString();
  }

  @Override
  public int compareTo(Object o)
  {
    if (this == o) {
      return 0;
    }
    if (o == null || getClass() != o.getClass()) {
      return 1;
    }

    return getName().compareTo(((DruidServer) o).getName());
  }

  public ImmutableDruidServer toImmutableDruidServer()
  {
    return new ImmutableDruidServer(
        metadata,
        currSize,
        ImmutableMap.copyOf(
            Maps.transformValues(
                dataSources,
                new Function<DruidDataSource, ImmutableDruidDataSource>()
                {
                  @Override
                  public ImmutableDruidDataSource apply(DruidDataSource input)
                  {
                    return input.toImmutableDruidDataSource();
                  }
                }
            )
        ),
        ImmutableMap.copyOf(segments)
    );
  }
}
