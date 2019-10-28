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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A mutable collection of metadata of segments ({@link DataSegment} objects), stored on a particular Druid server
 * (typically historical).
 *
 * This class should not be subclassed, it isn't declared final only to make it possible to mock the class with EasyMock
 * in tests.
 *
 * @see ImmutableDruidServer - an immutable counterpart of this class
 */
public class DruidServer implements Comparable<DruidServer>
{
  public static final int DEFAULT_PRIORITY = 0;
  public static final int DEFAULT_NUM_REPLICANTS = 2;
  public static final String DEFAULT_TIER = "_default_tier";

  private static final Logger log = new Logger(DruidServer.class);

  private final DruidServerMetadata metadata;

  private final ConcurrentHashMap<String, DruidDataSource> dataSources = new ConcurrentHashMap<>();
  private final AtomicInteger totalSegments = new AtomicInteger();
  private final AtomicLong currSize = new AtomicLong(0);

  public DruidServer(DruidNode node, DruidServerConfig config, ServerType type)
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
    return currSize.get();
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

  /**
   * Returns an iterable to go over all segments in all data sources, stored on this DruidServer. The order in which
   * segments are iterated is unspecified.
   *
   * Since this DruidServer can be mutated concurrently, the set of segments observed during an iteration may _not_ be
   * a momentary snapshot of the segments on the server, in other words, it may be that there was no moment when the
   * DruidServer stored exactly the returned set of segments.
   *
   * Note: the iteration may not be as trivially cheap as, for example, iteration over an ArrayList. Try (to some
   * reasonable extent) to organize the code so that it iterates the returned iterable only once rather than several
   * times.
   */
  public Iterable<DataSegment> iterateAllSegments()
  {
    return () -> dataSources.values().stream().flatMap(dataSource -> dataSource.getSegments().stream()).iterator();
  }

  /**
   * Returns the current number of segments, stored in this DruidServer object. This number if weakly consistent with
   * the number of segments if {@link #iterateAllSegments} is iterated about the same time, because segments might be
   * added or removed in parallel.
   */
  public int getTotalSegments()
  {
    return totalSegments.get();
  }

  public DataSegment getSegment(SegmentId segmentId)
  {
    DruidDataSource dataSource = dataSources.get(segmentId.getDataSource());
    if (dataSource == null) {
      return null;
    }
    return dataSource.getSegment(segmentId);
  }

  public DruidServer addDataSegment(DataSegment segment)
  {
    // ConcurrentHashMap.compute() ensures that all actions for specific dataSource are linearizable.
    dataSources.compute(
        segment.getDataSource(),
        (dataSourceName, dataSource) -> {
          if (dataSource == null) {
            dataSource = new DruidDataSource(dataSourceName, ImmutableMap.of("client", "side"));
          }
          if (dataSource.addSegmentIfAbsent(segment)) {
            currSize.addAndGet(segment.getSize());
            totalSegments.incrementAndGet();
          } else {
            log.warn("Asked to add data segment that already exists!? server[%s], segment[%s]", getName(), segment);
          }
          return dataSource;
        }
    );
    return this;
  }

  public DruidServer addDataSegments(DruidServer server)
  {
    server.iterateAllSegments().forEach(this::addDataSegment);
    return this;
  }

  @Nullable
  public DataSegment removeDataSegment(SegmentId segmentId)
  {
    // To pass result from inside the lambda.
    DataSegment[] segmentRemoved = new DataSegment[1];
    // ConcurrentHashMap.compute() ensures that all actions for specific dataSource are linearizable.
    dataSources.compute(
        segmentId.getDataSource(),
        (dataSourceName, dataSource) -> {
          if (dataSource == null) {
            log.warn(
                "Asked to remove data segment from a data source that doesn't exist!? server[%s], segment[%s]",
                getName(),
                segmentId
            );
            // Returning null from the lambda here makes the ConcurrentHashMap to not record any entry.
            return null;
          }
          DataSegment segment = dataSource.removeSegment(segmentId);
          if (segment != null) {
            segmentRemoved[0] = segment;
            currSize.addAndGet(-segment.getSize());
            totalSegments.decrementAndGet();
          } else {
            log.warn("Asked to remove data segment that doesn't exist!? server[%s], segment[%s]", getName(), segmentId);
          }
          // Returning null from the lambda here makes the ConcurrentHashMap to remove the current entry.
          return dataSource.isEmpty() ? null : dataSource;
        }
    );
    return segmentRemoved[0];
  }

  public DruidDataSource getDataSource(String dataSource)
  {
    return dataSources.get(dataSource);
  }

  public Collection<DruidDataSource> getDataSources()
  {
    return dataSources.values();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DruidServer)) {
      return false;
    }

    DruidServer that = (DruidServer) o;

    return metadata.equals(that.metadata);
  }

  @Override
  public int hashCode()
  {
    return metadata.hashCode();
  }

  @Override
  public String toString()
  {
    return metadata.toString();
  }

  @Override
  public int compareTo(DruidServer o)
  {
    return getName().compareTo(o.getName());
  }

  public ImmutableDruidServer toImmutableDruidServer()
  {
    ImmutableMap<String, ImmutableDruidDataSource> immutableDataSources = ImmutableMap.copyOf(
        Maps.transformValues(this.dataSources, DruidDataSource::toImmutableDruidDataSource)
    );
    // Computing the size and the total number of segments using the resulting immutableDataSources rather that taking
    // currSize.get() and totalSegments.get() to avoid inconsistency: segments could be added and deleted while we are
    // running toImmutableDruidDataSource().
    long size =
        immutableDataSources.values().stream().mapToLong(ImmutableDruidDataSource::getTotalSizeOfSegments).sum();
    int totalSegments =
        immutableDataSources.values().stream().mapToInt(dataSource -> dataSource.getSegments().size()).sum();
    return new ImmutableDruidServer(metadata, size, immutableDataSources, totalSegments);
  }
}
