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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 */
public class DruidServer implements Comparable
{
  public static final int DEFAULT_PRIORITY = 0;
  public static final int DEFAULT_NUM_REPLICANTS = 2;
  public static final String DEFAULT_TIER = "_default_tier";

  private static final Logger log = new Logger(DruidServer.class);

  private static long sumSize(Map<String, DataSegment> segmentMap)
  {
    return segmentMap.values().stream().mapToLong(DataSegment::getSize).sum();
  }

  private final ConcurrentMap<String, DataSegment> segments = new ConcurrentHashMap<>();

  private final DruidServerMetadata metadata;

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
    return sumSize(segments);
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

  public DruidServer addDataSegment(DataSegment segment)
  {
    return addDataSegment(segment.getIdentifier(), segment);
  }

  public DruidServer addDataSegment(String segmentId, DataSegment segment)
  {
    segments.putIfAbsent(segmentId, segment);
    return this;
  }

  public DruidServer addDataSegments(DruidServer server)
  {
    server.segments.values().forEach(this::addDataSegment);
    return this;
  }

  public DruidServer removeDataSegment(String segmentId)
  {
    final DataSegment segment = segments.remove(segmentId);

    if (segment == null) {
      log.warn("Asked to remove data segment that doesn't exist!? server[%s], segment[%s]", getName(), segmentId);
      return this;
    }

    return this;
  }

  public DruidDataSource getDataSource(String dataSource)
  {
    return segments
        .values()
        .stream()
        .filter(segment -> dataSource.equals(segment.getDataSource()))
        .collect(new Collector<DataSegment, DruidDataSource, DruidDataSource>()
        {
          @Override
          public Supplier<DruidDataSource> supplier()
          {
            return () -> new DruidDataSource(dataSource, ImmutableMap.of("client", "side"));
          }

          @Override
          public BiConsumer<DruidDataSource, DataSegment> accumulator()
          {
            return (druidDataSource, dataSegment) -> druidDataSource.addSegment(
                dataSegment.getIdentifier(),
                dataSegment
            );
          }

          @Override
          public BinaryOperator<DruidDataSource> combiner()
          {
            return (druidDataSource, druidDataSource2) -> {
              final Map<String, DataSegment> otherSegments = druidDataSource2
                  .getSegments()
                  .stream()
                  .collect(Collectors.toMap(
                      DataSegment::getIdentifier,
                      Function.identity()
                  ));
              return druidDataSource.addSegments(otherSegments);
            };
          }

          @Override
          public Function<DruidDataSource, DruidDataSource> finisher()
          {
            return druidDataSource -> {
              if (druidDataSource.getSegments().isEmpty()) {
                // Mimic the behavior of `Map::get`
                return null;
              }
              return druidDataSource;
            };
          }

          @Override
          public Set<Characteristics> characteristics()
          {
            return ImmutableSet.of(Characteristics.UNORDERED);
          }
        });
  }

  public Set<String> getDataSourceNames()
  {
    return segments
        .values()
        .stream()
        .map(DataSegment::getDataSource)
        .collect(Collectors.toSet());
  }

  public Collection<ImmutableDruidDataSource> getDataSources()
  {
    return toImmutableDruidServer().getDataSources();
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
    final ImmutableMap<String, DataSegment> segmentMap = ImmutableMap.copyOf(segments);
    final Map<String, DruidDataSource> dataSourceMap = new HashMap<>();
    segmentMap.values().forEach(ds -> dataSourceMap.computeIfAbsent(ds.getDataSource(), name -> new DruidDataSource(
        name,
        ImmutableMap.of("client", "side")
    )).addSegment(ds.getIdentifier(), ds));
    final long size = sumSize(segmentMap);
    final Map<String, ImmutableDruidDataSource> dataSourceImmutableMap = Maps.transformValues(
        dataSourceMap,
        DruidDataSource::toImmutableDruidDataSource
    );
    return new ImmutableDruidServer(
        metadata,
        size,
        ImmutableMap.copyOf(dataSourceImmutableMap),
        segmentMap
    );
  }
}
