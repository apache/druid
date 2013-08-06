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

package com.metamx.druid.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import com.metamx.druid.coordination.DruidServerMetadata;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class DruidServer implements Comparable
{
  public static final String DEFAULT_TIER = "_default_tier";
  private static final Logger log = new Logger(DruidServer.class);

  private final Object lock = new Object();

  private final ConcurrentMap<String, DruidDataSource> dataSources;
  private final ConcurrentMap<String, Integer> dataSourceCount;
  private final ConcurrentMap<String, DataSegment> segments;

  private final DruidServerMetadata metadata;

  private final ConcurrentMap<DateTime,Map<Interval,Integer>> intervalMap;

  private volatile long currSize;

  public DruidServer(
      DruidServerConfig config,
      String type
  )
  {
    this(
        config.getServerName(),
        config.getHost(),
        config.getMaxSize(),
        type,
        config.getTier()
    );
  }

  @JsonCreator
  public DruidServer(
      @JsonProperty("name") String name,
      @JsonProperty("host") String host,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") String type,
      @JsonProperty("tier") String tier
  )
  {
    this.metadata = new DruidServerMetadata(name, host, maxSize, type, tier);

    this.dataSources = new ConcurrentHashMap<String, DruidDataSource>();
    this.segments = new ConcurrentHashMap<String, DataSegment>();
    this.intervalMap = new ConcurrentHashMap<DateTime,Map<Interval,Integer>>();
    this.dataSourceCount = new ConcurrentHashMap<String, Integer>();
  }

  public String getName()
  {
    return metadata.getName();
  }

  public DruidServerMetadata getMetadata()
  {
    return metadata;
  }

  @JsonProperty
  public String getHost()
  {
    return metadata.getHost();
  }

  @JsonProperty
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
  public String getType()
  {
    return metadata.getType();
  }

  @JsonProperty
  public String getTier()
  {
    return metadata.getTier();
  }

  @JsonProperty
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

      if(dataSourceCount.get(dataSourceName)==null)
      {
        dataSourceCount.put(dataSourceName,1);
      }
      else{
        dataSourceCount.put(dataSourceName,dataSourceCount.get(dataSourceName)+1);
      }

      dataSource.addSegment(segmentId, segment);
      segments.put(segmentId, segment);
      Interval segmentInterval = segment.getInterval();
      DateTime intervalMonth = segmentInterval.getStart().toDateTime().withDayOfMonth(1);
      Map<Interval,Integer> monthMap = intervalMap.get(intervalMonth);
      if (monthMap==null)
      {
        intervalMap.put(intervalMonth,new HashMap<Interval, Integer>());
      }
      intervalMap.get(intervalMonth).put(segmentInterval,1);
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
      Integer dataSourceCountValue = dataSourceCount.get(segment.getDataSource());
      if (dataSourceCountValue==null)
      {
        log.warn("Datasource not in DatasourceCount for some weird reason.");
      }
      else{
        if (dataSourceCountValue.equals(1))
        {
          dataSourceCount.remove(segment.getDataSource());
        }
        else
        {
          dataSourceCount.put(segment.getDataSource(),dataSourceCountValue-1);
        }
      }

      dataSource.removePartition(segmentId);

      segments.remove(segmentId);
      intervalMap.get(segment.getInterval().getStart().withDayOfMonth(1)).remove(segment.getInterval());
      currSize -= segment.getSize();

      if (dataSource.isEmpty()) {
        dataSources.remove(dataSource.getName());
      }
    }

    return this;
  }

  public Map<String, Integer> getDataSourceCount()
  {
    return Collections.unmodifiableMap(dataSourceCount);
  }

  public Map<DateTime, Map<Interval, Integer>> getIntervalMap()
  {
    return Collections.unmodifiableMap(intervalMap);
  }

  public DruidDataSource getDataSource(String dataSource)
  {
    return dataSources.get(dataSource);
  }

  public Iterable<DruidDataSource> getDataSources()
  {
    return dataSources.values();
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
}
