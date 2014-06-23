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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 */
public class DruidDataSource
{
  private final Object lock = new Object();

  private final String name;
  private final Map<String, String> properties;
  private final Map<String, DataSegment> partitionNames;
  private final ConcurrentSkipListSet<DataSegment> segmentsHolder;

  public DruidDataSource(
      String name,
      Map<String, String> properties
  )
  {
    this.name = name;
    this.properties = properties;

    this.partitionNames = Maps.newHashMap();
    this.segmentsHolder = new ConcurrentSkipListSet<DataSegment>();
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public Map<String, String> getProperties()
  {
    return properties;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return Collections.unmodifiableSet(segmentsHolder);
  }

  public DruidDataSource addSegment(String partitionName, DataSegment dataSegment)
  {
    synchronized (lock) {
      partitionNames.put(partitionName, dataSegment);

      segmentsHolder.add(dataSegment);
    }
    return this;
  }

  public DruidDataSource addSegments(Map<String, DataSegment> partitionMap)
  {
    synchronized (lock) {
      partitionNames.putAll(partitionMap);

      segmentsHolder.addAll(partitionMap.values());
    }
    return this;
  }

  public DruidDataSource removePartition(String partitionName)
  {
    synchronized (lock) {
      DataSegment dataPart = partitionNames.remove(partitionName);

      if (dataPart == null) {
        return this;
      }

      segmentsHolder.remove(dataPart);
    }

    return this;
  }

  public boolean isEmpty()
  {
    return segmentsHolder.isEmpty();
  }

  @Override
  public String toString()
  {
    return "DruidDataSource{" +
           "properties=" + properties +
           ", partitions=" + segmentsHolder.toString() +
           '}';
  }

  public ImmutableDruidDataSource toImmutableDruidDataSource()
  {
    return new ImmutableDruidDataSource(
        name,
        ImmutableMap.copyOf(properties),
        ImmutableMap.copyOf(partitionNames),
        ImmutableSet.copyOf(segmentsHolder)
    );
  }
}
