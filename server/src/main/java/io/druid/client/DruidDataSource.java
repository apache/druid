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
    synchronized (lock) {
      return "DruidDataSource{" +
             "properties=" + properties +
             ", partitions=" + segmentsHolder.toString() +
             '}';
    }
  }

  public ImmutableDruidDataSource toImmutableDruidDataSource()
  {
    synchronized (lock) {
      return new ImmutableDruidDataSource(
          name,
          ImmutableMap.copyOf(properties),
          ImmutableMap.copyOf(partitionNames),
          ImmutableSet.copyOf(segmentsHolder)
      );
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

    DruidDataSource that = (DruidDataSource) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (partitionNames != null ? !partitionNames.equals(that.partitionNames) : that.partitionNames != null) {
      return false;
    }
    if (properties != null ? !properties.equals(that.properties) : that.properties != null) {
      return false;
    }
    if (segmentsHolder != null ? !segmentsHolder.equals(that.segmentsHolder) : that.segmentsHolder != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (properties != null ? properties.hashCode() : 0);
    result = 31 * result + (partitionNames != null ? partitionNames.hashCode() : 0);
    result = 31 * result + (segmentsHolder != null ? segmentsHolder.hashCode() : 0);
    return result;
  }

  public DataSegment getSegment(String identifier)
  {
    return partitionNames.get(identifier);
  }
}
