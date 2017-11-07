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
import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 */
public class DruidDataSource
{
  private final Object lock = new Object();

  private final String name;
  private final Map<String, String> properties;
  private final Map<String, DataSegment> idToSegmentMap;
  private final Set<DataSegment> segmentsHolder;

  public DruidDataSource(
      String name,
      Map<String, String> properties
  )
  {
    this.name = name;
    this.properties = properties;

    this.idToSegmentMap = new HashMap<>();
    this.segmentsHolder = new HashSet<>();
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

  public DruidDataSource addSegment(DataSegment dataSegment)
  {
    synchronized (lock) {
      idToSegmentMap.put(dataSegment.getIdentifier(), dataSegment);

      segmentsHolder.add(dataSegment);
    }
    return this;
  }

  public DruidDataSource addSegments(Map<String, DataSegment> partitionMap)
  {
    synchronized (lock) {
      idToSegmentMap.putAll(partitionMap);

      segmentsHolder.addAll(partitionMap.values());
    }
    return this;
  }

  public DruidDataSource removePartition(String segmentId)
  {
    synchronized (lock) {
      DataSegment dataPart = idToSegmentMap.remove(segmentId);

      if (dataPart == null) {
        return this;
      }

      segmentsHolder.remove(dataPart);
    }

    return this;
  }

  public DataSegment getSegment(String identifier)
  {
    synchronized (lock) {
      return idToSegmentMap.get(identifier);
    }
  }

  public boolean isEmpty()
  {
    synchronized (lock) {
      return segmentsHolder.isEmpty();
    }
  }

  public ImmutableDruidDataSource toImmutableDruidDataSource()
  {
    synchronized (lock) {
      return new ImmutableDruidDataSource(
          name,
          ImmutableMap.copyOf(properties),
          ImmutableMap.copyOf(idToSegmentMap),
          ImmutableSet.copyOf(segmentsHolder)
      );
    }
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
}
