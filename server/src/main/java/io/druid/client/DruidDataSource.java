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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.timeline.DataSegment;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class DruidDataSource
{
  private final String name;
  private final Map<String, String> properties;
  private final ConcurrentHashMap<String, DataSegment> idToSegmentMap;

  public DruidDataSource(
      String name,
      Map<String, String> properties
  )
  {
    this.name = Preconditions.checkNotNull(name);
    this.properties = properties;
    this.idToSegmentMap = new ConcurrentHashMap<>();
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

  public Collection<DataSegment> getSegments()
  {
    return Collections.unmodifiableCollection(idToSegmentMap.values());
  }

  public DruidDataSource addSegment(DataSegment dataSegment)
  {
    idToSegmentMap.put(dataSegment.getIdentifier(), dataSegment);
    return this;
  }

  public DruidDataSource removePartition(String segmentId)
  {
    idToSegmentMap.remove(segmentId);
    return this;
  }

  public DataSegment getSegment(String identifier)
  {
    return idToSegmentMap.get(identifier);
  }

  public boolean isEmpty()
  {
    return idToSegmentMap.isEmpty();
  }

  public ImmutableDruidDataSource toImmutableDruidDataSource()
  {
    return new ImmutableDruidDataSource(
        name,
        ImmutableMap.copyOf(properties),
        ImmutableMap.copyOf(idToSegmentMap)
    );
  }

  @Override
  public String toString()
  {
    return "DruidDataSource{" +
           "properties=" + properties +
           ", partitions=" + idToSegmentMap.values() +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    throw new UnsupportedOperationException("Use ImmutableDruidDataSource instead");
  }

  @Override
  public int hashCode()
  {
    throw new UnsupportedOperationException("Use ImmutableDruidDataSource instead");
  }
}
