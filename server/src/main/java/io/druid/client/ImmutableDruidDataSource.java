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
import com.google.common.collect.ImmutableSet;
import io.druid.timeline.DataSegment;

import java.util.Map;
import java.util.Set;

/**
 */
public class ImmutableDruidDataSource
{
  private final String name;
  private final ImmutableMap<String, String> properties;
  private final ImmutableMap<String, DataSegment> partitionNames;
  private final ImmutableSet<DataSegment> segmentsHolder;

  public ImmutableDruidDataSource(
      String name,
      ImmutableMap<String, String> properties,
      ImmutableMap<String, DataSegment> partitionNames,
      ImmutableSet<DataSegment> segmentsHolder
  )
  {
    this.name = name;
    this.properties = properties;
    this.partitionNames = partitionNames;
    this.segmentsHolder = segmentsHolder;
  }

  public String getName()
  {
    return name;
  }

  public Map<String, String> getProperties()
  {
    return properties;
  }

  public Map<String, DataSegment> getPartitionNames()
  {
    return partitionNames;
  }

  public boolean isEmpty()
  {
    return segmentsHolder.isEmpty();
  }

  public Set<DataSegment> getSegments()
  {
    return segmentsHolder;
  }

  @Override
  public String toString()
  {
    // partitionNames is intentionally ignored because it is usually large
    return "ImmutableDruidDataSource{"
           + "name='" + name
           + "', segments='" + segmentsHolder
           + "', properties='" + properties
           + "'}";
  }
}
