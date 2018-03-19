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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.timeline.DataSegment;

import java.util.Collection;
import java.util.Objects;

/**
 */
public class ImmutableDruidDataSource
{
  private final String name;
  private final ImmutableMap<String, String> properties;
  private final ImmutableMap<String, DataSegment> idToSegments;

  public ImmutableDruidDataSource(
      String name,
      ImmutableMap<String, String> properties,
      ImmutableMap<String, DataSegment> idToSegments
  )
  {
    this.name = Preconditions.checkNotNull(name);
    this.properties = properties;
    this.idToSegments = idToSegments;
  }

  public String getName()
  {
    return name;
  }

  public Collection<DataSegment> getSegments()
  {
    return idToSegments.values();
  }

  public DataSegment getSegment(String segmentIdentifier)
  {
    return idToSegments.get(segmentIdentifier);
  }

  @Override
  public String toString()
  {
    // The detail of idToSegments is intentionally ignored because it is usually large
    return "ImmutableDruidDataSource{"
           + "name='" + name
           + "', # of segments='" + idToSegments.size()
           + "', properties='" + properties
           + "'}";
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }

    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }

    final ImmutableDruidDataSource that = (ImmutableDruidDataSource) o;
    if (!this.name.equals(that.name)) {
      return false;
    }

    if (!this.properties.equals(that.properties)) {
      return false;
    }

    return this.idToSegments.equals(that.idToSegments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, properties, idToSegments);
  }
}
