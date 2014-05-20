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
}
