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

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 */
public class FillCapacityWithAffinityConfig
{
  // key:Datasource, value:[nodeHostNames]
  private Map<String, List<String>> affinity = Maps.newHashMap();

  @JsonCreator
  public FillCapacityWithAffinityConfig(
      @JsonProperty("affinity") Map<String, List<String>> affinity
  )
  {
    this.affinity = affinity;
  }

  @JsonProperty
  public Map<String, List<String>> getAffinity()
  {
    return affinity;
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

    FillCapacityWithAffinityConfig that = (FillCapacityWithAffinityConfig) o;

    if (affinity != null
        ? !Maps.difference(affinity, that.affinity).entriesDiffering().isEmpty()
        : that.affinity != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return affinity != null ? affinity.hashCode() : 0;
  }
}
