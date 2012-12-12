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

package com.metamx.druid.result;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

public class SegmentMetadataResultValue
{
  public static class Dimension {
    @JsonProperty public long size;
    @JsonProperty public int cardinality;

    @JsonCreator
    public Dimension(
        @JsonProperty("size") long size,
        @JsonProperty("cardinality") int cardinality
    )
    {
      this.size = size;
      this.cardinality = cardinality;
    }
  }
  public static class Metric {
    @JsonProperty public String type;
    @JsonProperty public long size;

    @JsonCreator
    public Metric(
            @JsonProperty("type") String type,
            @JsonProperty("size") long size
    )
    {
      this.type = type;
      this.size = size;
    }
  }

  private final String id;
  private final Map<String, Dimension> dimensions;
  private final Map<String, Metric> metrics;
  private final long size;

  @JsonCreator
  public SegmentMetadataResultValue(
      @JsonProperty("id") String id,
      @JsonProperty("dimensions") Map<String, Dimension> dimensions,
      @JsonProperty("metrics") Map<String, Metric> metrics,
      @JsonProperty("size") long size

  )
  {
    this.id = id;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.size = size;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public Map<String, Dimension> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public Map<String, Metric> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public long getSize()
  {
    return size;
  }
}
