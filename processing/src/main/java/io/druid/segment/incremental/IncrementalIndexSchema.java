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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.Collections;
import java.util.List;

/**
 */
public class IncrementalIndexSchema
{
  private final long minTimestamp;
  private final QueryGranularity gran;
  private final List<String> dimensions;
  private final List<SpatialDimensionSchema> spatialDimensions;
  private final AggregatorFactory[] metrics;

  public IncrementalIndexSchema(
      long minTimestamp,
      QueryGranularity gran,
      List<String> dimensions,
      List<SpatialDimensionSchema> spatialDimensions,
      AggregatorFactory[] metrics
  )
  {
    this.minTimestamp = minTimestamp;
    this.gran = gran;
    this.dimensions = dimensions;
    this.spatialDimensions = spatialDimensions;
    this.metrics = metrics;
  }

  public long getMinTimestamp()
  {
    return minTimestamp;
  }

  public QueryGranularity getGran()
  {
    return gran;
  }

  public List<String> getDimensions()
  {
    return dimensions;
  }

  public List<SpatialDimensionSchema> getSpatialDimensions()
  {
    return spatialDimensions;
  }

  public AggregatorFactory[] getMetrics()
  {
    return metrics;
  }

  public static class Builder
  {
    private long minTimestamp;
    private QueryGranularity gran;
    private List<String> dimensions;
    private List<SpatialDimensionSchema> spatialDimensions;
    private AggregatorFactory[] metrics;

    public Builder()
    {
      this.minTimestamp = 0L;
      this.gran = QueryGranularity.NONE;
      this.dimensions = Lists.newArrayList();
      this.spatialDimensions = Lists.newArrayList();
      this.metrics = new AggregatorFactory[]{};
    }

    public Builder withMinTimestamp(long minTimestamp)
    {
      this.minTimestamp = minTimestamp;
      return this;
    }

    public Builder withQueryGranularity(QueryGranularity gran)
    {
      this.gran = gran;
      return this;
    }

    public Builder withDimensions(Iterable<String> dimensions)
    {
      this.dimensions = Lists.newArrayList(
          Iterables.transform(
              dimensions, new Function<String, String>()
          {
            @Override
            public String apply(String input)
            {
              return input.toLowerCase();
            }
          }
          )
      );
      Collections.sort(this.dimensions);
      return this;
    }

    public Builder withSpatialDimensions(InputRowParser parser)
    {
      if (parser != null
          && parser.getParseSpec() != null
          && parser.getParseSpec().getDimensionsSpec() != null
          && parser.getParseSpec().getDimensionsSpec().getSpatialDimensions() != null) {
        this.spatialDimensions = parser.getParseSpec().getDimensionsSpec().getSpatialDimensions();
      } else {
        this.spatialDimensions = Lists.newArrayList();
      }

      return this;
    }

    public Builder withSpatialDimensions(List<SpatialDimensionSchema> spatialDimensions)
    {
      this.spatialDimensions = spatialDimensions;
      return this;
    }

    public Builder withMetrics(AggregatorFactory[] metrics)
    {
      this.metrics = metrics;
      return this;
    }

    public IncrementalIndexSchema build()
    {
      return new IncrementalIndexSchema(
          minTimestamp, gran, dimensions, spatialDimensions, metrics
      );
    }
  }
}
