/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
