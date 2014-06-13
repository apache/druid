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

import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;

/**
 */
public class IncrementalIndexSchema
{
  private final long minTimestamp;
  private final QueryGranularity gran;
  private final DimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] metrics;

  public IncrementalIndexSchema(
      long minTimestamp,
      QueryGranularity gran,
      DimensionsSpec dimensionsSpec,
      AggregatorFactory[] metrics
  )
  {
    this.minTimestamp = minTimestamp;
    this.gran = gran;
    this.dimensionsSpec = dimensionsSpec;
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

  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  public AggregatorFactory[] getMetrics()
  {
    return metrics;
  }

  public static class Builder
  {
    private long minTimestamp;
    private QueryGranularity gran;
    private DimensionsSpec dimensionsSpec;
    private AggregatorFactory[] metrics;

    public Builder()
    {
      this.minTimestamp = 0L;
      this.gran = QueryGranularity.NONE;
      this.dimensionsSpec = new DimensionsSpec(null, null, null);
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

    public Builder withDimensionsSpec(DimensionsSpec dimensionsSpec)
    {
      this.dimensionsSpec = dimensionsSpec;
      return this;
    }

    public Builder withDimensionsSpec(InputRowParser parser)
    {
      if (parser != null
          && parser.getParseSpec() != null
          && parser.getParseSpec().getDimensionsSpec() != null) {
        this.dimensionsSpec = parser.getParseSpec().getDimensionsSpec();
      } else {
        this.dimensionsSpec = new DimensionsSpec(null, null, null);
      }

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
          minTimestamp, gran, dimensionsSpec, metrics
      );
    }
  }
}
