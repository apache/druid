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

package io.druid.segment.incremental;

import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.granularity.QueryGranularity;
import io.druid.granularity.QueryGranularities;
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
      this.gran = QueryGranularities.NONE;
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
