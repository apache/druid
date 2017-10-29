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
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.VirtualColumns;

/**
 */
public class IncrementalIndexSchema
{
  public static final boolean DEFAULT_ROLLUP = true;
  private final long minTimestamp;
  private final TimestampSpec timestampSpec;
  private final Granularity gran;
  private final VirtualColumns virtualColumns;
  private final DimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] metrics;
  private final boolean rollup;

  public IncrementalIndexSchema(
      long minTimestamp,
      TimestampSpec timestampSpec,
      Granularity gran,
      VirtualColumns virtualColumns,
      DimensionsSpec dimensionsSpec,
      AggregatorFactory[] metrics,
      boolean rollup
  )
  {
    this.minTimestamp = minTimestamp;
    this.timestampSpec = timestampSpec;
    this.gran = gran;
    this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
    this.dimensionsSpec = dimensionsSpec;
    this.metrics = metrics;
    this.rollup = rollup;
  }

  public long getMinTimestamp()
  {
    return minTimestamp;
  }

  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  public Granularity getGran()
  {
    return gran;
  }

  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  public AggregatorFactory[] getMetrics()
  {
    return metrics;
  }

  public boolean isRollup()
  {
    return rollup;
  }

  public static class Builder
  {
    private long minTimestamp;
    private TimestampSpec timestampSpec;
    private Granularity gran;
    private VirtualColumns virtualColumns;
    private DimensionsSpec dimensionsSpec;
    private AggregatorFactory[] metrics;
    private boolean rollup;

    public Builder()
    {
      this.minTimestamp = 0L;
      this.gran = Granularities.NONE;
      this.virtualColumns = VirtualColumns.EMPTY;
      this.dimensionsSpec = DimensionsSpec.EMPTY;
      this.metrics = new AggregatorFactory[]{};
      this.rollup = true;
    }

    public Builder withMinTimestamp(long minTimestamp)
    {
      this.minTimestamp = minTimestamp;
      return this;
    }

    public Builder withTimestampSpec(TimestampSpec timestampSpec)
    {
      this.timestampSpec = timestampSpec;
      return this;
    }

    public Builder withTimestampSpec(InputRowParser parser)
    {
      if (parser != null
          && parser.getParseSpec() != null
          && parser.getParseSpec().getTimestampSpec() != null) {
        this.timestampSpec = parser.getParseSpec().getTimestampSpec();
      } else {
        this.timestampSpec = new TimestampSpec(null, null, null);
      }
      return this;
    }

    public Builder withQueryGranularity(Granularity gran)
    {
      this.gran = gran;
      return this;
    }

    public Builder withVirtualColumns(VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public Builder withDimensionsSpec(DimensionsSpec dimensionsSpec)
    {
      this.dimensionsSpec = dimensionsSpec == null ? DimensionsSpec.EMPTY : dimensionsSpec;
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

    public Builder withMetrics(AggregatorFactory... metrics)
    {
      this.metrics = metrics;
      return this;
    }

    public Builder withRollup(boolean rollup)
    {
      this.rollup = rollup;
      return this;
    }

    public IncrementalIndexSchema build()
    {
      return new IncrementalIndexSchema(
          minTimestamp, timestampSpec, gran, virtualColumns, dimensionsSpec, metrics, rollup
      );
    }
  }
}
