/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.incremental;

import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 */
public class IncrementalIndexSchema
{
  public static IncrementalIndexSchema.Builder builder()
  {
    return new Builder();
  }

  private final long minTimestamp;
  private final TimestampSpec timestampSpec;
  private final Granularity queryGranularity;
  private final VirtualColumns virtualColumns;
  private final DimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] metrics;
  private final boolean rollup;

  private final List<AggregateProjectionSpec> projections;

  public IncrementalIndexSchema(
      long minTimestamp,
      TimestampSpec timestampSpec,
      Granularity queryGranularity,
      VirtualColumns virtualColumns,
      DimensionsSpec dimensionsSpec,
      AggregatorFactory[] metrics,
      boolean rollup,
      List<AggregateProjectionSpec> projections
  )
  {
    this.minTimestamp = minTimestamp;
    this.timestampSpec = timestampSpec;
    this.queryGranularity = queryGranularity;
    this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
    this.dimensionsSpec = dimensionsSpec;
    this.metrics = metrics;
    this.rollup = rollup;
    this.projections = projections;
  }

  public long getMinTimestamp()
  {
    return minTimestamp;
  }

  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  public Granularity getQueryGranularity()
  {
    return queryGranularity;
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

  public List<AggregateProjectionSpec> getProjections()
  {
    return projections;
  }

  public static class Builder
  {
    private long minTimestamp;
    private TimestampSpec timestampSpec;
    private Granularity queryGranularity;
    private VirtualColumns virtualColumns;
    private DimensionsSpec dimensionsSpec;
    private AggregatorFactory[] metrics;
    private boolean rollup;
    private List<AggregateProjectionSpec> projections;

    public Builder()
    {
      this.minTimestamp = 0L;
      this.queryGranularity = Granularities.NONE;
      this.virtualColumns = VirtualColumns.EMPTY;
      this.dimensionsSpec = DimensionsSpec.EMPTY;
      this.metrics = new AggregatorFactory[]{};
      this.rollup = true;
      this.projections = Collections.emptyList();
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

    public Builder withQueryGranularity(Granularity gran)
    {
      this.queryGranularity = gran;
      return this;
    }

    public Builder withVirtualColumns(VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public Builder withDimensionsSpec(@Nullable DimensionsSpec dimensionsSpec)
    {
      this.dimensionsSpec = dimensionsSpec == null ? DimensionsSpec.EMPTY : dimensionsSpec;
      return this;
    }

    @Deprecated
    public Builder withDimensionsSpec(@Nullable InputRowParser parser)
    {
      if (parser != null
          && parser.getParseSpec() != null
          && parser.getParseSpec().getDimensionsSpec() != null) {
        this.dimensionsSpec = parser.getParseSpec().getDimensionsSpec();
      } else {
        this.dimensionsSpec = DimensionsSpec.EMPTY;
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

    public Builder withProjections(@Nullable List<AggregateProjectionSpec> projections)
    {
      this.projections = projections == null ? Collections.emptyList() : projections;
      return this;
    }

    public IncrementalIndexSchema build()
    {
      return new IncrementalIndexSchema(
          minTimestamp,
          timestampSpec,
          queryGranularity,
          virtualColumns,
          dimensionsSpec,
          metrics,
          rollup,
          projections
      );
    }
  }
}
