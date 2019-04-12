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

package org.apache.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.transform.RowFunction;
import org.apache.druid.segment.transform.Transform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.transform.Transformer;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DimFilterHavingSpec extends BaseHavingSpec
{
  private static final boolean DEFAULT_FINALIZE = true;

  private final DimFilter dimFilter;
  private final boolean finalize;

  private Map<String, ValueType> rowSignature = new HashMap<>();
  private Map<String, AggregatorFactory> aggregators = new HashMap<>();
  private Transformer transformer = null;
  private int evalCount;

  @JsonCreator
  public DimFilterHavingSpec(
      @JsonProperty("filter") final DimFilter dimFilter,
      @JsonProperty("finalize") final Boolean finalize
  )
  {
    this.dimFilter = Preconditions.checkNotNull(dimFilter, "filter");
    this.finalize = finalize == null ? DEFAULT_FINALIZE : finalize;
  }

  @JsonProperty("filter")
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public boolean isFinalize()
  {
    return finalize;
  }

  @Override
  public void setRowSignature(Map<String, ValueType> rowSignature)
  {
    this.rowSignature = rowSignature;
  }

  @Override
  public void setAggregators(final Map<String, AggregatorFactory> aggregators)
  {
    this.aggregators = aggregators;
  }

  @Override
  public boolean eval(final Row row)
  {
    int oldEvalCount = evalCount;
    evalCount++;

    if (transformer == null) {
      transformer = createTransformer(dimFilter, rowSignature, aggregators, finalize);
    }

    final boolean retVal = transformer.transform(new RowAsInputRow(row)) != null;

    if (evalCount != oldEvalCount + 1) {
      // Oops, someone was using this from two different threads, bad caller.
      throw new IllegalStateException("concurrent 'eval' calls not permitted!");
    }

    return retVal;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DimFilterHavingSpec that = (DimFilterHavingSpec) o;
    return finalize == that.finalize &&
           Objects.equals(dimFilter, that.dimFilter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimFilter, finalize);
  }

  @Override
  public String toString()
  {
    return "DimFilterHavingSpec{" +
           "dimFilter=" + dimFilter +
           ", finalize=" + finalize +
           '}';
  }

  private static Transformer createTransformer(
      final DimFilter filter,
      final Map<String, ValueType> rowSignature,
      final Map<String, AggregatorFactory> aggregators,
      final boolean finalize
  )
  {
    final List<Transform> transforms = new ArrayList<>();

    if (finalize) {
      for (AggregatorFactory aggregator : aggregators.values()) {
        final String name = aggregator.getName();

        transforms.add(
            new Transform()
            {
              @Override
              public String getName()
              {
                return name;
              }

              @Override
              public RowFunction getRowFunction()
              {
                return row -> aggregator.finalizeComputation(row.getRaw(name));
              }
            }
        );
      }
    }

    return new TransformSpec(filter, transforms).toTransformer(rowSignature);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(HavingSpecUtil.CACHE_TYPE_ID_DIM_FILTER)
        .appendCacheable(dimFilter)
        .appendByte((byte) (isFinalize() ? 1 : 0))
        .build();
  }

  private static class RowAsInputRow implements InputRow
  {
    private final Row row;

    public RowAsInputRow(final Row row)
    {
      this.row = row;
    }

    @Override
    public List<String> getDimensions()
    {
      return Collections.emptyList();
    }

    @Override
    public long getTimestampFromEpoch()
    {
      return row.getTimestampFromEpoch();
    }

    @Override
    public DateTime getTimestamp()
    {
      return row.getTimestamp();
    }

    @Override
    public List<String> getDimension(final String dimension)
    {
      return row.getDimension(dimension);
    }

    @Override
    public Object getRaw(final String dimension)
    {
      return row.getRaw(dimension);
    }

    @Override
    public Number getMetric(final String metric)
    {
      return row.getMetric(metric);
    }

    @Override
    public int compareTo(final Row o)
    {
      return row.compareTo(o);
    }

    @Override
    public boolean equals(final Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final RowAsInputRow that = (RowAsInputRow) o;
      return Objects.equals(row, that.row);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(row);
    }

    @Override
    public String toString()
    {
      return "RowAsInputRow{" +
             "row=" + row +
             '}';
    }
  }
}
