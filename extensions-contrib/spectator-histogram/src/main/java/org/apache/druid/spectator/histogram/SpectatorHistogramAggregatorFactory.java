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

package org.apache.druid.spectator.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@JsonTypeName(SpectatorHistogramAggregatorFactory.TYPE_NAME)
public class SpectatorHistogramAggregatorFactory extends AggregatorFactory
{
  @Nonnull
  private final String name;

  @Nonnull
  private final String fieldName;

  @Nonnull
  private final byte cacheTypeId;

  public static final String TYPE_NAME = "spectatorHistogram";
  public static final ColumnType TYPE = ColumnType.ofComplex(TYPE_NAME);

  @JsonCreator
  public SpectatorHistogramAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    this(name, fieldName, AggregatorUtil.SPECTATOR_HISTOGRAM_CACHE_TYPE_ID);
  }

  public SpectatorHistogramAggregatorFactory(
      final String name,
      final String fieldName,
      final byte cacheTypeId
  )
  {
    this.name = Objects.requireNonNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName = Objects.requireNonNull(fieldName, "Parameter fieldName must be specified");
    this.cacheTypeId = cacheTypeId;
  }


  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(
        cacheTypeId
    ).appendString(fieldName).build();
  }


  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new SpectatorHistogramAggregator(metricFactory.makeColumnValueSelector(fieldName));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new SpectatorHistogramBufferAggregator(metricFactory.makeColumnValueSelector(fieldName));
  }

  // This is used when writing metrics to segment files to check whether the column is sorted.
  // Since there is no sensible way really to compare histograms, compareTo always returns 1.
  public static final Comparator<SpectatorHistogram> COMPARATOR = (o, o1) -> {
    if (o == null && o1 == null) {
      return 0;
    } else if (o != null && o1 == null) {
      return -1;
    } else if (o == null) {
      return 1;
    }
    return Integer.compare(o.hashCode(), o1.hashCode());
  };

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (lhs == null) {
      return rhs;
    }
    if (rhs == null) {
      return lhs;
    }
    SpectatorHistogram lhsHisto = (SpectatorHistogram) lhs;
    SpectatorHistogram rhsHisto = (SpectatorHistogram) rhs;
    lhsHisto.merge(rhsHisto);
    return lhsHisto;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new SpectatorHistogramAggregatorFactory(name, name);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public Object deserialize(Object serializedHistogram)
  {
    return SpectatorHistogram.deserialize(serializedHistogram);
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return TYPE;
  }

  @Override
  public ColumnType getResultType()
  {
    return TYPE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return SpectatorHistogram.getMaxIntermdiateHistogramSize();
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new ObjectAggregateCombiner<SpectatorHistogram>()
    {
      private SpectatorHistogram combined = null;

      @Override
      public void reset(final ColumnValueSelector selector)
      {
        combined = null;
        fold(selector);
      }

      @Override
      public void fold(final ColumnValueSelector selector)
      {
        SpectatorHistogram other = (SpectatorHistogram) selector.getObject();
        if (other == null) {
          return;
        }
        if (combined == null) {
          combined = new SpectatorHistogram();
        }
        combined.merge(other);
      }

      @Nullable
      @Override
      public SpectatorHistogram getObject()
      {
        return combined;
      }

      @Override
      public Class<SpectatorHistogram> classOfObject()
      {
        return SpectatorHistogram.class;
      }
    };
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    final SpectatorHistogramAggregatorFactory that = (SpectatorHistogramAggregatorFactory) o;

    return Objects.equals(name, that.name) &&
           Objects.equals(fieldName, that.fieldName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{"
           + "name=" + name
           + ", fieldName=" + fieldName
           + "}";
  }

  @JsonTypeName(SpectatorHistogramAggregatorFactory.Timer.TYPE_NAME)
  public static class Timer extends SpectatorHistogramAggregatorFactory
  {
    public static final String TYPE_NAME = "spectatorHistogramTimer";
    public static final ColumnType TYPE = ColumnType.ofComplex(TYPE_NAME);

    public Timer(
        @JsonProperty("name") final String name,
        @JsonProperty("fieldName") final String fieldName
    )
    {
      super(name, fieldName, AggregatorUtil.SPECTATOR_HISTOGRAM_TIMER_CACHE_TYPE_ID);
    }

    public Timer(final String name, final String fieldName, final byte cacheTypeId)
    {
      super(name, fieldName, cacheTypeId);
    }

    @Override
    public ColumnType getIntermediateType()
    {
      return TYPE;
    }

    @Override
    public ColumnType getResultType()
    {
      return TYPE;
    }

    @Override
    public AggregatorFactory getCombiningFactory()
    {
      return new SpectatorHistogramAggregatorFactory.Timer(getName(), getName());
    }
  }

  @JsonTypeName(SpectatorHistogramAggregatorFactory.Distribution.TYPE_NAME)
  public static class Distribution extends SpectatorHistogramAggregatorFactory
  {
    public static final String TYPE_NAME = "spectatorHistogramDistribution";
    public static final ColumnType TYPE = ColumnType.ofComplex(TYPE_NAME);

    public Distribution(
        @JsonProperty("name") final String name,
        @JsonProperty("fieldName") final String fieldName
    )
    {
      super(name, fieldName, AggregatorUtil.SPECTATOR_HISTOGRAM_DISTRIBUTION_CACHE_TYPE_ID);
    }

    public Distribution(final String name, final String fieldName, final byte cacheTypeId)
    {
      super(name, fieldName, cacheTypeId);
    }

    @Override
    public ColumnType getIntermediateType()
    {
      return TYPE;
    }

    @Override
    public ColumnType getResultType()
    {
      return TYPE;
    }

    @Override
    public AggregatorFactory getCombiningFactory()
    {
      return new SpectatorHistogramAggregatorFactory.Distribution(getName(), getName());
    }
  }
}
