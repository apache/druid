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

package org.apache.druid.query.aggregation.cardinality.accurate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.NoopAggregator;
import org.apache.druid.query.aggregation.NoopBufferAggregator;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.Collector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.CollectorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.RoaringBitmapCollectorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.types.AccurateCardinalityAggregatorColumnSelectorStrategy;
import org.apache.druid.query.aggregation.cardinality.accurate.types.AccurateCardinalityAggregatorColumnSelectorStrategyFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ValueType;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;


public class AccurateCardinalityAggregatorFactory extends AggregatorFactory
{
  private static final AccurateCardinalityAggregatorColumnSelectorStrategyFactory STRATEGY_FACTORY =
      new AccurateCardinalityAggregatorColumnSelectorStrategyFactory();

  private static final CollectorFactory DEFAULT_BITMAP_FACTORY = new RoaringBitmapCollectorFactory();

  private final String name;
  private final DimensionSpec field;
  private final CollectorFactory collectorFactory;

  @JsonCreator
  public AccurateCardinalityAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("field") final DimensionSpec field,
      @JsonProperty("collectorFactory") CollectorFactory collectorFactory
  )
  {
    this.name = name;
    this.field = field;
    this.collectorFactory = collectorFactory == null ? DEFAULT_BITMAP_FACTORY : collectorFactory;
  }

  public AccurateCardinalityAggregatorFactory(
      String name,
      DimensionSpec field
  )
  {
    this(name, field, DEFAULT_BITMAP_FACTORY);
  }

  public AccurateCardinalityAggregatorFactory(
      String name,
      String field,
      CollectorFactory collectorFactory
  )
  {
    this(name, new DefaultDimensionSpec(field, field, ValueType.LONG), collectorFactory);
  }

  @JsonProperty
  public DimensionSpec getField()
  {
    return field;
  }

  @JsonProperty
  public CollectorFactory getCollectorFactory()
  {
    return collectorFactory;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnFactory)
  {
    ColumnSelectorPlus<AccurateCardinalityAggregatorColumnSelectorStrategy> selectorPlus = DimensionHandlerUtils.createColumnSelectorPlus(
        STRATEGY_FACTORY,
        field,
        columnFactory
    );
    if (selectorPlus == null) {
      return NoopAggregator.instance();
    }
    return new AccurateCardinalityAggregator(name, selectorPlus, collectorFactory.makeEmptyCollector());
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    ColumnSelectorPlus<AccurateCardinalityAggregatorColumnSelectorStrategy> selectorPlus = DimensionHandlerUtils.createColumnSelectorPlus(
        STRATEGY_FACTORY,
        field,
        columnFactory
    );
    if (selectorPlus == null) {
      return NoopBufferAggregator.instance();
    }
    return new AccurateCardinalityBufferAggregator(selectorPlus, collectorFactory);
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator<Collector>()
    {
      @Override
      public int compare(Collector c1, Collector c2)
      {
        return c1.compareTo(c2);
      }
    };
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    Collector lhsCollector = (Collector) lhs;
    return lhsCollector.fold((Collector) rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new BitmapAggregatorFactory(name, name);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Lists.transform(
        ImmutableList.of(field),
        new Function<DimensionSpec, AggregatorFactory>()
        {
          @Override
          public AggregatorFactory apply(DimensionSpec input)
          {
            return new AccurateCardinalityAggregatorFactory(input.getOutputName(), input, collectorFactory);
          }
        }
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    final ByteBuffer buffer;

    if (object instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      // Be conservative, don't assume we own this buffer.
      buffer = ((ByteBuffer) object).duplicate();
    } else if (object instanceof String) {
      buffer = ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)));
    } else {
      return object;
    }
    return collectorFactory.makeCollector(buffer);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    if (object == null) {
      return 0;
    }
    return ((Collector) object).getCardinality();
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of(field.getOutputName());
  }

  @Override
  public String getTypeName()
  {
    return AccurateCardinalityModule.BITMAP_COLLECTOR;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 1024;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimSpecKey = field.getCacheKey();
    ByteBuffer retBuf = ByteBuffer.allocate(2 + dimSpecKey.length);
    retBuf.put(AggregatorUtil.ACCURATE_CARDINALITY_CACHE_TYPE_ID);
    retBuf.put(dimSpecKey);

    return retBuf.array();
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
    AccurateCardinalityAggregatorFactory factory = (AccurateCardinalityAggregatorFactory) o;
    return Objects.equal(name, factory.name) &&
           Objects.equal(field, factory.field) &&
           Objects.equal(collectorFactory, factory.collectorFactory);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(name, field, collectorFactory);
  }
}
