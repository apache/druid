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

package io.druid.query.aggregation.cardinality;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.hll.HyperLogLogCollector;
import io.druid.java.util.common.StringUtils;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.aggregation.AggregateCombiner;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.NoopAggregator;
import io.druid.query.aggregation.NoopBufferAggregator;
import io.druid.query.aggregation.cardinality.types.CardinalityAggregatorColumnSelectorStrategy;
import io.druid.query.aggregation.cardinality.types.CardinalityAggregatorColumnSelectorStrategyFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.cache.CacheKeyBuilder;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionHandlerUtils;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CardinalityAggregatorFactory extends AggregatorFactory
{
  private static List<String> makeRequiredFieldNamesFromFields(List<DimensionSpec> fields)
  {
    return ImmutableList.copyOf(
        Lists.transform(
            fields,
            new Function<DimensionSpec, String>()
            {
              @Override
              public String apply(DimensionSpec input)
              {
                return input.getDimension();
              }
            }
        )
    );
  }

  private static List<DimensionSpec> makeFieldsFromFieldNames(List<String> fieldNames)
  {
    return ImmutableList.copyOf(
        Lists.transform(
            fieldNames,
            new Function<String, DimensionSpec>()
            {
              @Override
              public DimensionSpec apply(String input)
              {
                return new DefaultDimensionSpec(input, input);
              }
            }
        )
    );
  }

  private static final CardinalityAggregatorColumnSelectorStrategyFactory STRATEGY_FACTORY =
      new CardinalityAggregatorColumnSelectorStrategyFactory();

  private final String name;
  private final List<DimensionSpec> fields;
  private final boolean byRow;
  private final boolean round;

  @JsonCreator
  public CardinalityAggregatorFactory(
      @JsonProperty("name") String name,
      @Deprecated @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("fields") final List<DimensionSpec> fields,
      @JsonProperty("byRow") final boolean byRow,
      @JsonProperty("round") final boolean round
  )
  {
    this.name = name;
    // 'fieldNames' is deprecated, since CardinalityAggregatorFactory now accepts DimensionSpecs instead of Strings.
    // The old 'fieldNames' is still supported for backwards compatibility, but the user is not allowed to specify both
    // 'fields' and 'fieldNames'.
    if (fields == null) {
      Preconditions.checkArgument(fieldNames != null, "Must provide 'fieldNames' if 'fields' is null.");
      this.fields = makeFieldsFromFieldNames(fieldNames);
    } else {
      Preconditions.checkArgument(fieldNames == null, "Cannot specify both 'fieldNames' and 'fields.");
      this.fields = fields;
    }
    this.byRow = byRow;
    this.round = round;
  }

  public CardinalityAggregatorFactory(
      String name,
      final List<DimensionSpec> fields,
      final boolean byRow
  )
  {
    this(name, null, fields, byRow, false);
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnFactory)
  {
    ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] selectorPluses =
        DimensionHandlerUtils.createColumnSelectorPluses(
            STRATEGY_FACTORY,
            fields,
            columnFactory
        );

    if (selectorPluses.length == 0) {
      return NoopAggregator.instance();
    }
    return new CardinalityAggregator(name, selectorPluses, byRow);
  }


  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] selectorPluses =
        DimensionHandlerUtils.createColumnSelectorPluses(
            STRATEGY_FACTORY,
            fields,
            columnFactory
        );

    if (selectorPluses.length == 0) {
      return NoopBufferAggregator.instance();
    }
    return new CardinalityBufferAggregator(selectorPluses, byRow);
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator<HyperLogLogCollector>()
    {
      @Override
      public int compare(HyperLogLogCollector lhs, HyperLogLogCollector rhs)
      {
        return lhs.compareTo(rhs);
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
    return ((HyperLogLogCollector) lhs).fold((HyperLogLogCollector) rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new HyperLogLogCollectorAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HyperUniquesAggregatorFactory(name, name, false, round);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return fields.stream()
                 .map(
                     field ->
                         new CardinalityAggregatorFactory(
                             field.getOutputName(),
                             null,
                             Collections.singletonList(field),
                             byRow,
                             round
                         )
                 )
                 .collect(Collectors.toList());
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

    return HyperLogLogCollector.makeCollector(buffer);
  }

  @Override

  public Object finalizeComputation(Object object)
  {
    return HyperUniquesAggregatorFactory.estimateCardinality(object, round);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return makeRequiredFieldNamesFromFields(fields);
  }

  @JsonProperty
  public List<DimensionSpec> getFields()
  {
    return fields;
  }

  @JsonProperty
  public boolean isByRow()
  {
    return byRow;
  }

  @JsonProperty
  public boolean isRound()
  {
    return round;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.CARD_CACHE_TYPE_ID)
        .appendCacheables(fields)
        .appendBoolean(byRow)
        .appendBoolean(round)
        .build();
  }

  @Override
  public String getTypeName()
  {
    return "hyperUnique";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return HyperLogLogCollector.getLatestNumBytesForDenseStorage();
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
    final CardinalityAggregatorFactory that = (CardinalityAggregatorFactory) o;
    return byRow == that.byRow &&
           round == that.round &&
           Objects.equals(name, that.name) &&
           Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fields, byRow, round);
  }

  @Override
  public String toString()
  {
    return "CardinalityAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fields=" + fields +
           ", byRow=" + byRow +
           ", round=" + round +
           '}';
  }
}
