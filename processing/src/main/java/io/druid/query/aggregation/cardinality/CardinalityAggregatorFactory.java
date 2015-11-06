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
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import org.apache.commons.codec.binary.Base64;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class CardinalityAggregatorFactory extends AggregatorFactory
{
  public static Object estimateCardinality(Object object)
  {
    if (object == null) {
      return 0;
    }

    return ((HyperLogLogCollector) object).estimateCardinality();
  }

  private static final byte CACHE_TYPE_ID = (byte) 0x8;

  private final String name;
  private final List<String> fieldNames;
  private final boolean byRow;

  @JsonCreator
  public CardinalityAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("byRow") final boolean byRow
  )
  {
    this.name = name;
    this.fieldNames = fieldNames;
    this.byRow = byRow;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnFactory)
  {
    List<DimensionSelector> selectors = makeDimensionSelectors(columnFactory);

    if (selectors.isEmpty()) {
      return Aggregators.noopAggregator();
    }

    return new CardinalityAggregator(name, selectors, byRow);
  }


  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    List<DimensionSelector> selectors = makeDimensionSelectors(columnFactory);

    if (selectors.isEmpty()) {
      return Aggregators.noopBufferAggregator();
    }

    return new CardinalityBufferAggregator(selectors, byRow);
  }

  private List<DimensionSelector> makeDimensionSelectors(final ColumnSelectorFactory columnFactory)
  {
    return Lists.newArrayList(
        Iterables.filter(
            Iterables.transform(
                fieldNames, new Function<String, DimensionSelector>()
            {
              @Nullable
              @Override
              public DimensionSelector apply(@Nullable String input)
              {
                return columnFactory.makeDimensionSelector(new DefaultDimensionSpec(input, input));
              }
            }
            ), Predicates.notNull()
        )
    );
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
  public AggregatorFactory getCombiningFactory()
  {
    return new HyperUniquesAggregatorFactory(name, name);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    throw new UnsupportedOperationException("can't merge CardinalityAggregatorFactory");
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Lists.transform(
        fieldNames,
        new Function<String, AggregatorFactory>()
        {
          @Override
          public AggregatorFactory apply(String input)
          {
            return new CardinalityAggregatorFactory(input, fieldNames, byRow);
          }
        }
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return HyperLogLogCollector.makeCollector(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return HyperLogLogCollector.makeCollector((ByteBuffer) object);
    } else if (object instanceof String) {
      return HyperLogLogCollector.makeCollector(
          ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)))
      );
    }
    return object;
  }

  @Override

  public Object finalizeComputation(Object object)
  {
    return estimateCardinality(object);
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
    return fieldNames;
  }

  @JsonProperty
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  public boolean isByRow()
  {
    return byRow;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(Joiner.on("\u0001").join(fieldNames));

    return ByteBuffer.allocate(2 + fieldNameBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put((byte)(byRow ? 1 : 0))
                     .array();
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
  public Object getAggregatorStartValue()
  {
    return HyperLogLogCollector.makeLatestCollector();
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

    CardinalityAggregatorFactory that = (CardinalityAggregatorFactory) o;

    if (byRow != that.byRow) {
      return false;
    }
    if (fieldNames != null ? !fieldNames.equals(that.fieldNames) : that.fieldNames != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldNames != null ? fieldNames.hashCode() : 0);
    result = 31 * result + (byRow ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "CardinalityAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldNames='" + fieldNames + '\'' +
           '}';
  }
}
