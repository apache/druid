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

package org.apache.druid.query.aggregation.ddsketch;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.curator.shaded.com.google.common.math.IntMath;
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

/**
 * Aggregation operations over the ddsketch quantile sketch
 * available on <a href="https://github.com/DataDog/sketches-java">github</a> and described
 * in the paper
 * <a href="https://blog.acolyer.org/2019/09/06/ddsketch/">
 * Computing relative error quantiles using ddsketch</a>.
 * <p>
 */
@JsonTypeName(DDSketchAggregatorFactory.TYPE_NAME)
public class DDSketchAggregatorFactory extends AggregatorFactory
{
  // Default relative error
  public static final double DEFAULT_RELATIVE_ERROR = 0.01;

  // Default num bins
  public static final int DEFAULT_NUM_BINS = 1000;

  @Nonnull
  private final String name;
  @Nonnull
  private final String fieldName;

  private final double relativeError;

  private final int numBins;

  private final byte cacheTypeId;

  public static final String TYPE_NAME = "ddSketch";
  public static final ColumnType TYPE = ColumnType.ofComplex(TYPE_NAME);

  @JsonCreator
  public DDSketchAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("relativeError") final Double relativeError,
      @JsonProperty("numBins") final Integer numBins
  )
  {
    this(name, fieldName, relativeError, numBins, AggregatorUtil.DDSKETCH_CACHE_TYPE_ID);
  }

  DDSketchAggregatorFactory(
      final String name,
      final String fieldName,
      @Nullable final Double relativeError,
      @Nullable final Integer numBins,
      final byte cacheTypeId
  )
  {
    this.name = Objects.requireNonNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName = Objects.requireNonNull(fieldName, "Parameter fieldName must be specified");
    this.relativeError = relativeError == null ? DEFAULT_RELATIVE_ERROR : relativeError;
    this.numBins = numBins == null ? DEFAULT_NUM_BINS : numBins;
    this.cacheTypeId = cacheTypeId;
  }


  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(
        cacheTypeId
    ).appendString(fieldName).appendDouble(relativeError).appendInt(numBins).build();
  }


  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new DDSketchAggregator(metricFactory.makeColumnValueSelector(fieldName), relativeError, numBins);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new DDSketchBufferAggregator(metricFactory.makeColumnValueSelector(fieldName), relativeError, numBins);
  }

  public static final Comparator<DDSketch> COMPARATOR = Comparator.nullsFirst(
      Comparator.comparingLong(a -> a.serializedSize())
  );

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
    DDSketch union = (DDSketch) lhs;
    union.mergeWith((DDSketch) rhs);
    return union;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DDSketchAggregatorFactory(name, name, relativeError, numBins);
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
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(
        new DDSketchAggregatorFactory(
            name,
            fieldName,
            relativeError,
            numBins
        )
    );
  }

  @Override
  public Object deserialize(Object serializedSketch)
  {
    return DDSketchUtils.deserialize(serializedSketch);
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : ((DDSketch) object).getCount();
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

  @JsonProperty
  public double getRelativeError()
  {
    return relativeError;
  }

  @JsonProperty
  public int getNumBins()
  {
    return numBins;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  /**
   * actual type is {@link DDSketch}
   */
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

  /*
   * Each bounded lower collapsing store yields a max size of numBins * 8 bytes (size Of Double) in terms of size.
   * Since the sketch contains a store for positive values and negative values, a fully filled sketch at maximum would contain:
   * 2 * numBins * 8Bytes for storage. Other tracked members of the serialized sketch are constant,
   * so we add 12 to account for these members. These members include mapping reconstruction, and zero counts.
   * These are tracked through doubles and integers and do not increase in size as the sketch accepts new values and merged.
   *
   */
  @Override
  public int getMaxIntermediateSize()
  {
    return IntMath.checkedMultiply(numBins, Double.BYTES * 2) // Postive + Negative Stores
        + Double.BYTES // zeroCount
        + Double.BYTES // gamma
        + Double.BYTES // indexOffset
        + Integer.BYTES // interpolationEnum
        + 12;  // collective protoscope descriptor max sizes

  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new DDSketchAggregatorFactory(newName, getFieldName(), getRelativeError(), getNumBins(), cacheTypeId);
  }

  @Override
  public AggregateCombiner<DDSketch> makeAggregateCombiner()
  {
    return new ObjectAggregateCombiner<DDSketch>()
    {
      private DDSketch combined = DDSketches.collapsingLowestDense(relativeError, numBins);

      @Override
      public void reset(final ColumnValueSelector selector)
      {
        combined.clear();
        fold(selector);
      }

      @Override
      public void fold(final ColumnValueSelector selector)
      {
        DDSketch other = (DDSketch) selector.getObject();
        if (other == null) {
          return;
        }
        combined.mergeWith(other);
      }

      @Nullable
      @Override
      public DDSketch getObject()
      {
        return combined;
      }

      @Override
      public Class<DDSketch> classOfObject()
      {
        return DDSketch.class;
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
    final DDSketchAggregatorFactory that = (DDSketchAggregatorFactory) o;

    return Objects.equals(name, that.name) &&
           Objects.equals(fieldName, that.fieldName) &&
           relativeError == that.relativeError &&
           numBins == that.numBins;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, relativeError, numBins);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{"
           + "name=" + name
           + ", fieldName=" + fieldName
           + ", relativeError=" + relativeError
           + ", numBins=" + numBins
           + "}";
  }
}
