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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.druid.jackson.DefaultTrueJsonIncludeFilter;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.java.util.common.StringEncodingDefaultUTF16LEJsonIncludeFilter;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Base class for both build and merge factories
 */
public abstract class HllSketchAggregatorFactory extends AggregatorFactory
{
  public static final boolean DEFAULT_ROUND = false;
  public static final boolean DEFAULT_SHOULD_FINALIZE = true;
  public static final int DEFAULT_LG_K = 12;
  public static final TgtHllType DEFAULT_TGT_HLL_TYPE = TgtHllType.HLL_4;
  public static final StringEncoding DEFAULT_STRING_ENCODING = StringEncoding.UTF16LE;

  static final Comparator<HllSketchHolder> COMPARATOR =
      Comparator.nullsFirst(Comparator.comparingDouble(HllSketchHolder::getEstimate));

  private final String name;
  private final String fieldName;
  private final int lgK;
  private final TgtHllType tgtHllType;
  private final StringEncoding stringEncoding;
  private final boolean shouldFinalize;
  private final boolean round;

  HllSketchAggregatorFactory(
      final String name,
      final String fieldName,
      @Nullable final Integer lgK,
      @Nullable final String tgtHllType,
      @Nullable final StringEncoding stringEncoding,
      final Boolean shouldFinalize,
      final boolean round
  )
  {
    this.name = Objects.requireNonNull(name);
    this.fieldName = Objects.requireNonNull(fieldName);
    this.lgK = lgK == null ? DEFAULT_LG_K : lgK;
    this.tgtHllType = tgtHllType == null ? DEFAULT_TGT_HLL_TYPE : TgtHllType.valueOf(tgtHllType);
    this.stringEncoding = stringEncoding == null ? DEFAULT_STRING_ENCODING : stringEncoding;
    this.shouldFinalize = shouldFinalize == null ? DEFAULT_SHOULD_FINALIZE : shouldFinalize;
    this.round = round;
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
  public int getLgK()
  {
    return lgK;
  }

  @JsonProperty
  public String getTgtHllType()
  {
    return tgtHllType.toString();
  }

  @JsonProperty
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = StringEncodingDefaultUTF16LEJsonIncludeFilter.class)
  public StringEncoding getStringEncoding()
  {
    return stringEncoding;
  }

  @JsonProperty
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = DefaultTrueJsonIncludeFilter.class)
  public boolean isShouldFinalize()
  {
    return shouldFinalize;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isRound()
  {
    return round;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public HllSketchHolder deserialize(final Object object)
  {
    if (object == null) {
      return HllSketchHolder.of(new HllSketch(lgK, tgtHllType));
    }
    return HllSketchHolder.fromObj(object);
  }

  @Override
  public Object combine(final Object lhs, final Object rhs)
  {
    if (lhs == null) {
      return rhs;
    }

    if (rhs == null) {
      return lhs;
    }
    return ((HllSketchHolder) lhs).merge((HllSketchHolder) rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new ObjectAggregateCombiner<HllSketchHolder>()
    {
      private final Union union = new Union(lgK);

      @Override
      public void reset(final ColumnValueSelector selector)
      {
        union.reset();
        fold(selector);
      }

      @Override
      public void fold(final ColumnValueSelector selector)
      {
        final HllSketchHolder sketchHolder = (HllSketchHolder) selector.getObject();
        // sketchHolder can be null here, if the sketch is empty. This is an optimisation done by
        // HllSketchHolderObjectStrategy. If the holder is null, this should be a no-op.
        if (sketchHolder != null) {
          union.update(sketchHolder.getSketch());
        }
      }

      @Nullable
      @Override
      public HllSketchHolder getObject()
      {
        return HllSketchHolder.of(union.getResult(tgtHllType));
      }

      @Override
      public Class<HllSketchHolder> classOfObject()
      {
        return HllSketchHolder.class;
      }
    };
  }

  @Override
  public ColumnType getResultType()
  {
    if (shouldFinalize) {
      return round ? ColumnType.LONG : ColumnType.DOUBLE;
    } else {
      return getIntermediateType();
    }
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable final Object object)
  {
    if (!shouldFinalize) {
      return object;
    }

    if (object == null) {
      return 0.0D;
    }

    final HllSketchHolder sketch = HllSketchHolder.fromObj(object);
    final double estimate = sketch.getEstimate();

    if (round) {
      return Math.round(estimate);
    } else {
      return estimate;
    }
  }

  @Override
  public Comparator<HllSketchHolder> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HllSketchMergeAggregatorFactory(
        getName(),
        getName(),
        getLgK(),
        getTgtHllType(),
        getStringEncoding(),
        isShouldFinalize(),
        isRound()
    );
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(getCacheTypeId())
        .appendString(name)
        .appendString(fieldName)
        .appendInt(lgK)
        .appendInt(tgtHllType.ordinal())
        .appendCacheable(stringEncoding)
        .build();
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
    HllSketchAggregatorFactory that = (HllSketchAggregatorFactory) o;
    return lgK == that.lgK
           && shouldFinalize == that.shouldFinalize
           && round == that.round
           && Objects.equals(name, that.name)
           && Objects.equals(fieldName, that.fieldName)
           && tgtHllType == that.tgtHllType
           && stringEncoding == that.stringEncoding;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, lgK, tgtHllType, stringEncoding, shouldFinalize, round);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", lgK=" + lgK +
           ", tgtHllType=" + tgtHllType +
           (stringEncoding != DEFAULT_STRING_ENCODING ? ", stringEncoding=" + stringEncoding : "") +
           (shouldFinalize != DEFAULT_SHOULD_FINALIZE ? ", shouldFinalize=" + shouldFinalize : "") +
           (round != DEFAULT_ROUND ? ", round=" + round : "") +
           '}';
  }

  protected abstract byte getCacheTypeId();
}
