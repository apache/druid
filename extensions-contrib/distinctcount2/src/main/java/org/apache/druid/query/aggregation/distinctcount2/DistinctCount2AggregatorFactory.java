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

package org.apache.druid.query.aggregation.distinctcount2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.druid.collections.bitmap.BitSetBitmapFactory;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class DistinctCount2AggregatorFactory extends AggregatorFactory
{

  private static final Logger log = new Logger(DistinctCount2AggregatorFactory.class);

  private static final BitmapFactory BITMAP_FACTORY = new BitSetBitmapFactory();
  public static final ColumnType TYPE = ColumnType.ofComplex(DistinctCount2DruidModule.TYPE_NAME);

  private final String name;
  private final String fieldName;
  private final int lgK;
  private final TgtHllType tgtHllType;

  @JsonCreator
  public DistinctCount2AggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("lgK") Integer lgK,
      @JsonProperty("tgtHllType") String tgtHllType
  )
  {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(fieldName);
    this.name = name;
    this.fieldName = fieldName;
    this.lgK = lgK == null || lgK == 0 ? 12 : lgK;
    this.tgtHllType = tgtHllType == null ? TgtHllType.HLL_4 : TgtHllType.valueOf(tgtHllType);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnFactory)
  {
    DimensionSelector selector = makeDimensionSelector(columnFactory);
    if (selector == null) {
      return new NoopDistinctCount2Aggregator();
    } else {
      return new DistinctCount2Aggregator(
          selector,
          BITMAP_FACTORY.makeEmptyMutableBitmap(),
          lgK,
          tgtHllType
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    DimensionSelector selector = makeDimensionSelector(columnFactory);
    if (selector == null) {
      return NoopDistinctCount2BufferAggregator.instance();
    } else {
      return new DistinctCount2BufferAggregator(makeDimensionSelector(columnFactory), lgK, tgtHllType);
    }
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new DistinctCount2AggregatorFactory(newName, getFieldName(), lgK, tgtHllType.name());
  }

  private DimensionSelector makeDimensionSelector(final ColumnSelectorFactory columnFactory)
  {
    return columnFactory.makeDimensionSelector(new DefaultDimensionSpec(fieldName, fieldName));
  }

  public static Comparator<HllSketchHolder> COMPARATOR = Comparator.nullsFirst(
      Comparator.comparingDouble(HllSketchHolder::getEstimate));

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (lhs == null && rhs == null) {
      return 0L;
    }
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
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
        final HllSketchHolder sketch = (HllSketchHolder) selector.getObject();
        union.update(sketch.getSketch());
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
  public AggregatorFactory getCombiningFactory()
  {
    return new DistinctCount2AggregatorFactory(name, name, lgK, tgtHllType.name());
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable final Object object)
  {
    if (object == null) {
      return object;
    }

    final HllSketchHolder sketch = HllSketchHolder.fromObj(object);
    final double estimate = sketch.getEstimate();

    return estimate;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
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
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.DISTINCT_COUNT2_CACHE_KEY)
      .appendString(fieldName)
      .build();
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
  public ColumnType getResultType()
  {
    return ColumnType.LONG;
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return TYPE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return HllSketch.getMaxUpdatableSerializationBytes(lgK, tgtHllType);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DistinctCount2AggregatorFactory)) {
      return false;
    }

    DistinctCount2AggregatorFactory that = (DistinctCount2AggregatorFactory) o;

    if (lgK != that.lgK) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    return Objects.equals(tgtHllType, that.tgtHllType);
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
    result = 31 * result + lgK;
    result = 31 * result + (tgtHllType != null ? tgtHllType.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "DistinctCount2AggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }
}
