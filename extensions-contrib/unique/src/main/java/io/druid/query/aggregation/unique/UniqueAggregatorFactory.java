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

package io.druid.query.aggregation.unique;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.Comparators;
import io.druid.query.aggregation.AggregateCombiner;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.ObjectAggregateCombiner;
import io.druid.query.cache.CacheKeyBuilder;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;


public class UniqueAggregatorFactory extends AggregatorFactory
{
  private static final int DEFAULT_BITMAP_BYTES = 512 * 1024;
  private static final int DEFAULT_MIN_BITMAP_BYTES = 1024;
  private static final int DEFAULT_MAX_BITMAP_BYTES = 256 * 1024 * 1024;

  private final String name;
  private final String fieldName;
  private final Integer maxCardinality;

  @JsonCreator
  public UniqueAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @Nullable @JsonProperty("maxCardinality") Integer maxCardinality
  )
  {
    this.name = Objects.requireNonNull(name);
    this.fieldName = Objects.requireNonNull(fieldName);
    this.maxCardinality = maxCardinality;
  }

  private int computeMaxSize(Integer maxCardinality)
  {
    if (maxCardinality == null) {
      return DEFAULT_BITMAP_BYTES;
    } else {
      int size = (int) Math.ceil(maxCardinality / 8 / 1024 + 128);
      return Math.min(Math.max(size, DEFAULT_MIN_BITMAP_BYTES), DEFAULT_MAX_BITMAP_BYTES);
    }
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new UniqueBuildAggregator(metricFactory.makeColumnValueSelector(getFieldName()));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new UniqueBufferAggregator(metricFactory.makeColumnValueSelector(getFieldName()));
  }

  @Override
  public Comparator getComparator()
  {
    return Comparators.naturalNullsFirst();
  }

  @Override
  public ImmutableRoaringBitmap combine(Object lhs, Object rhs)
  {
    if (rhs == null) {
      return (ImmutableRoaringBitmap) lhs;
    }
    if (lhs == null) {
      return (ImmutableRoaringBitmap) rhs;
    }
    return ImmutableRoaringBitmap.or((ImmutableRoaringBitmap) lhs, (ImmutableRoaringBitmap) rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new UniqueAggregatorFactory(name, name, maxCardinality);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    AggregatorFactory aggregatorFactory = new UniqueAggregatorFactory(name, fieldName, maxCardinality);
    return Collections.singletonList(aggregatorFactory);
  }

  @Override
  public ImmutableRoaringBitmap deserialize(Object object)
  {
    ByteBuffer buffer;

    if (object instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      buffer = ((ByteBuffer) object).duplicate();
    } else if (object instanceof String) {
      buffer = ByteBuffer.wrap(Base64.getDecoder().decode(StringUtils.toUtf8((String) object)));
    } else if (object instanceof ImmutableRoaringBitmap) {
      return (ImmutableRoaringBitmap) object;
    } else if (object instanceof int[]) {
      IntegratedIntCompressor iic = new IntegratedIntCompressor();
      int[] uncompress = iic.uncompress((int[]) object);
      return MutableRoaringBitmap.bitmapOf(uncompress);
    } else if (object instanceof List<?>) {
      List<Integer> integers = (List<Integer>) object;
      IntegratedIntCompressor iic = new IntegratedIntCompressor();
      return MutableRoaringBitmap.bitmapOf(iic.uncompress(integers.stream().mapToInt(i -> i).toArray()));
    } else {
      throw new IAE("Object is not of a type that can be deserialized to an ImmutableRoaringBitmap:"
                    + object.getClass().getName());
    }
    return new ImmutableRoaringBitmap(buffer);
  }

  @Override
  public Integer finalizeComputation(Object object)
  {
    return ((ImmutableRoaringBitmap) object).getCardinality();
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

  @Nullable
  @JsonProperty
  public Integer getMaxCardinality()
  {
    return maxCardinality;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new ObjectAggregateCombiner<MutableRoaringBitmap>()
    {
      private MutableRoaringBitmap bitmap;

      @Nullable
      @Override
      public MutableRoaringBitmap getObject()
      {
        return bitmap;
      }

      @Override
      public Class<MutableRoaringBitmap> classOfObject()
      {
        return MutableRoaringBitmap.class;
      }

      @Override
      public void reset(ColumnValueSelector selector)
      {
        bitmap = null;
        fold(selector);
      }

      @Override
      public void fold(ColumnValueSelector selector)
      {
        ImmutableRoaringBitmap other = (ImmutableRoaringBitmap) selector.getObject();
        if (other == null) {
          return;
        }
        if (bitmap == null) {
          bitmap = new MutableRoaringBitmap();
        }
        bitmap.or(other);
      }
    };
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    return super.getMergingFactory(other);
  }

  @Override
  public String getTypeName()
  {
    return DruidUniqueModule.TYPE_NAME;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return computeMaxSize(maxCardinality);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.UNIQUE_CACHE_TYPE_ID)
        .appendString(name)
        .appendString(fieldName)
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
    UniqueAggregatorFactory that = (UniqueAggregatorFactory) o;
    return Objects.equals(name, that.name) &&
           Objects.equals(fieldName, that.fieldName) &&
           Objects.equals(maxCardinality, that.maxCardinality);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, maxCardinality);
  }

  @Override
  public String toString()
  {
    return "UniqueAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", maxCardinality=" + maxCardinality +
           '}';
  }
}
