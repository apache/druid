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

package org.apache.druid.query.aggregation.longunique;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
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
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class LongUniqueAggregatorFactory extends AggregatorFactory
{
  static final Comparator COMPARATOR = Ordering.from(Comparator.comparingLong(o -> ((Roaring64NavigableMap) o).getLongCardinality()))
          .nullsFirst();

  private static final int DEFAULT_BITMAP_BYTES = 512 * 1024;
  private static final int DEFAULT_MIN_BITMAP_BYTES = 1024;
  private static final int DEFAULT_MAX_BITMAP_BYTES = 256 * 1024 * 1024;


  private final String name;
  private final String fieldName;
  private final Integer maxCardinality;

  public static final ColumnType TYPE = ColumnType.ofComplex("longUnique");

  @JsonCreator
  public LongUniqueAggregatorFactory(
          @JsonProperty("name") String name,
          @JsonProperty("fieldName") String fieldName,
          @Nullable @JsonProperty("maxCardinality") Integer maxCardinality
  )
  {
    this.name = Objects.requireNonNull(name);
    this.fieldName = Objects.requireNonNull(fieldName);
    this.maxCardinality = computeMaxSize(maxCardinality);
  }

  private int computeMaxSize(Integer maxCardinality)
  {
    if (maxCardinality == null) {
      return DEFAULT_BITMAP_BYTES;
    } else {
      int size = (int) Math.ceil((Integer) (maxCardinality / 8 / 1024) + 128);
      return Math.min(Math.max(size, DEFAULT_MIN_BITMAP_BYTES), DEFAULT_MAX_BITMAP_BYTES);
    }
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new LongUniqueBuildAggregator(metricFactory.makeColumnValueSelector(getFieldName()));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new LongUniqueBufferAggregator(metricFactory.makeColumnValueSelector(getFieldName()));
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Roaring64NavigableMap combine(Object lhs, Object rhs)
  {
    if (rhs == null) {
      return (Roaring64NavigableMap) lhs;
    }
    if (lhs == null) {
      return (Roaring64NavigableMap) rhs;
    }
    Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();
    roaring64NavigableMap.or((Roaring64NavigableMap) lhs);
    roaring64NavigableMap.or((Roaring64NavigableMap) rhs);
    return roaring64NavigableMap;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongUniqueAggregatorFactory(name, name, maxCardinality);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    AggregatorFactory aggregatorFactory = new LongUniqueAggregatorFactory(name, fieldName, maxCardinality);
    return Collections.singletonList(aggregatorFactory);
  }

  @Override
  public Roaring64NavigableMap deserialize(Object object)
  {
    if (object == null) {
      return new Roaring64NavigableMap();
    }
    ByteBuffer buffer;
    byte[] bytes;
    if (object instanceof byte[]) {
      bytes = (byte[]) object;
    } else if (object instanceof ByteBuffer) {
      buffer = ((ByteBuffer) object).duplicate();
      bytes = new byte[buffer.remaining()];
      buffer.get(bytes, 0, bytes.length);
    } else if (object instanceof String) {
      buffer = ByteBuffer.wrap(Base64.getDecoder().decode(StringUtils.toUtf8((String) object)));
      bytes = new byte[buffer.remaining()];
      buffer.get(bytes, 0, bytes.length);
    } else if (object instanceof Roaring64NavigableMap) {
      return (Roaring64NavigableMap) object;
    } else {
      throw new IAE("Object is not of a type that can be deserialized to an Roaring64NavigableMap:"
              + object.getClass().getName());
    }
    return getRoaring64NavigableMap(bytes);
  }

  public static Roaring64NavigableMap getRoaring64NavigableMap(byte[] bytes)
  {
    Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();
    InputStream inputStream = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(inputStream);
    try {
      roaring64NavigableMap.deserialize(dis);
    }
    catch (IOException e) {
      throw new IAE("Bytes can not be deserialized to an Roaring64NavigableMap: ",
              e.getMessage());
    }
    return roaring64NavigableMap;
  }

  @Override
  public Long finalizeComputation(Object object)
  {
    return ((Roaring64NavigableMap) object).getLongCardinality();
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
  public ColumnType getType() 
  {
    return TYPE;
  }

  @Override
  public ColumnType getFinalizedType()
  {
    return TYPE;
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new ObjectAggregateCombiner<Roaring64NavigableMap>()
    {
      private Roaring64NavigableMap bitmap;

      @Nullable
      @Override
      public Roaring64NavigableMap getObject()
      {
        return bitmap;
      }

      @Override
      public Class<Roaring64NavigableMap> classOfObject()
      {
        return Roaring64NavigableMap.class;
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
        Roaring64NavigableMap other = (Roaring64NavigableMap) selector.getObject();
        if (other == null) {
          return;
        }
        if (bitmap == null) {
          bitmap = new Roaring64NavigableMap();
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
  public int getMaxIntermediateSize()
  {
    return this.maxCardinality;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.LONG_UNIQUE_CACHE_TYPE_ID)
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
    LongUniqueAggregatorFactory that = (LongUniqueAggregatorFactory) o;
    return Objects.equals(name, that.name) &&
            Objects.equals(fieldName, that.fieldName);
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
            ", maxCardinality='" + maxCardinality + '\'' +
            '}';
  }
}
