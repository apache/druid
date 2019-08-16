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

package org.apache.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@JsonTypeName(FixedBucketsHistogramAggregator.TYPE_NAME)
public class FixedBucketsHistogramAggregatorFactory extends AggregatorFactory
{
  private static int DEFAULT_NUM_BUCKETS = 10;

  private final String name;
  private final String fieldName;

  private double lowerLimit;
  private double upperLimit;
  private int numBuckets;

  private FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode;

  private boolean finalizeAsBase64Binary;

  @JsonCreator
  public FixedBucketsHistogramAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("numBuckets") @Nullable Integer numBuckets,
      @JsonProperty("lowerLimit") double lowerLimit,
      @JsonProperty("upperLimit") double upperLimit,
      @JsonProperty("outlierHandlingMode") FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode,
      @JsonProperty("finalizeAsBase64Binary") @Nullable Boolean finalizeAsBase64Binary
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.numBuckets = numBuckets == null ? DEFAULT_NUM_BUCKETS : numBuckets;
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.outlierHandlingMode = outlierHandlingMode;
    this.finalizeAsBase64Binary = finalizeAsBase64Binary == null ? false : finalizeAsBase64Binary;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new FixedBucketsHistogramAggregator(
        metricFactory.makeColumnValueSelector(fieldName),
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new FixedBucketsHistogramBufferAggregator(
        metricFactory.makeColumnValueSelector(fieldName),
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode
    );
  }

  @Override
  public Comparator getComparator()
  {
    return FixedBucketsHistogramAggregator.COMPARATOR;
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (lhs == null) {
      if (rhs == null) {
        return null;
      } else {
        return rhs;
      }
    } else {
      ((FixedBucketsHistogram) lhs).combineHistogram((FixedBucketsHistogram) rhs);
      return lhs;
    }
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new ObjectAggregateCombiner()
    {
      private final FixedBucketsHistogram combined = new FixedBucketsHistogram(
          lowerLimit,
          upperLimit,
          numBuckets,
          outlierHandlingMode
      );

      @Override
      public void reset(ColumnValueSelector selector)
      {
        FixedBucketsHistogram first = (FixedBucketsHistogram) selector.getObject();
        combined.combineHistogram(first);
      }

      @Override
      public void fold(ColumnValueSelector selector)
      {
        FixedBucketsHistogram other = (FixedBucketsHistogram) selector.getObject();
        combined.combineHistogram(other);
      }

      @Override
      public FixedBucketsHistogram getObject()
      {
        return combined;
      }

      @Override
      public Class<FixedBucketsHistogram> classOfObject()
      {
        return FixedBucketsHistogram.class;
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new FixedBucketsHistogramAggregatorFactory(
        name,
        name,
        numBuckets,
        lowerLimit,
        upperLimit,
        outlierHandlingMode,
        finalizeAsBase64Binary
    );
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other)
  {
    return new FixedBucketsHistogramAggregatorFactory(
        name,
        name,
        numBuckets,
        lowerLimit,
        upperLimit,
        outlierHandlingMode,
        finalizeAsBase64Binary
    );
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(
        new FixedBucketsHistogramAggregatorFactory(
            fieldName,
            fieldName,
            numBuckets,
            lowerLimit,
            upperLimit,
            outlierHandlingMode,
            finalizeAsBase64Binary
        )
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof String) {
      byte[] bytes = StringUtils.decodeBase64(StringUtils.toUtf8((String) object));
      final FixedBucketsHistogram fbh = FixedBucketsHistogram.fromBytes(bytes);
      return fbh;
    } else {
      return object;
    }
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    if (object == null) {
      return null;
    }

    if (finalizeAsBase64Binary) {
      return object;
    } else {
      return object.toString();
    }
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
    return Collections.singletonList(fieldName);
  }

  @Override
  public String getTypeName()
  {
    return FixedBucketsHistogramAggregator.TYPE_NAME;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return FixedBucketsHistogram.SERDE_HEADER_SIZE + FixedBucketsHistogram.getFullStorageSize(numBuckets);
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(AggregatorUtil.FIXED_BUCKET_HIST_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendInt(outlierHandlingMode.ordinal())
        .appendInt(numBuckets)
        .appendDouble(lowerLimit)
        .appendDouble(upperLimit)
        .appendBoolean(finalizeAsBase64Binary);

    return builder.build();
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public double getLowerLimit()
  {
    return lowerLimit;
  }

  @JsonProperty
  public double getUpperLimit()
  {
    return upperLimit;
  }

  @JsonProperty
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @JsonProperty
  public FixedBucketsHistogram.OutlierHandlingMode getOutlierHandlingMode()
  {
    return outlierHandlingMode;
  }

  @JsonProperty
  public boolean isFinalizeAsBase64Binary()
  {
    return finalizeAsBase64Binary;
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
    FixedBucketsHistogramAggregatorFactory that = (FixedBucketsHistogramAggregatorFactory) o;
    return Double.compare(that.getLowerLimit(), getLowerLimit()) == 0 &&
           Double.compare(that.getUpperLimit(), getUpperLimit()) == 0 &&
           getNumBuckets() == that.getNumBuckets() &&
           Objects.equals(getName(), that.getName()) &&
           Objects.equals(getFieldName(), that.getFieldName()) &&
           getOutlierHandlingMode() == that.getOutlierHandlingMode() &&
           isFinalizeAsBase64Binary() == that.isFinalizeAsBase64Binary();
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getName(),
        getFieldName(),
        getLowerLimit(),
        getUpperLimit(),
        getNumBuckets(),
        getOutlierHandlingMode(),
        isFinalizeAsBase64Binary()
    );
  }

  @Override
  public String toString()
  {
    return "FixedBucketsHistogramAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", lowerLimit=" + lowerLimit +
           ", upperLimit=" + upperLimit +
           ", numBuckets=" + numBuckets +
           ", outlierHandlingMode=" + outlierHandlingMode +
           ", finalizeAsBase64Binary=" + finalizeAsBase64Binary +
           '}';
  }
}
