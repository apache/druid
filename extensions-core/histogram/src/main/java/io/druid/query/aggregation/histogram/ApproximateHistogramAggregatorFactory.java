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

package io.druid.query.aggregation.histogram;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;

import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("approxHistogram")
public class ApproximateHistogramAggregatorFactory extends AggregatorFactory
{
  protected final String name;
  protected final String fieldName;

  protected final int resolution;
  protected final int numBuckets;

  protected final float lowerLimit;
  protected final float upperLimit;

  @JsonCreator
  public ApproximateHistogramAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("resolution") Integer resolution,
      @JsonProperty("numBuckets") Integer numBuckets,
      @JsonProperty("lowerLimit") Float lowerLimit,
      @JsonProperty("upperLimit") Float upperLimit

  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.resolution = resolution == null ? ApproximateHistogram.DEFAULT_HISTOGRAM_SIZE : resolution;
    this.numBuckets = numBuckets == null ? ApproximateHistogram.DEFAULT_BUCKET_SIZE : numBuckets;
    this.lowerLimit = lowerLimit == null ? Float.NEGATIVE_INFINITY : lowerLimit;
    this.upperLimit = upperLimit == null ? Float.POSITIVE_INFINITY : upperLimit;

    Preconditions.checkArgument(this.resolution > 0, "resolution must be greater than 1");
    Preconditions.checkArgument(this.numBuckets > 0, "numBuckets must be greater than 1");
    Preconditions.checkArgument(this.upperLimit > this.lowerLimit, "upperLimit must be greater than lowerLimit");
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new ApproximateHistogramAggregator(
        metricFactory.makeFloatColumnSelector(fieldName),
        resolution,
        lowerLimit,
        upperLimit
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new ApproximateHistogramBufferAggregator(
        metricFactory.makeFloatColumnSelector(fieldName),
        resolution,
        lowerLimit,
        upperLimit
    );
  }

  @Override
  public Comparator getComparator()
  {
    return ApproximateHistogramAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return ApproximateHistogramAggregator.combineHistograms(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new ApproximateHistogramFoldingAggregatorFactory(name, name, resolution, numBuckets, lowerLimit, upperLimit);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof ApproximateHistogramAggregatorFactory) {
      ApproximateHistogramAggregatorFactory castedOther = (ApproximateHistogramAggregatorFactory) other;

      return new ApproximateHistogramFoldingAggregatorFactory(
          name,
          name,
          Math.max(resolution, castedOther.resolution),
          numBuckets,
          Math.min(lowerLimit, castedOther.lowerLimit),
          Math.max(upperLimit, castedOther.upperLimit)
      );

    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(
        new ApproximateHistogramAggregatorFactory(
            fieldName,
            fieldName,
            resolution,
            numBuckets,
            lowerLimit,
            upperLimit
        )
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      final ApproximateHistogram ah = ApproximateHistogram.fromBytes((byte[]) object);
      ah.setLowerLimit(lowerLimit);
      ah.setUpperLimit(upperLimit);

      return ah;
    } else if (object instanceof ByteBuffer) {
      final ApproximateHistogram ah = ApproximateHistogram.fromBytes((ByteBuffer) object);
      ah.setLowerLimit(lowerLimit);
      ah.setUpperLimit(upperLimit);

      return ah;
    } else if (object instanceof String) {
      byte[] bytes = Base64.decodeBase64(StringUtils.toUtf8((String) object));
      final ApproximateHistogram ah = ApproximateHistogram.fromBytes(bytes);
      ah.setLowerLimit(lowerLimit);
      ah.setUpperLimit(upperLimit);

      return ah;
    } else {
      return object;
    }
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((ApproximateHistogram) object).toHistogram(numBuckets);
  }

  @JsonProperty
  @Override
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
  public int getResolution()
  {
    return resolution;
  }

  @JsonProperty
  public float getLowerLimit()
  {
    return lowerLimit;
  }

  @JsonProperty
  public float getUpperLimit()
  {
    return upperLimit;
  }

  @JsonProperty
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length + Ints.BYTES * 2 + Floats.BYTES * 2)
                     .put(AggregatorUtil.APPROX_HIST_CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .putInt(resolution)
                     .putInt(numBuckets)
                     .putFloat(lowerLimit)
                     .putFloat(upperLimit).array();
  }

  @Override
  public String getTypeName()
  {
    return "approximateHistogram";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return new ApproximateHistogram(resolution).getMaxStorageSize();
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

    ApproximateHistogramAggregatorFactory that = (ApproximateHistogramAggregatorFactory) o;

    if (Float.compare(that.lowerLimit, lowerLimit) != 0) {
      return false;
    }
    if (numBuckets != that.numBuckets) {
      return false;
    }
    if (resolution != that.resolution) {
      return false;
    }
    if (Float.compare(that.upperLimit, upperLimit) != 0) {
      return false;
    }
    if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
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
    result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
    result = 31 * result + resolution;
    result = 31 * result + numBuckets;
    result = 31 * result + (lowerLimit != +0.0f ? Float.floatToIntBits(lowerLimit) : 0);
    result = 31 * result + (upperLimit != +0.0f ? Float.floatToIntBits(upperLimit) : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ApproximateHistogramAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", resolution=" + resolution +
           ", numBuckets=" + numBuckets +
           ", lowerLimit=" + lowerLimit +
           ", upperLimit=" + upperLimit +
           '}';
  }
}
