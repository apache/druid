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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class HistogramAggregatorFactory extends AggregatorFactory
{
  private final String name;
  private final String fieldName;
  private final List<Float> breaksList;

  private final float[] breaks;

  @JsonCreator
  public HistogramAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("breaks") final List<Float> breaksList
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.breaksList = (breaksList == null) ? Lists.<Float>newArrayList() : breaksList;
    this.breaks = new float[this.breaksList.size()];
    for (int i = 0; i < this.breaksList.size(); ++i) {
      this.breaks[i] = this.breaksList.get(i);
    }
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new HistogramAggregator(metricFactory.makeFloatColumnSelector(fieldName), breaks);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new HistogramBufferAggregator(
        metricFactory.makeFloatColumnSelector(fieldName),
        breaks
    );
  }

  @Override
  public Comparator getComparator()
  {
    return HistogramAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return HistogramAggregator.combineHistograms(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HistogramAggregatorFactory(name, name, breaksList);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new HistogramAggregatorFactory(fieldName, fieldName, breaksList));
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return Histogram.fromBytes((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      return Histogram.fromBytes((ByteBuffer) object);
    } else if (object instanceof String) {
      byte[] bytes = Base64.decodeBase64(StringUtils.toUtf8((String) object));
      return Histogram.fromBytes(bytes);
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((Histogram) object).asVisual();
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
  public List<Float> getBreaks()
  {
    return breaksList;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    ByteBuffer buf = ByteBuffer
        .allocate(1 + fieldNameBytes.length + Floats.BYTES * breaks.length)
        .put(AggregatorUtil.HIST_CACHE_TYPE_ID)
        .put(fieldNameBytes)
        .put((byte) 0xFF);
    buf.asFloatBuffer().put(breaks);

    return buf.array();
  }

  @Override
  public String getTypeName()
  {
    return "histogram";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES * (breaks.length + 1) + Floats.BYTES * 2;
  }

  @Override
  public String toString()
  {
    return "HistogramAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", breaks=" + Arrays.toString(breaks) +
           '}';
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

    HistogramAggregatorFactory that = (HistogramAggregatorFactory) o;

    if (!Arrays.equals(breaks, that.breaks)) {
      return false;
    }
    if (breaksList != null ? !breaksList.equals(that.breaksList) : that.breaksList != null) {
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
    result = 31 * result + (breaksList != null ? breaksList.hashCode() : 0);
    result = 31 * result + (breaks != null ? Arrays.hashCode(breaks) : 0);
    return result;
  }
}
