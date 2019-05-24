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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    this.breaksList = (breaksList == null) ? new ArrayList<>() : breaksList;
    this.breaks = new float[this.breaksList.size()];
    for (int i = 0; i < this.breaksList.size(); ++i) {
      this.breaks[i] = this.breaksList.get(i);
    }
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new HistogramAggregator(metricFactory.makeColumnValueSelector(fieldName), breaks);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new HistogramBufferAggregator(metricFactory.makeColumnValueSelector(fieldName), breaks);
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
  public AggregateCombiner makeAggregateCombiner()
  {
    // HistogramAggregatorFactory.combine() delegates to HistogramAggregator.combineHistograms() and it doesn't check
    // for nulls, so this AggregateCombiner neither.
    return new ObjectAggregateCombiner<Histogram>()
    {
      private Histogram combined;

      @Override
      public void reset(ColumnValueSelector selector)
      {
        Histogram first = (Histogram) selector.getObject();
        if (combined == null) {
          combined = new Histogram(first);
        } else {
          combined.copyFrom(first);
        }
      }

      @Override
      public void fold(ColumnValueSelector selector)
      {
        Histogram other = (Histogram) selector.getObject();
        combined.fold(other);
      }

      @Override
      public Class<Histogram> classOfObject()
      {
        return Histogram.class;
      }

      @Nullable
      @Override
      public Histogram getObject()
      {
        return combined;
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HistogramAggregatorFactory(name, name, breaksList);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(
        new HistogramAggregatorFactory(fieldName, fieldName, breaksList));
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return Histogram.fromBytes((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      return Histogram.fromBytes((ByteBuffer) object);
    } else if (object instanceof String) {
      byte[] bytes = StringUtils.decodeBase64(StringUtils.toUtf8((String) object));
      return Histogram.fromBytes(bytes);
    }
    return object;
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : ((Histogram) object).asVisual();
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
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    ByteBuffer buf = ByteBuffer
        .allocate(1 + fieldNameBytes.length + Float.BYTES * breaks.length)
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
    return Long.BYTES * (breaks.length + 1) + Float.BYTES * 2;
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
