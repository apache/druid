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

package io.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.IAE;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesUnion;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.cache.CacheKeyBuilder;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.column.ValueType;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class DoublesSketchAggregatorFactory extends AggregatorFactory
{

  private static final int DEFAULT_K = 128;

  // Used for sketch size estimation.
  private static final long MAX_STREAM_LENGTH = 1_000_000_000;

  private final String name;
  private final String fieldName;
  private final int k;
  private final byte cacheTypeId;

  @JsonCreator
  public DoublesSketchAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("k") final Integer k)
  {
    this(name, fieldName, k, AggregatorUtil.QUANTILES_DOUBLES_SKETCH_BUILD_CACHE_TYPE_ID);
  }

  DoublesSketchAggregatorFactory(final String name, final String fieldName, final Integer k, final byte cacheTypeId)
  {
    if (name == null) {
      throw new IAE("Must have a valid, non-null aggregator name");
    }
    this.name = name;
    if (fieldName == null) {
      throw new IAE("Parameter fieldName must be specified");
    }
    this.fieldName = fieldName;
    this.k = k == null ? DEFAULT_K : k;
    Util.checkIfPowerOf2(this.k, "size");
    this.cacheTypeId = cacheTypeId;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory metricFactory)
  {
    if (metricFactory.getColumnCapabilities(fieldName) != null
        && ValueType.isNumeric(metricFactory.getColumnCapabilities(fieldName).getType())) {
      final ColumnValueSelector<Double> valueSelector = metricFactory.makeColumnValueSelector(fieldName);
      if (valueSelector == null) {
        return new DoublesSketchNoOpAggregator();
      }
      return new DoublesSketchBuildAggregator(valueSelector, k);
    }
    final ColumnValueSelector<DoublesSketch> selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector == null) {
      return new DoublesSketchNoOpAggregator();
    }
    return new DoublesSketchMergeAggregator(selector, k);
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory metricFactory)
  {
    if (metricFactory.getColumnCapabilities(fieldName) != null
        && ValueType.isNumeric(metricFactory.getColumnCapabilities(fieldName).getType())) {
      return getDoubleBufferAggregator(metricFactory.makeColumnValueSelector(fieldName));
    }
    final ColumnValueSelector<DoublesSketch> selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector == null) {
      return new DoublesSketchNoOpBufferAggregator();
    }
    return new DoublesSketchMergeBufferAggregator(selector, k, getMaxIntermediateSize());
  }

  private BufferAggregator getDoubleBufferAggregator(final ColumnValueSelector<Double> selector)
  {
    if (selector == null) {
      return new DoublesSketchNoOpBufferAggregator();
    }
    return new DoublesSketchBuildBufferAggregator(selector, k, getMaxIntermediateSize());
  }

  @Override
  public Object deserialize(final Object object)
  {
    return DoublesSketchOperations.deserialize(object);
  }

  public static final Comparator<DoublesSketch> COMPARATOR = new Comparator<DoublesSketch>()
  {
    @Override
    public int compare(DoublesSketch a, DoublesSketch b)
    {
      return Long.compare(a.getN(), b.getN());
    }
  };

  @Override
  public Comparator<DoublesSketch> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object combine(final Object lhs, final Object rhs)
  {
    final DoublesUnion union = DoublesUnion.builder().setMaxK(k).build();
    union.update((DoublesSketch) lhs);
    union.update((DoublesSketch) rhs);
    return union.getResultAndReset();
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
  public int getK()
  {
    return k;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return DoublesSketch.getUpdatableStorageBytes(k, MAX_STREAM_LENGTH);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.<AggregatorFactory> singletonList(
        new DoublesSketchAggregatorFactory(
            fieldName,
            fieldName,
            k));
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoublesSketchMergeAggregatorFactory(name, k);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof DoublesSketchAggregatorFactory) {
      return new DoublesSketchMergeAggregatorFactory(name, k);
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public Object finalizeComputation(final Object object)
  {
    return ((DoublesSketch) object).getN();
  }

  @Override
  public String getTypeName()
  {
    return DoublesSketchModule.DOUBLES_SKETCH;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(cacheTypeId).appendString(name).appendString(fieldName).appendInt(k).build();
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
    final DoublesSketchAggregatorFactory that = (DoublesSketchAggregatorFactory) o;
    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    if (k != that.k) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + Integer.hashCode(k);
    result = 31 * result + Byte.hashCode(cacheTypeId);
    return result;
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{"
        + "name=" + name
        + ", fieldName=" + fieldName
        + ", k=" + k
        + "}";
  }

}
