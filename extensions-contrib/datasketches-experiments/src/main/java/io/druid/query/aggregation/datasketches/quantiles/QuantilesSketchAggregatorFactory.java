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
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesUnion;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class QuantilesSketchAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 16;

  public static final int DEFAULT_MAX_SKETCH_SIZE = 1024;

  protected final String name;
  protected final String fieldName;
  protected final int size;
  private final boolean isInputSketch;
  private final int maxIntermediateSize;

  public static final Comparator<DoublesSketch> COMPARATOR = new Comparator<DoublesSketch>()
  {
    @Override
    public int compare(DoublesSketch o, DoublesSketch o1)
    {
      return Doubles.compare(o.getN(), o1.getN());
    }
  };

  @JsonCreator
  public QuantilesSketchAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("size") Integer size,
      @JsonProperty("isInputSketch") boolean isInputSketch,
      @JsonProperty("maxIntermediateSize") Integer maxIntermediateSize
  )
  {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.size = size == null ? DEFAULT_MAX_SKETCH_SIZE : size;

    //validate the size with sketch library
    Util.checkIfPowerOf2(this.size, "size");
    //dummy sketch creation just to ensure all size restrictions are fulfilled.
    QuantilesSketchUtils.buildSketch(this.size);

    this.maxIntermediateSize = (maxIntermediateSize == null) ? 1 : maxIntermediateSize;
    Preconditions.checkArgument(this.maxIntermediateSize > 0, "maxIntermediateSize must be > 0.");

    this.isInputSketch = isInputSketch;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      return new EmptyQuantilesSketchAggregator(name);
    } else {
      return new QuantilesSketchAggregator(name, selector, size);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      return new EmptyQuantilesSketchBufferAggregator();
    } else {
      return new QuantilesSketchBufferAggregator(selector, size, getMaxIntermediateSize());
    }
  }

  @Override
  public Object deserialize(Object object)
  {
    return QuantilesSketchUtils.deserialize(object);
  }

  @Override
  public Comparator<DoublesSketch> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (lhs == null || ((DoublesSketch) lhs).isEmpty()) {
      return rhs;
    }

    if (rhs == null || ((DoublesSketch) rhs).isEmpty()) {
      return lhs;
    }

    DoublesUnion union = QuantilesSketchUtils.buildUnion(size);
    union.update((DoublesSketch) lhs);
    union.update((DoublesSketch) rhs);

    return union.getResult();
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
  public int getSize()
  {
    return size;
  }

  @JsonProperty
  public boolean getIsInputSketch()
  {
    return isInputSketch;
  }

  @Override
  @JsonProperty
  public int getMaxIntermediateSize()
  {
    return maxIntermediateSize;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return QuantilesSketchUtils.buildSketch(size);
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.<AggregatorFactory>singletonList(
        new QuantilesSketchAggregatorFactory(
            fieldName,
            fieldName,
            size,
            isInputSketch,
            maxIntermediateSize
        )
    );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new QuantilesSketchAggregatorFactory(name, name, size, false, maxIntermediateSize);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof QuantilesSketchAggregatorFactory) {
      QuantilesSketchAggregatorFactory castedOther = (QuantilesSketchAggregatorFactory) other;

      return new QuantilesSketchAggregatorFactory(
          name,
          name,
          Math.max(size, castedOther.size),
          false,
          maxIntermediateSize
      );
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  /**
   * Finalize the computation on sketch object and returns numEntries in it.
   * sketch.
   *
   * @param object the sketch object
   *
   * @return sketch object
   */
  @Override
  public Object finalizeComputation(Object object)
  {
    return ((DoublesSketch) object).getN();
  }

  @Override
  public String getTypeName()
  {
    if (isInputSketch) {
      return QuantilesSketchModule.QUANTILES_SKETCH_MERGE_AGG;
    } else {
      return QuantilesSketchModule.QUANTILES_SKETCH_BUILD_AGG;
    }
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] nameBytes = StringUtils.toUtf8(name);
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);

    return ByteBuffer.allocate(1 + nameBytes.length + 1 + fieldNameBytes.length + 1 + Ints.BYTES + 2)
                     .put(CACHE_TYPE_ID)
                     .put(nameBytes)
                     .put(AggregatorUtil.STRING_SEPARATOR)
                     .put(fieldNameBytes)
                     .put(AggregatorUtil.STRING_SEPARATOR)
                     .putInt(size)
                     .put(isInputSketch ? (byte) 1 : (byte) 0)
                     .array();
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

    QuantilesSketchAggregatorFactory that = (QuantilesSketchAggregatorFactory) o;

    if (size != that.size) {
      return false;
    }
    if (isInputSketch != that.isInputSketch) {
      return false;
    }
    if (maxIntermediateSize != that.maxIntermediateSize) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    return fieldName.equals(that.fieldName);

  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + size;
    result = 31 * result + (isInputSketch ? 1 : 0);
    result = 31 * result + maxIntermediateSize;
    return result;
  }

  @Override
  public String toString()
  {
    return "QuantilesSketchAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", size=" + size +
           ", isInputSketch=" + isInputSketch +
           ", maxIntermediateSize=" + maxIntermediateSize +
           '}';
  }
}
