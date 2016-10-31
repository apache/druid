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

package io.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class SketchAggregatorFactory extends AggregatorFactory
{
  public static final int DEFAULT_MAX_SKETCH_SIZE = 16384;

  protected final String name;
  protected final String fieldName;
  protected final int size;
  private final byte cacheId;

  public static final Comparator<Object> COMPARATOR = Ordering.from(
      new Comparator()
      {
        @Override
        public int compare(Object o1, Object o2)
        {
          Sketch s1 = SketchAggregatorFactory.toSketch(o1);
          Sketch s2 = SketchAggregatorFactory.toSketch(o2);
          return Doubles.compare(s1.getEstimate(), s2.getEstimate());
        }
      }
  ).nullsFirst();

  public SketchAggregatorFactory(String name, String fieldName, Integer size, byte cacheId)
  {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.size = size == null ? DEFAULT_MAX_SKETCH_SIZE : size;
    Util.checkIfPowerOf2(this.size, "size");

    this.cacheId = cacheId;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      return new EmptySketchAggregator();
    } else {
      return new SketchAggregator(selector, size);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      return new EmptySketchBufferAggregator();
    } else {
      return new SketchBufferAggregator(selector, size, getMaxIntermediateSize());
    }
  }

  @Override
  public Object deserialize(Object object)
  {
    return SketchOperations.deserialize(object);
  }

  @Override
  public Comparator<Object> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    final Union union;
    if (lhs instanceof Union) {
      union = (Union) lhs;
      updateUnion(union, rhs);
    } else if (rhs instanceof Union) {
      union = (Union) rhs;
      updateUnion(union, lhs);
    } else {
      union = (Union) SetOperation.builder().build(size, Family.UNION);
      updateUnion(union, lhs);
      updateUnion(union, rhs);
    }


    return union;
  }

  private void updateUnion(Union union, Object obj)
  {
    if (obj == null) {
      return;
    } else if (obj instanceof Memory) {
      union.update((Memory) obj);
    } else if (obj instanceof Sketch) {
      union.update((Sketch) obj);
    } else if (obj instanceof Union) {
      union.update(((Union) obj).getResult(false, null));
    } else {
      throw new IAE("Object of type [%s] can not be unioned", obj.getClass().getName());
    }
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

  @Override
  public int getMaxIntermediateSize()
  {
    return SetOperation.getMaxUnionBytes(size);
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = fieldName.getBytes();
    return ByteBuffer.allocate(1 + Ints.BYTES + fieldNameBytes.length)
                     .put(cacheId)
                     .putInt(size)
                     .put(fieldNameBytes)
                     .array();
  }

  public final static Sketch toSketch(Object obj)
  {
    if (obj instanceof Sketch) {
      return (Sketch) obj;
    } else if (obj instanceof Union) {
      return ((Union) obj).getResult(true, null);
    } else {
      throw new IAE("Can't convert to Sketch object [%s]", obj.getClass());
    }
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{"
           + "fieldName='" + fieldName + '\''
           + ", name='" + name + '\''
           + ", size=" + size
           + '}';
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

    SketchAggregatorFactory that = (SketchAggregatorFactory) o;

    if (size != that.size) {
      return false;
    }
    if (cacheId != that.cacheId) {
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
    result = 31 * result + (int) cacheId;
    return result;
  }
}
