/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Floats;
import com.nativelibs4java.opencl.CLContext;
import com.nativelibs4java.opencl.CLDevice;
import com.nativelibs4java.opencl.CLPlatform;
import com.nativelibs4java.opencl.CLQueue;
import com.nativelibs4java.opencl.JavaCL;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.gpu.CLUtils;
import io.druid.segment.BufferSelectorFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.FloatBufferSelector;

import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class KernelAggregatorFactory implements AggregatorFactory
{
  private final String fieldName;
  private final String name;

  private final CLContext context;
  private final CLQueue queue;
  private final ByteOrder byteOrder;

  @JsonCreator
  public KernelAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;

    CLDevice device = CLUtils.findDevice("i7");
    if (device == null) {
      throw new IllegalStateException("No OpenCL-capable device!");
    }

    this.context = JavaCL.createContext(new HashMap<CLPlatform.ContextProperties, Object>(), device);
    this.queue = this.context.createDefaultQueue();
    this.byteOrder = this.context.getByteOrder();
  }

  public KernelAggregator factorizeKernel(BufferSelectorFactory columnFactory)
  {
    FloatBufferSelector floatBufferSelector = columnFactory.makeFloatBufferSelector(fieldName, byteOrder);
    return new FloatKernelAggregator(floatBufferSelector, context, queue);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object deserialize(Object object)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> requiredFields()
  {
    return null;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }

  @Override
  public String getTypeName()
  {
    return null;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Floats.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return null;
  }

  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }
}
