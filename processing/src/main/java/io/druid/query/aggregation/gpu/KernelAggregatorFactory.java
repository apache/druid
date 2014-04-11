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

package io.druid.query.aggregation.gpu;

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
import io.druid.segment.BufferSelectorFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.FloatBufferSelector;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class KernelAggregatorFactory implements AggregatorFactory
{
  private final String fieldName;
  private final String name;
  private final String kernelName;
  private final String src;

  private final CLContext context;
  private final CLQueue queue;
  private final ByteOrder byteOrder;

  @JsonCreator
  public KernelAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("kernelName") final String kernelName,
      @JsonProperty("src") final String src
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.kernelName = kernelName;
    this.src = src;

    CLDevice device = CLUtils.getDevice();

    this.context = JavaCL.createContext(new HashMap<CLPlatform.ContextProperties, Object>(), device);
    this.queue = this.context.createDefaultQueue();
    this.byteOrder = this.context.getByteOrder();
  }

  public KernelAggregator factorizeKernel(BufferSelectorFactory columnFactory)
  {
    final FloatBufferSelector floatBufferSelector = columnFactory.makeFloatBufferSelector(fieldName, byteOrder);
    if(kernelName == null || kernelName.isEmpty()) {
      return new ReducingKernelAggregator(floatBufferSelector, context, queue);
    }
    else {
      return new FloatKernelAggregator(floatBufferSelector, context, queue, src, kernelName);
    }
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
    return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }

  @Override
  public String getTypeName()
  {
    return "float";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // in this case it indicates the size of each ouput buffer element
    return Floats.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return 0f;
  }

  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }
}
