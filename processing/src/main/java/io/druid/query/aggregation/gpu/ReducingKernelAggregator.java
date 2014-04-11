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

import com.nativelibs4java.opencl.CLContext;
import com.nativelibs4java.opencl.CLQueue;
import com.nativelibs4java.opencl.util.OpenCLType;
import com.nativelibs4java.opencl.util.ReductionUtils;
import io.druid.query.aggregation.gpu.AbstractFloatKernelAggregator;
import io.druid.segment.FloatBufferSelector;
import org.bridj.Pointer;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class ReducingKernelAggregator extends AbstractFloatKernelAggregator
{
  private final ReductionUtils.Reductor<Float> reductor;

  public ReducingKernelAggregator(
      FloatBufferSelector selector,
      CLContext context,
      CLQueue queue
  )
  {
    super(queue, selector, context);
    this.reductor = ReductionUtils.createReductor(context, ReductionUtils.Operation.Add, OpenCLType.Float, 1);
  }

  @Override
  public void run(IntBuffer buckets, ByteBuffer out, int position)
  {
    Pointer<Float> ptr = reductor.reduce(queue, this.totalBuffer);
    Float value = ptr.get();

    // TODO: count in buckets
    out.asFloatBuffer().put(position, value);
  }
}
