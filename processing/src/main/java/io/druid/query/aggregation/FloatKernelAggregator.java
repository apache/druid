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

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLContext;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import com.nativelibs4java.opencl.CLQueue;
import com.nativelibs4java.opencl.util.OpenCLType;
import com.nativelibs4java.opencl.util.ReductionUtils;
import io.druid.segment.FloatBufferSelector;
import org.bridj.Pointer;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

public class FloatKernelAggregator implements KernelAggregator
{
  private final FloatBufferSelector selector;

  private final CLContext context;
  private final CLQueue queue;

  private final CLBuffer<Float> totalBuffer;
  private int totalBufferOffset = 0;

  private final ReductionUtils.Reductor<Float> reductor;

  private List<CLEvent> copyEvents;

  public FloatKernelAggregator(
      FloatBufferSelector selector,
      CLContext context,
      CLQueue queue
  )
  {
    this.selector = selector;
    this.context = context;
    this.queue = queue;

    this.totalBuffer = this.context.createFloatBuffer(CLMem.Usage.Input, selector.size());
    this.reductor = ReductionUtils.createReductor(context, ReductionUtils.Operation.Add, OpenCLType.Float, 1);

    copyEvents = new ArrayList<CLEvent>();
  }


  @Override
  public void copyBuffer()
  {
    FloatBuffer currentBuffer = selector.getBuffer();
    CLBuffer<Float> buf = context.createFloatBuffer(CLMem.Usage.Input, Pointer.pointerToFloats(currentBuffer));

    int bufRemaining = currentBuffer.remaining();
    CLEvent copyEvent = buf.copyTo(queue, 0, bufRemaining, totalBuffer, totalBufferOffset);
    totalBufferOffset += bufRemaining;

    copyEvents.add(copyEvent);
  }

  @Override
  public void run(IntBuffer buckets, ByteBuffer out, int position)
  {
    Pointer<Float> ptr = reductor.reduce(queue, this.totalBuffer, copyEvents.toArray(new CLEvent[copyEvents.size()]));
    Float value = ptr.get();

    // TODO: count in buckets
    out.asFloatBuffer().put(position, value);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return null;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return 0;
  }

  @Override
  public void close()
  {
    totalBuffer.release();
  }
}
