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

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLContext;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import com.nativelibs4java.opencl.CLQueue;
import io.druid.segment.FloatBufferSelector;
import org.bridj.Pointer;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractFloatKernelAggregator implements KernelAggregator
{
  protected final FloatBufferSelector selector;
  protected final CLContext context;
  protected final CLQueue queue;
  protected final CLBuffer<Float> totalBuffer;
  protected List<CLEvent> copyEvents;
  private int totalBufferOffset = 0;
  protected long t = 0;

  public AbstractFloatKernelAggregator(
      CLQueue queue,
      FloatBufferSelector selector,
      CLContext context
  )
  {
    this.copyEvents = new ArrayList<>();
    this.context = context;
    this.totalBuffer = this.context.createFloatBuffer(CLMem.Usage.Input, selector.size());
    this.queue = queue;
    this.selector = selector;
  }

  public abstract void run(IntBuffer buckets, ByteBuffer out, int position);

  @Override
  public void copyBuffer()
  {
    FloatBuffer currentBuffer = selector.getBuffer();
    CLBuffer<Float> buf = context.createFloatBuffer(CLMem.Usage.Input, Pointer.pointerToFloats(currentBuffer));

    int bufRemaining = currentBuffer.remaining();

    long t0 = System.nanoTime();
    CLEvent copyEvent = buf.copyTo(queue, 0, bufRemaining, totalBuffer, totalBufferOffset);
    copyEvent.waitFor();
    t += System.nanoTime() - t0;
    copyEvents.add(copyEvent);
    totalBufferOffset += bufRemaining;
  }

  @Override
  public void close()
  {
    totalBuffer.release();
  }
}
