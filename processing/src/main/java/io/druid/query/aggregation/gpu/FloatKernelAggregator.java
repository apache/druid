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
import com.nativelibs4java.opencl.CLKernel;
import com.nativelibs4java.opencl.CLMem;
import com.nativelibs4java.opencl.CLProgram;
import com.nativelibs4java.opencl.CLQueue;
import io.druid.segment.FloatBufferSelector;
import org.bridj.Pointer;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class FloatKernelAggregator extends AbstractFloatKernelAggregator
{
  private final CLProgram program;
  private final CLKernel kernel;

  public FloatKernelAggregator(
      FloatBufferSelector selector,
      CLContext context,
      CLQueue queue,
      String src,
      String name
  )
  {
    super(queue, selector, context);
    this.program = context.createProgram(src);
    this.kernel = program.createKernel(name);
  }

  @Override
  public void run(IntBuffer buckets, ByteBuffer out, int position)
  {
    final int n = buckets.remaining() / 2;
    CLBuffer<Float> kernelOut = context.createFloatBuffer(CLMem.Usage.Output, n);
    kernel.setArgs(buckets, totalBuffer, kernelOut, n);

    final int[] globalSizes = new int[] { n };
    CLEvent addEvt = kernel.enqueueNDRange(queue, globalSizes);

    kernelOut.read(queue, Pointer.pointerToFloats(out.asFloatBuffer()), true, addEvt)
             .waitFor();
  }

  @Override
  public void close()
  {
    super.close();
    kernel.release();
    program.release();
  }
}
