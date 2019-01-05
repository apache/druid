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

package org.apache.druid.query.aggregation.momentsketch;

import com.github.stanfordfuturedata.momentsketch.MomentSolver;
import com.github.stanfordfuturedata.momentsketch.MomentStruct;

import java.nio.ByteBuffer;

/**
 * Class for wrapping the operations of the moments sketch for use in
 * the moment sketch aggregator
 * {@link org.apache.druid.query.aggregation.momentsketch.aggregator.MomentSketchAggregatorFactory}.
 *
 * k controls the size and accuracy provided by the sketch.
 * The sinh function is used to compress the range of data to allow for more robust results
 * on skewed and long-tailed metrics, but slightly reducing accuracy on metrics with more uniform
 * distributions.
 */
public class MomentSketchWrapper
{
  // The MomentStruct object stores the relevant statistics about a metric distribution.
  protected MomentStruct data;
  // Whether we use arcsinh to compress the range
  protected boolean useArcSinh = true;

  public MomentSketchWrapper(int k)
  {
    data = new MomentStruct(k);
  }

  public MomentSketchWrapper(MomentStruct data)
  {
    this.data = data;
  }

  public void setCompressed(boolean flag)
  {
    useArcSinh = flag;
  }

  public boolean getCompressed()
  {
    return useArcSinh;
  }

  public int getK()
  {
    return data.power_sums.length;
  }

  public double[] getPowerSums()
  {
    return data.power_sums;
  }

  public double getMin()
  {
    if (useArcSinh) {
      return Math.sinh(data.min);
    } else {
      return data.min;
    }
  }

  public double getMax()
  {
    if (useArcSinh) {
      return Math.sinh(data.max);
    } else {
      return data.max;
    }
  }

  public void add(double rawX)
  {
    double x = rawX;
    if (useArcSinh) {
      // Since Java does not have a native arcsinh implementation we
      // compute it manually using the following formula.
      // This is the inverse operation of Math.sinh
      x = Math.log(rawX + Math.sqrt(1 + rawX * rawX));
    }
    data.add(x);
  }

  public void merge(MomentSketchWrapper other)
  {
    data.merge(other.data);
  }

  public byte[] toByteArray()
  {
    ByteBuffer bb = ByteBuffer.allocate(2 * Integer.BYTES + (data.power_sums.length + 2) * Double.BYTES);
    return toBytes(bb).array();
  }

  public MomentSolver getSolver()
  {
    MomentSolver ms = new MomentSolver(data);
    return ms;
  }

  /**
   * Estimates quantiles given the statistics in a moments sketch.
   * @param fractions real values between [0,1] for which we want to estimate quantiles
   *
   * @return estimated quantiles.
   */
  public double[] getQuantiles(double[] fractions)
  {
    // The solver attempts to construct a distribution estimate which matches the
    // statistics tracked by the moments sketch. We can then read off quantile estimates
    // from the reconstructed distribution.
    // This operation can be relatively expensive (~1 ms) so we set the parameters from distribution
    // reconstruction to conservative values.
    MomentSolver ms = new MomentSolver(data);
    // Constants here are chosen to yield maximum precision while keeping solve times ~1ms on 2Ghz cpu
    // Grid size can be increased if longer solve times are acceptable
    ms.setGridSize(1024);
    ms.setMaxIter(15);
    ms.solve();
    double[] rawQuantiles = ms.getQuantiles(fractions);
    for (int i = 0; i < fractions.length; i++) {
      if (useArcSinh) {
        rawQuantiles[i] = Math.sinh(rawQuantiles[i]);
      }
    }
    return rawQuantiles;
  }

  public ByteBuffer toBytes(ByteBuffer bb)
  {
    int compressedInt = getCompressed() ? 1 : 0;
    bb.putInt(data.power_sums.length);
    bb.putInt(compressedInt);
    bb.putDouble(data.min);
    bb.putDouble(data.max);
    for (double x : data.power_sums) {
      bb.putDouble(x);
    }
    return bb;
  }

  public static MomentSketchWrapper fromBytes(ByteBuffer bb)
  {
    int k = bb.getInt();
    int compressedInt = bb.getInt();
    boolean compressed = (compressedInt > 0);
    MomentStruct m = new MomentStruct(k);
    m.min = bb.getDouble();
    m.max = bb.getDouble();
    for (int i = 0; i < k; i++) {
      m.power_sums[i] = bb.getDouble();
    }
    MomentSketchWrapper mw = new MomentSketchWrapper(m);
    mw.setCompressed(compressed);
    return mw;
  }

  public static MomentSketchWrapper fromByteArray(byte[] input)
  {
    ByteBuffer bb = ByteBuffer.wrap(input);
    return fromBytes(bb);
  }

  @Override
  public String toString()
  {
    return data.toString();
  }
}
