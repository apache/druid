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

public class MomentSketchWrapper
{
  public MomentStruct data;
  // Whether we use arcsinh to compress the range
  public boolean useArcSinh = true;

  public MomentSketchWrapper(
      int k
  )
  {
    data = new MomentStruct(k);
  }

  public MomentSketchWrapper(
      MomentStruct data
  )
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
    return data.min;
  }

  public double getMax()
  {
    return data.max;
  }

  public void add(double rawX)
  {
    double x = rawX;
    if (useArcSinh) {
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

  public double[] getQuantiles(double[] fractions)
  {
    MomentSolver ms = new MomentSolver(data);
    ms.setGridSize(1024);
    ms.setMaxIter(15);
    ms.solve();

    double[] quantiles = new double[fractions.length];
    for (int i = 0; i < fractions.length; i++) {
      double rawQuantile = ms.getQuantile(fractions[i]);
      if (useArcSinh) {
        quantiles[i] = Math.sinh(rawQuantile);
      } else {
        quantiles[i] = rawQuantile;
      }
    }

    return quantiles;
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

  public String toString()
  {
    return data.toString();
  }
}
