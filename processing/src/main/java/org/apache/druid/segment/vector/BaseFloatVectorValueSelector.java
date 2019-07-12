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

package org.apache.druid.segment.vector;

public abstract class BaseFloatVectorValueSelector implements VectorValueSelector
{
  protected final ReadableVectorOffset offset;

  private int longId = ReadableVectorOffset.NULL_ID;
  private int doubleId = ReadableVectorOffset.NULL_ID;

  private long[] longVector;
  private double[] doubleVector;

  public BaseFloatVectorValueSelector(final ReadableVectorOffset offset)
  {
    this.offset = offset;
  }

  @Override
  public int getCurrentVectorSize()
  {
    return offset.getCurrentVectorSize();
  }

  @Override
  public int getMaxVectorSize()
  {
    return offset.getMaxVectorSize();
  }

  @Override
  public long[] getLongVector()
  {
    if (longId == offset.getId()) {
      return longVector;
    }

    if (longVector == null) {
      longVector = new long[offset.getMaxVectorSize()];
    }

    final float[] floatVector = getFloatVector();
    for (int i = 0; i < getCurrentVectorSize(); i++) {
      longVector[i] = (long) floatVector[i];
    }

    longId = offset.getId();
    return longVector;
  }

  @Override
  public double[] getDoubleVector()
  {
    if (doubleId == offset.getId()) {
      return doubleVector;
    }

    if (doubleVector == null) {
      doubleVector = new double[offset.getMaxVectorSize()];
    }

    final float[] floatVector = getFloatVector();
    for (int i = 0; i < getCurrentVectorSize(); i++) {
      doubleVector[i] = (double) floatVector[i];
    }

    doubleId = offset.getId();
    return doubleVector;
  }
}
