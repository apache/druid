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

public abstract class BaseLongVectorValueSelector implements VectorValueSelector
{
  protected final ReadableVectorOffset offset;

  private int floatId = ReadableVectorInspector.NULL_ID;
  private int doubleId = ReadableVectorInspector.NULL_ID;

  private float[] floatVector;
  private double[] doubleVector;

  public BaseLongVectorValueSelector(final ReadableVectorOffset offset)
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
  public float[] getFloatVector()
  {
    if (floatId == offset.getId()) {
      return floatVector;
    }

    if (floatVector == null) {
      floatVector = new float[offset.getMaxVectorSize()];
    }

    final long[] longVector = getLongVector();
    for (int i = 0; i < getCurrentVectorSize(); i++) {
      floatVector[i] = (float) longVector[i];
    }

    floatId = offset.getId();
    return floatVector;
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

    final long[] longVector = getLongVector();
    for (int i = 0; i < getCurrentVectorSize(); i++) {
      doubleVector[i] = (double) longVector[i];
    }

    doubleId = offset.getId();
    return doubleVector;
  }
}
