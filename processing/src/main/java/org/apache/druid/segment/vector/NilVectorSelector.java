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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.QueryableIndexStorageAdapter;

import javax.annotation.Nullable;

public class NilVectorSelector
    implements VectorValueSelector, VectorObjectSelector, SingleValueDimensionVectorSelector, IdLookup
{
  private static final boolean[] DEFAULT_NULLS_VECTOR = new boolean[QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE];
  private static final int[] DEFAULT_INT_VECTOR = new int[QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE];
  private static final long[] DEFAULT_LONG_VECTOR = new long[QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE];
  private static final float[] DEFAULT_FLOAT_VECTOR = new float[QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE];
  private static final double[] DEFAULT_DOUBLE_VECTOR = new double[QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE];
  private static final Object[] DEFAULT_OBJECT_VECTOR = new Object[QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE];

  static {
    for (int i = 0; i < DEFAULT_NULLS_VECTOR.length; i++) {
      DEFAULT_NULLS_VECTOR[i] = true;
    }
  }

  private final VectorSizeInspector vectorSizeInspector;
  private final boolean[] nulls;
  private final int[] ints;
  private final long[] longs;
  private final float[] floats;
  private final double[] doubles;
  private final Object[] objects;

  private NilVectorSelector(
      final VectorSizeInspector vectorSizeInspector,
      final boolean[] nulls,
      final int[] ints,
      final long[] longs,
      final float[] floats,
      final double[] doubles,
      final Object[] objects
  )
  {
    this.vectorSizeInspector = vectorSizeInspector;
    this.nulls = nulls;
    this.ints = ints;
    this.longs = longs;
    this.floats = floats;
    this.doubles = doubles;
    this.objects = objects;
  }

  public static NilVectorSelector create(final VectorSizeInspector vectorSizeInspector)
  {
    if (vectorSizeInspector.getMaxVectorSize() <= QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE) {
      // Reuse static vars when possible.
      return new NilVectorSelector(
          vectorSizeInspector,
          DEFAULT_NULLS_VECTOR,
          DEFAULT_INT_VECTOR,
          DEFAULT_LONG_VECTOR,
          DEFAULT_FLOAT_VECTOR,
          DEFAULT_DOUBLE_VECTOR,
          DEFAULT_OBJECT_VECTOR
      );
    } else {
      return new NilVectorSelector(
          vectorSizeInspector,
          new boolean[vectorSizeInspector.getMaxVectorSize()],
          new int[vectorSizeInspector.getMaxVectorSize()],
          new long[vectorSizeInspector.getMaxVectorSize()],
          new float[vectorSizeInspector.getMaxVectorSize()],
          new double[vectorSizeInspector.getMaxVectorSize()],
          new Object[vectorSizeInspector.getMaxVectorSize()]
      );
    }
  }

  @Override
  public long[] getLongVector()
  {
    return longs;
  }

  @Override
  public float[] getFloatVector()
  {
    return floats;
  }

  @Override
  public double[] getDoubleVector()
  {
    return doubles;
  }

  @Nullable
  @Override
  public boolean[] getNullVector()
  {
    return nulls;
  }

  @Override
  public int[] getRowVector()
  {
    return ints;
  }

  @Override
  public int getValueCardinality()
  {
    return 1;
  }

  @Nullable
  @Override
  public String lookupName(final int id)
  {
    assert id == 0 : "id = " + id;
    return null;
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return false;
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return this;
  }

  @Override
  public int lookupId(@Nullable final String name)
  {
    return NullHandling.isNullOrEquivalent(name) ? 0 : -1;
  }

  @Override
  public Object[] getObjectVector()
  {
    return objects;
  }

  @Override
  public int getCurrentVectorSize()
  {
    return vectorSizeInspector.getCurrentVectorSize();
  }

  @Override
  public int getMaxVectorSize()
  {
    return vectorSizeInspector.getMaxVectorSize();
  }
}
