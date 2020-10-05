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

import org.apache.druid.segment.IdLookup;

import javax.annotation.Nullable;
import java.util.Arrays;

public class ConstantVectorSelectors
{
  public static VectorValueSelector vectorValueSelector(VectorSizeInspector inspector, @Nullable Number constant)
  {
    if (constant == null) {
      return NilVectorSelector.create(inspector);
    }
    final long[] longVector = new long[inspector.getMaxVectorSize()];
    final float[] floatVector = new float[inspector.getMaxVectorSize()];
    final double[] doubleVector = new double[inspector.getMaxVectorSize()];
    Arrays.fill(longVector, constant.longValue());
    Arrays.fill(floatVector, constant.floatValue());
    Arrays.fill(doubleVector, constant.doubleValue());
    return new VectorValueSelector()
    {
      @Override
      public long[] getLongVector()
      {
        return longVector;
      }

      @Override
      public float[] getFloatVector()
      {
        return floatVector;
      }

      @Override
      public double[] getDoubleVector()
      {
        return doubleVector;
      }

      @Nullable
      @Override
      public boolean[] getNullVector()
      {
        return null;
      }

      @Override
      public int getMaxVectorSize()
      {
        return inspector.getMaxVectorSize();
      }

      @Override
      public int getCurrentVectorSize()
      {
        return inspector.getCurrentVectorSize();
      }
    };
  }

  public static VectorObjectSelector vectorObjectSelector(
      VectorSizeInspector inspector,
      @Nullable Object object
  )
  {
    if (object == null) {
      return NilVectorSelector.create(inspector);
    }

    final Object[] objects = new Object[inspector.getMaxVectorSize()];
    Arrays.fill(objects, object);

    return new VectorObjectSelector()
    {
      @Override
      public Object[] getObjectVector()
      {
        return objects;
      }

      @Override
      public int getMaxVectorSize()
      {
        return inspector.getMaxVectorSize();
      }

      @Override
      public int getCurrentVectorSize()
      {
        return inspector.getCurrentVectorSize();
      }
    };
  }

  public static SingleValueDimensionVectorSelector singleValueDimensionVectorSelector(
      VectorSizeInspector inspector,
      @Nullable String value
  )
  {
    if (value == null) {
      return NilVectorSelector.create(inspector);
    }

    final int[] row = new int[inspector.getMaxVectorSize()];
    return new SingleValueDimensionVectorSelector()
    {
      @Override
      public int[] getRowVector()
      {
        return row;
      }

      @Override
      public int getValueCardinality()
      {
        return 1;
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        return value;
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return true;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return null;
      }

      @Override
      public int getMaxVectorSize()
      {
        return inspector.getMaxVectorSize();
      }

      @Override
      public int getCurrentVectorSize()
      {
        return inspector.getCurrentVectorSize();
      }
    };
  }
}
