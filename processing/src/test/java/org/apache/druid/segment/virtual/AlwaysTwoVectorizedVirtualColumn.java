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

package org.apache.druid.segment.virtual;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorSizeInspector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public class AlwaysTwoVectorizedVirtualColumn implements VirtualColumn
{
  private final String outputName;
  private final ColumnCapabilities capabilities;
  private final boolean dictionaryEncoded;

  public AlwaysTwoVectorizedVirtualColumn(
      String name,
      ColumnCapabilities capabilites
  )
  {
    this.outputName = name;
    this.capabilities = capabilites;
    this.dictionaryEncoded = capabilites.isDictionaryEncoded().isTrue() && capabilites.areDictionaryValuesUnique().isTrue();
  }

  @Override
  public boolean canVectorize(ColumnInspector inspector)
  {
    return true;
  }

  @Override
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec, ColumnSelectorFactory factory
  )
  {
    throw new IllegalStateException("don't call this");
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName, ColumnSelectorFactory factory
  )
  {
    throw new IllegalStateException("don't call this");
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory
  )
  {

    return new SingleValueDimensionVectorSelector()
    {
      private final VectorSizeInspector inspector = factory.getVectorSizeInspector();
      private final int[] rowVector = new int[inspector.getMaxVectorSize()];

      @Override
      public int[] getRowVector()
      {

        return rowVector;
      }

      @Override
      public int getValueCardinality()
      {
        return dictionaryEncoded ? 1 : CARDINALITY_UNKNOWN;
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        return "2";
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return dictionaryEncoded;
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

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueVectorDimensionSelector(
      DimensionSpec dimensionSpec, VectorColumnSelectorFactory factory
  )
  {
    final IndexedInts[] rowVector = new IndexedInts[factory.getVectorSizeInspector().getMaxVectorSize()];
    Arrays.fill(rowVector, new IndexedInts()
    {
      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing
      }

      @Override
      public int size()
      {
        return 2;
      }

      @Override
      public int get(int index)
      {
        return 0;
      }
    });
    return new MultiValueDimensionVectorSelector()
    {
      private final VectorSizeInspector inspector = factory.getVectorSizeInspector();

      @Override
      public IndexedInts[] getRowVector()
      {
        return rowVector;
      }

      @Override
      public int getValueCardinality()
      {
        return dictionaryEncoded ? 1 : CARDINALITY_UNKNOWN;
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        return "2";
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return dictionaryEncoded;
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

  @Override
  public VectorValueSelector makeVectorValueSelector(
      String columnName, VectorColumnSelectorFactory factory
  )
  {
    final long[] longs = new long[factory.getVectorSizeInspector().getMaxVectorSize()];
    final double[] doubles = new double[factory.getVectorSizeInspector().getMaxVectorSize()];
    final float[] floats = new float[factory.getVectorSizeInspector().getMaxVectorSize()];
    Arrays.fill(longs, 2L);
    Arrays.fill(doubles, 2.0);
    Arrays.fill(floats, 2.0f);
    return new VectorValueSelector()
    {
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
        return null;
      }

      @Override
      public int getMaxVectorSize()
      {
        return factory.getVectorSizeInspector().getMaxVectorSize();
      }

      @Override
      public int getCurrentVectorSize()
      {
        return factory.getVectorSizeInspector().getCurrentVectorSize();
      }
    };
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(
      String columnName, VectorColumnSelectorFactory factory
  )
  {
    final Object[] objects = new Object[factory.getVectorSizeInspector().getMaxVectorSize()];
    Arrays.fill(objects, "2");
    return new VectorObjectSelector()
    {
      @Override
      public int getMaxVectorSize()
      {
        return factory.getVectorSizeInspector().getMaxVectorSize();
      }

      @Override
      public int getCurrentVectorSize()
      {
        return factory.getVectorSizeInspector().getCurrentVectorSize();
      }

      @Override
      public Object[] getObjectVector()
      {
        return objects;
      }
    };
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return capabilities;
  }

  @Override
  public List<String> requiredColumns()
  {
    return ImmutableList.of();
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }
}
