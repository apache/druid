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

package org.apache.druid.segment;

import org.apache.druid.error.DruidException;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.join.filter.AllNullColumnSelectorFactory;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A {@link CursorHolder} that yields no rows. Useful wherever a segment can be proven to match no rows without
 * needing to scan it.
 */
public final class EmptyCursorHolder implements CursorHolder
{
  private static final ColumnSelectorFactory NIL_SELECTOR_FACTORY = new AllNullColumnSelectorFactory();

  /**
   * An empty holder that advertises the spec's preferred ordering.
   */
  public static EmptyCursorHolder forSpec(CursorBuildSpec spec, ColumnInspector inspector)
  {
    return new EmptyCursorHolder(spec, inspector);
  }

  private final CursorBuildSpec spec;
  private final ColumnInspector inspector;

  private EmptyCursorHolder(CursorBuildSpec spec, ColumnInspector inspector)
  {
    this.spec = spec;
    this.inspector = inspector;
  }

  @Override
  public Cursor asCursor()
  {
    return new Cursor()
    {
      @Override
      public ColumnSelectorFactory getColumnSelectorFactory()
      {
        return NIL_SELECTOR_FACTORY;
      }

      @Override
      public void advance()
      {
      }

      @Override
      public void advanceUninterruptibly()
      {
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public boolean isDoneOrInterrupted()
      {
        return true;
      }

      @Override
      public void reset()
      {
      }
    };
  }

  @Override
  public boolean canVectorize()
  {
    return QueryableIndexCursorHolder.canVectorizeVirtualColumnsAndAggregators(
        spec.getVirtualColumns(),
        spec.getAggregators(),
        spec.getQueryContext(),
        inspector
    );
  }

  @Override
  public VectorCursor asVectorCursor()
  {
    final ReadableVectorInspector vectorInspector = new ReadableVectorInspector()
    {
      @Override
      public int getId()
      {
        return 0;
      }

      @Override
      public int getMaxVectorSize()
      {
        return spec.getQueryContext().getVectorSize();
      }

      @Override
      public int getCurrentVectorSize()
      {
        return 0;
      }
    };
    final VectorColumnSelectorFactory nilFactory = new VectorColumnSelectorFactory()
    {
      @Override
      public ReadableVectorInspector getReadableVectorInspector()
      {
        return vectorInspector;
      }

      @Override
      public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
      {
        return NilVectorSelector.create(vectorInspector);
      }

      @Override
      public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
      {
        // Only valid on columns whose capabilities report multi-value strings; this factory reports null
        // capabilities for every column, so engines should never route here.
        throw DruidException.defensive(
            "Cannot make multi-value dimension selector for column[%s] on an empty cursor",
            dimensionSpec.getDimension()
        );
      }

      @Override
      public VectorValueSelector makeValueSelector(String column)
      {
        return NilVectorSelector.create(vectorInspector);
      }

      @Override
      public VectorObjectSelector makeObjectSelector(String column)
      {
        return NilVectorSelector.create(vectorInspector);
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return null;
      }
    };
    return new VectorCursor()
    {
      @Override
      public VectorColumnSelectorFactory getColumnSelectorFactory()
      {
        return nilFactory;
      }

      @Override
      public void advance()
      {
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public void reset()
      {
      }

      @Override
      public int getMaxVectorSize()
      {
        return vectorInspector.getMaxVectorSize();
      }

      @Override
      public int getCurrentVectorSize()
      {
        return 0;
      }
    };
  }

  @Override
  public List<OrderBy> getOrdering()
  {
    // A cursor with no rows trivially satisfies any ordering, so it can claim whatever the query asked for.
    return spec.getPreferredOrdering();
  }
}
