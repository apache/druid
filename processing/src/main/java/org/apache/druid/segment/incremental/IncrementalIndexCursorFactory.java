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

package org.apache.druid.segment.incremental;

import com.google.common.collect.Iterables;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.NestedDataColumnIndexerV4;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.projections.Projection;

import javax.annotation.Nullable;

public class IncrementalIndexCursorFactory implements CursorFactory
{
  private static final ColumnCapabilities.CoercionLogic COERCE_LOGIC =
      new ColumnCapabilities.CoercionLogic()
      {
        @Override
        public boolean dictionaryEncoded()
        {
          return false;
        }

        @Override
        public boolean dictionaryValuesSorted()
        {
          return false;
        }

        @Override
        public boolean dictionaryValuesUnique()
        {
          return true;
        }

        @Override
        public boolean multipleValues()
        {
          return true;
        }

        @Override
        public boolean hasNulls()
        {
          return true;
        }
      };

  private final IncrementalIndex index;

  public IncrementalIndexCursorFactory(IncrementalIndex index)
  {
    this.index = index;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    final Projection<IncrementalIndexRowSelector> projection = index.getProjection(spec);
    if (projection == null) {
      return new IncrementalIndexCursorHolder(index, spec);
    } else {
      // currently we only have aggregated projections, so isPreAggregated is always true
      return new IncrementalIndexCursorHolder(
          projection.getRowSelector(),
          projection.getCursorBuildSpec()
      )
      {
        @Override
        public ColumnSelectorFactory makeSelectorFactory(CursorBuildSpec buildSpec, IncrementalIndexRowHolder currEntry)
        {
          return projection.wrapColumnSelectorFactory(super.makeSelectorFactory(buildSpec, currEntry));
        }

        @Override
        public boolean isPreAggregated()
        {
          return true;
        }
      };
    }
  }

  @Override
  public RowSignature getRowSignature()
  {
    final RowSignature.Builder builder = RowSignature.builder();

    for (final String column : Iterables.concat(index.getDimensionNames(true), index.getMetricNames())) {
      builder.add(column, ColumnType.fromCapabilities(index.getColumnCapabilities(column)));
    }

    return builder.build();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return snapshotColumnCapabilities(index, column);
  }

  static ColumnCapabilities snapshotColumnCapabilities(IncrementalIndexRowSelector selector, String column)
  {
    IncrementalIndex.DimensionDesc desc = selector.getDimension(column);
    // nested column indexer is a liar, and behaves like any type if it only processes unnested literals of a single
    // type, so force it to use nested column type
    if (desc != null && desc.getIndexer() instanceof NestedDataColumnIndexerV4) {
      return ColumnCapabilitiesImpl.createDefault().setType(ColumnType.NESTED_DATA);
    }

    // Different from index.getColumnCapabilities because, in a way, IncrementalIndex's string-typed dimensions
    // are always potentially multi-valued at query time. (Missing / null values for a row can potentially be
    // represented by an empty array; see StringDimensionIndexer.IndexerDimensionSelector's getRow method.)
    //
    // We don't want to represent this as having-multiple-values in index.getCapabilities, because that's used
    // at index-persisting time to determine if we need a multi-value column or not. However, that means we
    // need to tweak the capabilities here in the CursorFactory (a query-time construct), so at query time
    // they appear multi-valued.
    //
    // Note that this could be improved if we snapshot the capabilities at cursor creation time and feed those through
    // to the StringDimensionIndexer so the selector built on top of it can produce values from the snapshot state of
    // multi-valuedness at cursor creation time, instead of the latest state, and getSnapshotColumnCapabilities could
    // be removed.
    return ColumnCapabilitiesImpl.snapshot(
        selector.getColumnCapabilities(column),
        COERCE_LOGIC
    );
  }
}
