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
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.projections.QueryableProjection;

import javax.annotation.Nullable;
import java.util.List;

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
          return false;
        }

        @Override
        public boolean hasNulls()
        {
          return false;
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
    final QueryableProjection<IncrementalIndexRowSelector> projection = index.getProjection(spec);
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

        @Override
        public List<AggregatorFactory> getAggregatorsForPreAggregated()
        {
          return projection.getCursorBuildSpec().getAggregators();
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
    return ColumnCapabilitiesImpl.snapshot(
        selector.getColumnCapabilities(column),
        COERCE_LOGIC
    );
  }
}
