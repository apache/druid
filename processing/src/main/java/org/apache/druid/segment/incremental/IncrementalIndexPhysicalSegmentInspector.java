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

import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;

public class IncrementalIndexPhysicalSegmentInspector implements PhysicalSegmentInspector
{
  static final ColumnCapabilities.CoercionLogic SNAPSHOT_COERCE_LOGIC =
      new ColumnCapabilities.CoercionLogic()
      {
        @Override
        public boolean dictionaryEncoded()
        {
          return true;
        }

        @Override
        public boolean dictionaryValuesSorted()
        {
          return true;
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

  public IncrementalIndexPhysicalSegmentInspector(IncrementalIndex index)
  {
    this.index = index;
  }

  @Nullable
  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    IncrementalIndex.DimensionDesc desc = index.getDimension(column);
    if (desc == null) {
      return null;
    }

    DimensionIndexer indexer = desc.getIndexer();
    return indexer.getMinValue();
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    IncrementalIndex.DimensionDesc desc = index.getDimension(column);
    if (desc == null) {
      return null;
    }

    DimensionIndexer indexer = desc.getIndexer();
    return indexer.getMaxValue();
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    if (column.equals(ColumnHolder.TIME_COLUMN_NAME)) {
      return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
    }

    IncrementalIndex.DimensionDesc desc = index.getDimension(column);
    if (desc == null) {
      // non-existent dimension has cardinality = 1 (one null, nothing else).
      return 1;
    }

    return desc.getIndexer().getCardinality();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return ColumnCapabilitiesImpl.snapshot(
        index.getColumnCapabilities(column),
        SNAPSHOT_COERCE_LOGIC
    );
  }

  @Override
  public int getNumRows()
  {
    return index.numRows();
  }
}
