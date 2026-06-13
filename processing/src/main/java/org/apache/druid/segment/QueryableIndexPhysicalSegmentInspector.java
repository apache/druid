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

import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.index.semantic.DictionaryEncodedStringValueIndex;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;

public class QueryableIndexPhysicalSegmentInspector implements PhysicalSegmentInspector
{
  private final QueryableIndex index;

  public QueryableIndexPhysicalSegmentInspector(QueryableIndex index)
  {
    this.index = index;
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }

  @Override
  @Nullable
  public Comparable getMinValue(String dimension)
  {
    // Clustered segments have no top-level column to read a per-segment min from (data is split across per-group
    // sub-indexes, each with its own dictionary). Report unknown rather than a wrong value.
    if (index.getClusteredBaseSummary() != null) {
      return null;
    }
    BaseColumnHolder columnHolder = index.getColumnHolder(dimension);
    if (columnHolder != null && columnHolder.getCapabilities().hasBitmapIndexes()) {
      ColumnIndexSupplier indexSupplier = columnHolder.getIndexSupplier();
      DictionaryEncodedStringValueIndex index = indexSupplier.as(DictionaryEncodedStringValueIndex.class);
      return index.getCardinality() > 0 ? index.getValue(0) : null;
    }
    return null;
  }

  @Override
  @Nullable
  public Comparable getMaxValue(String dimension)
  {
    if (index.getClusteredBaseSummary() != null) {
      return null;
    }
    BaseColumnHolder columnHolder = index.getColumnHolder(dimension);
    if (columnHolder != null && columnHolder.getCapabilities().hasBitmapIndexes()) {
      ColumnIndexSupplier indexSupplier = columnHolder.getIndexSupplier();
      DictionaryEncodedStringValueIndex index = indexSupplier.as(DictionaryEncodedStringValueIndex.class);
      return index.getCardinality() > 0 ? index.getValue(index.getCardinality() - 1) : null;
    }
    return null;
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    // Clustered segments have no top-level dictionary; per-group dictionaries aren't a single segment-wide
    // cardinality. Report unknown rather than the misleading "1" the null-column-holder branch below would give.
    if (index.getClusteredBaseSummary() != null) {
      return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
    }
    BaseColumnHolder columnHolder = index.getColumnHolder(column);
    if (columnHolder == null) {
      // NullDimensionSelector has cardinality = 1 (one null, nothing else).
      return 1;
    }
    try (BaseColumn col = columnHolder.getColumn()) {
      if (!(col instanceof DictionaryEncodedColumn)) {
        return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
      }
      return ((DictionaryEncodedColumn) col).getCardinality();
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    // For a clustered segment (empty top-level columns) the index resolves logical-column capabilities from its
    // summary + first group sub-index; for everything else this is the usual holder-based lookup.
    return index.getColumnCapabilities(column);
  }

  @Override
  public int getNumRows()
  {
    return index.getNumRows();
  }
}
