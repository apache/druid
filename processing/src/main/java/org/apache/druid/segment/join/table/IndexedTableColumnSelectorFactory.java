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

package org.apache.druid.segment.join.table;

import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.IntSupplier;

public class IndexedTableColumnSelectorFactory implements ColumnSelectorFactory
{
  private final IndexedTable table;
  private final IntSupplier currentRow;

  IndexedTableColumnSelectorFactory(IndexedTable table, IntSupplier currentRow)
  {
    this.table = table;
    this.currentRow = currentRow;
  }

  @Nullable
  static ColumnCapabilities columnCapabilities(final IndexedTable table, final String columnName)
  {
    final ValueType valueType = table.rowSignature().getColumnType(columnName).orElse(null);

    if (valueType != null) {
      final ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl().setType(valueType);

      if (valueType == ValueType.STRING) {
        // IndexedTables are not _really_ dictionary-encoded, but we fake it using the row number as the dict. code.
        capabilities.setDictionaryEncoded(true);
      }

      capabilities.setDictionaryValuesSorted(false);
      capabilities.setDictionaryValuesUnique(false);
      capabilities.setHasMultipleValues(false);

      return capabilities;
    } else {
      return null;
    }
  }

  @Nonnull
  @Override
  public DimensionSelector makeDimensionSelector(final DimensionSpec dimensionSpec)
  {
    final int columnNumber = table.rowSignature().indexOf(dimensionSpec.getDimension());

    if (columnNumber < 0) {
      return dimensionSpec.decorate(DimensionSelector.constant(null, dimensionSpec.getExtractionFn()));
    } else {
      final DimensionSelector undecoratedSelector = new IndexedTableDimensionSelector(
          table,
          currentRow,
          columnNumber,
          dimensionSpec.getExtractionFn()
      );

      return dimensionSpec.decorate(undecoratedSelector);
    }
  }

  @Nonnull
  @Override
  public ColumnValueSelector makeColumnValueSelector(final String columnName)
  {
    final int columnNumber = table.rowSignature().indexOf(columnName);

    if (columnNumber < 0) {
      return NilColumnValueSelector.instance();
    } else {
      return new IndexedTableColumnValueSelector(table, currentRow, columnNumber);
    }
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(final String column)
  {
    return columnCapabilities(table, column);
  }
}
