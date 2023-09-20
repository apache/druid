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

import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinMatcher;
import org.apache.druid.segment.join.Joinable;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class IndexedTableJoinable implements Joinable
{
  private final IndexedTable table;

  public IndexedTableJoinable(final IndexedTable table)
  {
    this.table = table;
  }

  @Override
  public List<String> getAvailableColumns()
  {
    return table.rowSignature().getColumnNames();
  }

  @Override
  public int getCardinality(String columnName)
  {
    if (table.rowSignature().contains(columnName)) {
      return IndexedTableDimensionSelector.computeDimensionSelectorCardinality(table);
    } else {
      // NullDimensionSelector has cardinality = 1 (one null, nothing else).
      return 1;
    }
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    return IndexedTableColumnSelectorFactory.columnCapabilities(table, columnName);
  }

  @Override
  public JoinMatcher makeJoinMatcher(
      final ColumnSelectorFactory leftColumnSelectorFactory,
      final JoinConditionAnalysis condition,
      final boolean remainderNeeded,
      boolean descending,
      Closer closer
  )
  {
    return new IndexedTableJoinMatcher(
        table,
        leftColumnSelectorFactory,
        condition,
        remainderNeeded,
        descending,
        closer
    );
  }

  @Override
  public ColumnValuesWithUniqueFlag getMatchableColumnValues(String columnName, boolean includeNull, int maxNumValues)
  {
    final int columnPosition = table.rowSignature().indexOf(columnName);
    final InDimFilter.ValuesSet matchableValues = InDimFilter.ValuesSet.create();

    if (columnPosition < 0) {
      return new ColumnValuesWithUniqueFlag(matchableValues /* empty set */, false);
    }

    try (final IndexedTable.Reader reader = table.columnReader(columnPosition)) {
      boolean allUnique = true;

      for (int i = 0; i < table.numRows(); i++) {
        final String s = DimensionHandlerUtils.convertObjectToString(reader.read(i));

        if (includeNull || !NullHandling.isNullOrEquivalent(s)) {
          if (!matchableValues.add(s)) {
            // Duplicate found
            allUnique = false;
          }

          if (matchableValues.size() > maxNumValues) {
            return new ColumnValuesWithUniqueFlag(ImmutableSet.of(), false);
          }
        }
      }

      return new ColumnValuesWithUniqueFlag(matchableValues, allUnique);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<InDimFilter.ValuesSet> getCorrelatedColumnValues(
      String searchColumnName,
      String searchColumnValue,
      String retrievalColumnName,
      long maxCorrelationSetSize,
      boolean allowNonKeyColumnSearch
  )
  {
    int filterColumnPosition = table.rowSignature().indexOf(searchColumnName);
    int correlatedColumnPosition = table.rowSignature().indexOf(retrievalColumnName);

    if (filterColumnPosition < 0 || correlatedColumnPosition < 0) {
      return Optional.empty();
    }
    try (final Closer closer = Closer.create()) {
      InDimFilter.ValuesSet correlatedValues = InDimFilter.ValuesSet.create();
      if (table.keyColumns().contains(searchColumnName)) {
        IndexedTable.Index index = table.columnIndex(filterColumnPosition);
        IndexedTable.Reader reader = table.columnReader(correlatedColumnPosition);
        closer.register(reader);
        final IntSortedSet rowIndex = index.find(searchColumnValue);
        final IntBidirectionalIterator rowIterator = rowIndex.iterator();
        while (rowIterator.hasNext()) {
          int rowNum = rowIterator.nextInt();
          String correlatedDimVal = DimensionHandlerUtils.convertObjectToString(reader.read(rowNum));
          correlatedValues.add(correlatedDimVal);

          if (correlatedValues.size() > maxCorrelationSetSize) {
            return Optional.empty();
          }
        }
        return Optional.of(correlatedValues);
      } else {
        if (!allowNonKeyColumnSearch) {
          return Optional.empty();
        }

        IndexedTable.Reader dimNameReader = table.columnReader(filterColumnPosition);
        IndexedTable.Reader correlatedColumnReader = table.columnReader(correlatedColumnPosition);
        closer.register(dimNameReader);
        closer.register(correlatedColumnReader);
        for (int i = 0; i < table.numRows(); i++) {
          String dimVal = Objects.toString(dimNameReader.read(i), null);
          if (searchColumnValue.equals(dimVal)) {
            String correlatedDimVal = DimensionHandlerUtils.convertObjectToString(correlatedColumnReader.read(i));
            correlatedValues.add(correlatedDimVal);
            if (correlatedValues.size() > maxCorrelationSetSize) {
              return Optional.empty();
            }
          }
        }

        return Optional.of(correlatedValues);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<Closeable> acquireReferences()
  {
    return table.acquireReferences();
  }
}
