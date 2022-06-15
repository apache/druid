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
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.io.Closer;
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
import java.util.Set;
import java.util.TreeSet;

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
  public ColumnValuesWithUniqueFlag getNonNullColumnValues(String columnName, final int maxNumValues)
  {
    final int columnPosition = table.rowSignature().indexOf(columnName);

    if (columnPosition < 0) {
      return new ColumnValuesWithUniqueFlag(ImmutableSet.of(), false);
    }

    try (final IndexedTable.Reader reader = table.columnReader(columnPosition)) {
      // Sorted set to encourage "in" filters that result from this method to do dictionary lookups in order.
      // The hopes are that this will improve locality and therefore improve performance.
      final Set<String> allValues = createValuesSet();
      boolean allUnique = true;

      for (int i = 0; i < table.numRows(); i++) {
        final String s = DimensionHandlerUtils.convertObjectToString(reader.read(i));

        if (!NullHandling.isNullOrEquivalent(s)) {
          if (!allValues.add(s)) {
            // Duplicate found
            allUnique = false;
          }

          if (allValues.size() > maxNumValues) {
            return new ColumnValuesWithUniqueFlag(ImmutableSet.of(), false);
          }
        }
      }

      return new ColumnValuesWithUniqueFlag(allValues, allUnique);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<Set<String>> getCorrelatedColumnValues(
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
      Set<String> correlatedValues = createValuesSet();
      if (table.keyColumns().contains(searchColumnName)) {
        IndexedTable.Index index = table.columnIndex(filterColumnPosition);
        IndexedTable.Reader reader = table.columnReader(correlatedColumnPosition);
        closer.register(reader);
        IntList rowIndex = index.find(searchColumnValue);
        for (int i = 0; i < rowIndex.size(); i++) {
          int rowNum = rowIndex.getInt(i);
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

  /**
   * Create a Set that InDimFilter will accept without incurring a copy.
   */
  private static Set<String> createValuesSet()
  {
    return new TreeSet<>(Comparators.naturalNullsFirst());
  }
}
