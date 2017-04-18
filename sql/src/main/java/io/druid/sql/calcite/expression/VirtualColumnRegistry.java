/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.druid.java.util.common.ISE;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.virtual.ExtractionFnVirtualColumn;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexNode;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * A utility class that helps {@link Expressions} track which virtual columns are used by row extractions. This can
 * allow sharing of virtual columns between different extractions, leading to nicer generated queries.
 */
public class VirtualColumnRegistry
{
  private final RowSignature sourceRowSignature;
  private final Map<String, VirtualColumn> virtualColumnsByName = new HashMap<>();
  private final Map<String, VirtualColumn> virtualColumnsByRexNode = new HashMap<>();

  public VirtualColumnRegistry(final RowSignature sourceRowSignature)
  {
    this.sourceRowSignature = sourceRowSignature;
  }

  public VirtualColumn get(final String columnName)
  {
    return virtualColumnsByName.get(columnName);
  }

  public RowSignature getSourceRowSignature()
  {
    return sourceRowSignature;
  }

  public VirtualColumn register(final RexNode rexNode, final Function<String, VirtualColumn> columnMaker)
  {
    // Assumes that rexNode.toString() equality is a good proxy for rexNode equivalence. I think this is true
    // since Calcite sets it to a digest.
    final VirtualColumn virtualColumn = virtualColumnsByRexNode.compute(
        rexNode.toString(),
        (key, oldValue) -> {
          if (oldValue == null) {
            // Find an unused name.
            String name = Calcites.sanitizeColumnName(key);
            while (sourceRowSignature.getColumnType(name) != null || virtualColumnsByName.containsKey(name)) {
              name = name + "_";
            }
            return columnMaker.apply(name);
          } else if (oldValue.equals(columnMaker.apply(oldValue.getOutputName()))) {
            return oldValue;
          } else {
            throw new ISE("Attempt to put virtualColumns for expression[%s]", key);
          }
        }
    );

    virtualColumnsByName.put(virtualColumn.getOutputName(), virtualColumn);
    return virtualColumn;
  }

  public VirtualColumn register(final RexNode rexNode, final SimpleExtraction simpleExtraction)
  {
    return register(rexNode, (String name) -> {
      final VirtualColumn input = get(simpleExtraction.getColumn());
      if (input instanceof ExtractionFnVirtualColumn) {
        return new ExtractionFnVirtualColumn(
            name,
            ((ExtractionFnVirtualColumn) input).getFieldName(),
            ExtractionFns.cascade(
                ((ExtractionFnVirtualColumn) input).getExtractionFn(),
                simpleExtraction.getExtractionFn()
            )
        );
      } else {
        return new ExtractionFnVirtualColumn(name, simpleExtraction.getColumn(), simpleExtraction.getExtractionFn());
      }
    });
  }

  public SimpleExtraction toSimpleExtraction(final String columnName)
  {
    final VirtualColumn column = virtualColumnsByName.get(Preconditions.checkNotNull(columnName, "columnName"));

    // Strip the virtual column layer out completely, if we can.
    if (column instanceof ExtractionFnVirtualColumn) {
      final ExtractionFnVirtualColumn extractionColumn = (ExtractionFnVirtualColumn) column;
      if (sourceRowSignature.getColumnType(extractionColumn.getFieldName()) != null) {
        return SimpleExtraction.of(extractionColumn.getFieldName(), extractionColumn.getExtractionFn());
      }
    }

    return SimpleExtraction.of(columnName, null);
  }

  public VirtualColumns subset(final List<String> columnNames)
  {
    final Set<VirtualColumn> retVal = new TreeSet<>(Comparator.comparing(VirtualColumn::getOutputName));

    for (String columnName : columnNames) {
      for (String requiredColumnName : requiredVirtualColumnsRecursively(columnName)) {
        retVal.add(virtualColumnsByName.get(requiredColumnName));
      }
    }

    return VirtualColumns.create(ImmutableList.copyOf(retVal));
  }

  private Set<String> requiredVirtualColumnsRecursively(final String columnName)
  {
    final VirtualColumn column = virtualColumnsByName.get(columnName);
    if (column != null) {
      final Set<String> retVal = new HashSet<>();

      retVal.add(columnName);

      for (String requiredColumnName : column.requiredColumns()) {
        retVal.addAll(requiredVirtualColumnsRecursively(requiredColumnName));
      }

      return retVal;
    } else {
      return ImmutableSet.of();
    }
  }

  @Override
  public String toString()
  {
    return "VirtualColumnRegistry{" +
           "virtualColumnsByName=" + virtualColumnsByName +
           '}';
  }
}
