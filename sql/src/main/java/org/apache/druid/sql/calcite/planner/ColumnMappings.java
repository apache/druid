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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.RowSignature;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Maps column names from the query to output names desired by the user, in the order
 * desired by the user.
 *
 * The query is translated by {@link org.apache.druid.msq.querykit.QueryKit} into
 * a {@link org.apache.druid.msq.kernel.QueryDefinition}. So, this class also represents mappings from
 * {@link org.apache.druid.msq.kernel.QueryDefinition#getFinalStageDefinition()} into the output names desired
 * by the user.
 */
public class ColumnMappings
{
  private final List<ColumnMapping> mappings;
  private final Map<String, IntList> outputColumnNameToPositionMap;
  private final Map<String, IntList> queryColumnNameToPositionMap;

  @JsonCreator
  public ColumnMappings(final List<ColumnMapping> mappings)
  {
    this.mappings = Preconditions.checkNotNull(mappings, "mappings");
    this.outputColumnNameToPositionMap = new HashMap<>();
    this.queryColumnNameToPositionMap = new HashMap<>();

    for (int i = 0; i < mappings.size(); i++) {
      final ColumnMapping mapping = mappings.get(i);
      outputColumnNameToPositionMap.computeIfAbsent(mapping.getOutputColumn(), k -> new IntArrayList()).add(i);
      queryColumnNameToPositionMap.computeIfAbsent(mapping.getQueryColumn(), k -> new IntArrayList()).add(i);
    }
  }

  public static ColumnMappings identity(final RowSignature signature)
  {
    return new ColumnMappings(
        signature.getColumnNames()
                 .stream()
                 .map(column -> new ColumnMapping(column, column))
                 .collect(Collectors.toList())
    );
  }

  /**
   * Number of output columns.
   */
  public int size()
  {
    return mappings.size();
  }

  /**
   * All output column names, in order. Some names may appear more than once, unless
   * {@link #hasUniqueOutputColumnNames()} is true.
   */
  public List<String> getOutputColumnNames()
  {
    return mappings.stream().map(ColumnMapping::getOutputColumn).collect(Collectors.toList());
  }

  /**
   * Whether output column names from {@link #getOutputColumnNames()} are all unique.
   */
  public boolean hasUniqueOutputColumnNames()
  {
    final Set<String> encountered = new HashSet<>();

    for (final ColumnMapping mapping : mappings) {
      if (!encountered.add(mapping.getOutputColumn())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Whether a particular output column name exists.
   */
  public boolean hasOutputColumn(final String outputColumnName)
  {
    return outputColumnNameToPositionMap.containsKey(outputColumnName);
  }

  /**
   * Query column name for a particular output column position.
   *
   * @throws IllegalArgumentException if the output column position is out of range
   */
  public String getQueryColumnName(final int outputColumn)
  {
    if (outputColumn < 0 || outputColumn >= mappings.size()) {
      throw new IAE("Output column position[%d] out of range", outputColumn);
    }

    return mappings.get(outputColumn).getQueryColumn();
  }

  /**
   * Output column name for a particular output column position.
   *
   * @throws IllegalArgumentException if the output column position is out of range
   */
  public String getOutputColumnName(final int outputColumn)
  {
    if (outputColumn < 0 || outputColumn >= mappings.size()) {
      throw new IAE("Output column position[%d] out of range", outputColumn);
    }

    return mappings.get(outputColumn).getOutputColumn();
  }

  /**
   * Output column positions for a particular output column name.
   */
  public IntList getOutputColumnsByName(final String outputColumnName)
  {
    return outputColumnNameToPositionMap.getOrDefault(outputColumnName, IntLists.emptyList());
  }

  /**
   * Output column positions for a particular query column name.
   */
  public IntList getOutputColumnsForQueryColumn(final String queryColumnName)
  {
    final IntList outputColumnPositions = queryColumnNameToPositionMap.get(queryColumnName);

    if (outputColumnPositions == null) {
      return IntLists.emptyList();
    }

    return outputColumnPositions;
  }

  @JsonValue
  public List<ColumnMapping> getMappings()
  {
    return mappings;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnMappings that = (ColumnMappings) o;
    return Objects.equals(mappings, that.mappings);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(mappings);
  }

  @Override
  public String toString()
  {
    return "ColumnMappings{" +
           "mappings=" + mappings +
           '}';
  }
}
