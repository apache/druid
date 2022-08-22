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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ColumnMappings
{
  private final List<ColumnMapping> mappings;
  private final Map<String, String> outputToQueryColumnMap;
  private final Map<String, List<String>> queryToOutputColumnsMap;

  @JsonCreator
  public ColumnMappings(final List<ColumnMapping> mappings)
  {
    this.mappings = validateNoDuplicateOutputColumns(Preconditions.checkNotNull(mappings, "mappings"));
    this.outputToQueryColumnMap = new HashMap<>();
    this.queryToOutputColumnsMap = new HashMap<>();

    for (final ColumnMapping mapping : mappings) {
      outputToQueryColumnMap.put(mapping.getOutputColumn(), mapping.getQueryColumn());
      queryToOutputColumnsMap.computeIfAbsent(mapping.getQueryColumn(), k -> new ArrayList<>())
                             .add(mapping.getOutputColumn());
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

  public List<String> getOutputColumnNames()
  {
    return mappings.stream().map(ColumnMapping::getOutputColumn).collect(Collectors.toList());
  }

  public boolean hasOutputColumn(final String columnName)
  {
    return outputToQueryColumnMap.containsKey(columnName);
  }

  public String getQueryColumnForOutputColumn(final String outputColumn)
  {
    final String queryColumn = outputToQueryColumnMap.get(outputColumn);
    if (queryColumn != null) {
      return queryColumn;
    } else {
      throw new IAE("No such output column [%s]", outputColumn);
    }
  }

  public List<String> getOutputColumnsForQueryColumn(final String queryColumn)
  {
    final List<String> outputColumns = queryToOutputColumnsMap.get(queryColumn);
    if (outputColumns != null) {
      return outputColumns;
    } else {
      return Collections.emptyList();
    }
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

  private static List<ColumnMapping> validateNoDuplicateOutputColumns(final List<ColumnMapping> mappings)
  {
    final Set<String> encountered = new HashSet<>();

    for (final ColumnMapping mapping : mappings) {
      if (!encountered.add(mapping.getOutputColumn())) {
        throw new ISE("Duplicate output column [%s]", mapping.getOutputColumn());
      }
    }

    return mappings;
  }
}
