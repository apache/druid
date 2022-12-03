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

package org.apache.druid.catalog.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;
import org.apache.druid.java.util.common.IAE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Definition for all tables in the catalog. All tables have both
 * properties and a schema. Subclasses define specific table types
 * such as datasources or input tables. Some tables may be parameterized
 * to allow the table to appear in a SQL table function by implementing
 * the {@link ParameterizedDefn} interface.
 */
public class TableDefn extends ObjectDefn
{
  /**
   * Human-readable description of the datasource.
   */
  public static final String DESCRIPTION_PROPERTY = "description";

  private final Map<String, ColumnDefn> columnDefns;

  public TableDefn(
      final String name,
      final String typeValue,
      final List<PropertyDefn<?>> properties,
      final List<ColumnDefn> columnDefns
  )
  {
    super(
        name,
        typeValue,
        CatalogUtils.concatLists(
            Collections.singletonList(
                new ModelProperties.StringPropertyDefn(DESCRIPTION_PROPERTY)
            ),
            properties
        )
    );
    this.columnDefns = columnDefns == null ? Collections.emptyMap() : toColumnMap(columnDefns);
  }

  public static Map<String, ColumnDefn> toColumnMap(final List<ColumnDefn> colTypes)
  {
    ImmutableMap.Builder<String, ColumnDefn> builder = ImmutableMap.builder();
    for (ColumnDefn colType : colTypes) {
      builder.put(colType.typeValue(), colType);
    }
    return builder.build();
  }

  /**
   * Validate a table spec using the table, field and column definitions defined
   * here. The column definitions validate the type of each property value using
   * the object mapper.
   */
  public void validate(ResolvedTable table)
  {
    validate(table.properties(), table.jsonMapper());
    validateColumns(table.spec().columns(), table.jsonMapper());
  }

  public void validateColumns(List<ColumnSpec> columns, ObjectMapper jsonMapper)
  {
    if (columns == null) {
      return;
    }
    Set<String> names = new HashSet<>();
    for (ColumnSpec colSpec : columns) {
      if (!names.add(colSpec.name())) {
        throw new IAE("Duplicate column name: " + colSpec.name());
      }
      ColumnDefn.ResolvedColumn resolvedCol = resolveColumn(colSpec);
      resolvedCol.validate(jsonMapper);
    }
  }

  /**
   * Resolve the column type to produce a composite object that holds
   * both the definition and the column spec.
   */
  public ColumnDefn.ResolvedColumn resolveColumn(ColumnSpec spec)
  {
    String type = spec.type();
    if (Strings.isNullOrEmpty(type)) {
      throw new IAE("The column type is required.");
    }
    ColumnDefn defn = columnDefns.get(type);
    if (defn == null) {
      throw new IAE("Column type [%s] is not valid for tables of type [%s].", type, typeValue());
    }
    return new ColumnDefn.ResolvedColumn(defn, spec);
  }

  /**
   * Merge a table spec with an update. The merge affects both the properties and
   * the list of columns.
   */
  public TableSpec merge(TableSpec spec, TableSpec update, ObjectMapper jsonMapper)
  {
    String updateType = update.type();
    if (updateType != null && !spec.type().equals(updateType)) {
      throw new IAE("The update type must be null or [%s]", spec.type());
    }
    Map<String, Object> revisedProps = mergeProperties(spec.properties(), update.properties());
    List<ColumnSpec> revisedColumns = mergeColumns(spec.columns(), update.columns());
    TableSpec revisedSpec = new TableSpec(spec.type(), revisedProps, revisedColumns);
    validate(new ResolvedTable(this, revisedSpec, jsonMapper));
    return revisedSpec;
  }

  /**
   * Merge the set of columns from an existing spec and an update.
   * Columns are matched by name. If the column exists, then it is updated. If
   * the column does not exist, then the new column is appended to the existing
   * list. This merge operation cannot remove columns or change order.
   */
  public List<ColumnSpec> mergeColumns(List<ColumnSpec> columns, List<ColumnSpec> update)
  {
    if (update == null || update.isEmpty()) {
      return columns;
    }
    Map<String, Integer> original = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      original.put(columns.get(i).name(), i);
    }
    List<ColumnSpec> merged = new ArrayList<>(columns);
    for (int i = 0; i < update.size(); i++) {
      ColumnSpec col = update.get(i);
      String colName = col.name();
      if (Strings.isNullOrEmpty(colName)) {
        throw new IAE("Column %d must have a name", i + 1);
      }
      Integer index = original.get(col.name());
      if (index == null) {
        if (Strings.isNullOrEmpty(col.type())) {
          throw new IAE("Column %d must have a type", i + 1);
        }
        merged.add(col);
      } else {
        ColumnDefn.ResolvedColumn resolvedCol = resolveColumn(columns.get(index));
        merged.set(index, resolvedCol.merge(col).spec());
      }
    }
    return merged;
  }
}
