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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.TableProjectionSpec;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Catalog layout metadata for plain {@link TableProjectionSpec} base tables. The layout has no shape of its own beyond
 * the catalog column list (declared column order is the physical segment storage and sort order), so this metadata
 * carries only the optional {@link #virtualColumns},which the plain spec restricts to the query-granularity carrier,
 * and optional per-column {@link #columnSchemas} customizations; {@link #createSpec(List)} combines them with the
 * declared columns into the physical spec used to generate segments.
 * <p>
 * The optional {@link #columnSchemas} do NOT define columns, the catalog column list remains the logical schema,
 * they are per-column customizations of the exact {@link DimensionSchema} used to create segments for a declared
 * column, replacing the default schema derived from the declared column type.
 */
@JsonTypeName(TableBaseTableMetadata.TYPE_NAME)
public class TableBaseTableMetadata implements DatasourceBaseTableMetadata
{
  public static final String TYPE_NAME = TableProjectionSpec.TYPE_NAME;

  private final VirtualColumns virtualColumns;
  private final List<DimensionSchema> columnSchemas;

  @JsonCreator
  public TableBaseTableMetadata(
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
      @JsonProperty("columnSchemas") @Nullable List<DimensionSchema> columnSchemas
  )
  {
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    this.columnSchemas = columnSchemas == null ? Collections.emptyList() : columnSchemas;
  }

  @Override
  @JsonProperty("type")
  public String getType()
  {
    return TYPE_NAME;
  }

  @Override
  @JsonProperty("virtualColumns")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  /**
   * Per-column customizations of the {@link DimensionSchema} used during segment creation, keyed by
   * {@link DimensionSchema#getName()}; empty when every declared column uses the schema derived from its declared
   * type. These do not define columns: every entry must customize a declared column.
   */
  @JsonProperty("columnSchemas")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<DimensionSchema> getColumnSchemas()
  {
    return columnSchemas;
  }

  /**
   * Creates the physical spec from the declared catalog columns, used verbatim: the declared column order is the
   * physical segment order and sort order. All layout rules — the explicit {@code __time} position, and the
   * restriction of {@link #virtualColumns} to the query-granularity carrier — are enforced by the spec itself.
   * <p>
   * A column with an entry in {@link #columnSchemas} uses that {@link DimensionSchema} verbatim in place of the
   * default derived from its declared type. Customizations may only target declared columns ({@code __time} is always
   * a long), and the schema's type must match the declared logical type so the physical schema cannot silently
   * contradict the SQL schema.
   */
  @Override
  public TableProjectionSpec createSpec(List<ColumnSpec> columns)
  {
    if (CollectionUtils.isNullOrEmpty(columns)) {
      throw InvalidInput.exception(
          "Cannot define a [%s] base table without declared columns; the catalog column list defines the table schema",
          TYPE_NAME
      );
    }
    final Map<String, DimensionSchema> customSchemas = BaseTableColumns.indexColumnSchemas(columnSchemas);
    final Set<String> declaredNames = new HashSet<>();
    final List<DimensionSchema> specColumns = new ArrayList<>(columns.size());
    for (ColumnSpec column : columns) {
      declaredNames.add(column.name());
      specColumns.add(BaseTableColumns.toDimensionSchema(column, customSchemas.get(column.name()), TYPE_NAME));
    }
    for (String customized : customSchemas.keySet()) {
      if (!declaredNames.contains(customized)) {
        throw InvalidInput.exception(
            "columnSchemas entry [%s] does not customize a declared column; column schemas do not define columns,"
            + " declare [%s] in the table's column list",
            customized,
            customized
        );
      }
    }
    return TableProjectionSpec.builder()
                              .virtualColumns(virtualColumns)
                              .columns(specColumns)
                              .build();
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableBaseTableMetadata that = (TableBaseTableMetadata) o;
    return Objects.equals(virtualColumns, that.virtualColumns)
           && Objects.equals(columnSchemas, that.columnSchemas);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(virtualColumns, columnSchemas);
  }

  @Override
  public String toString()
  {
    return "TableBaseTableMetadata{" +
           "virtualColumns=" + virtualColumns +
           ", columnSchemas=" + columnSchemas +
           '}';
  }
}
