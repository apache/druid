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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.impl.BaseTableProjectionSpec;
import org.apache.druid.segment.VirtualColumns;

import java.util.List;

/**
 * Catalog model for the physical layout of a datasource "base table", the counterpart of
 * {@link BaseTableProjectionSpec} for tables defined in the catalog. Unlike the physical spec, this metadata does NOT
 * declare the table schema: the catalog column list remains the single source of truth for column names, types, and
 * order. Implementations carry only the layout details the column list cannot express (such as clustering columns and
 * ingest-time virtual columns), and {@link #createSpec(List)} combines the two into the physical spec used to
 * generate segments.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(
        name = ClusteredValueGroupsBaseTableMetadata.TYPE_NAME,
        value = ClusteredValueGroupsBaseTableMetadata.class
    )
})
public interface DatasourceBaseTableMetadata
{
  /**
   * The type discriminator, declared as a regular JSON property ({@link JsonTypeInfo.As#EXISTING_PROPERTY}) rather
   * than a synthetic one so that it is emitted even when an instance is serialized from an untyped context;catalog
   * property values live in a plain {@code Map<String, Object>}, where Jackson serializes by runtime type and would
   * otherwise drop a synthetic type id.
   */
  String getType();

  /**
   * Virtual columns computed at ingest time to populate stored columns (for example a clustering column derived from
   * an expression over raw input columns). The inputs these virtual columns require are consumed at ingest but are
   * not themselves stored columns.
   */
  VirtualColumns getVirtualColumns();

  /**
   * Creates the physical {@link BaseTableProjectionSpec} by combining this layout metadata with the declared catalog
   * columns. Throws {@link org.apache.druid.error.DruidException} if the layout is inconsistent with the declared
   * columns, so this doubles as the cross-validation of the catalog property against the column list.
   */
  BaseTableProjectionSpec createSpec(List<ColumnSpec> columns);
}
