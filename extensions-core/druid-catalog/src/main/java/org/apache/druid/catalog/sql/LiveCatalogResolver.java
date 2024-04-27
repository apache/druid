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

package org.apache.druid.catalog.sql;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DatasourceTable.EffectiveColumnMetadata;
import org.apache.druid.sql.calcite.table.DatasourceTable.EffectiveMetadata;
import org.apache.druid.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;
import org.apache.druid.sql.calcite.table.DruidTable;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A catalog resolver that uses the catalog stored on the Druid coordinator.
 */
public class LiveCatalogResolver implements CatalogResolver
{
  private final MetadataCatalog catalog;

  @Inject
  public LiveCatalogResolver(final MetadataCatalog catalog)
  {
    this.catalog = catalog;
  }

  @Nullable
  private DatasourceFacade datasourceSpec(String name)
  {
    TableId tableId = TableId.datasource(name);
    ResolvedTable table = catalog.resolveTable(tableId);
    if (table == null) {
      return null;
    }
    if (!DatasourceDefn.isDatasource(table)) {
      return null;
    }
    return new DatasourceFacade(table);
  }

  /**
   * Create a {@link DruidTable} based on the physical segments, catalog entry, or both.
   */
  @Override
  public DruidTable resolveDatasource(String name, PhysicalDatasourceMetadata dsMetadata)
  {
    DatasourceFacade dsSpec = datasourceSpec(name);

    // No catalog metadata. If there is no physical metadata, then the
    // datasource does not exist. Else, if there is physical metadata, the
    // datasource is based entirely on the physical information.
    if (dsSpec == null) {
      return dsMetadata == null ? null : new DatasourceTable(dsMetadata);
    }
    if (dsMetadata == null) {
      // Datasource exists only in the catalog: no physical segments.
      return emptyDatasource(name, dsSpec);
    } else {
      // Datasource exists as both segments and a catalog entry.
      return mergeDatasource(dsMetadata, dsSpec);
    }
  }

  private DruidTable emptyDatasource(String name, DatasourceFacade dsSpec)
  {
    RowSignature.Builder builder = RowSignature.builder();
    Map<String, EffectiveColumnMetadata> columns = new HashMap<>();
    boolean hasTime = false;
    for (ColumnSpec col : dsSpec.columns()) {
      EffectiveColumnMetadata colMetadata = columnFromCatalog(col, null);
      if (colMetadata.name().equals(Columns.TIME_COLUMN)) {
        hasTime = true;
      }
      builder.add(col.name(), colMetadata.druidType());
      columns.put(col.name(), colMetadata);
    }
    if (!hasTime) {
      columns.put(Columns.TIME_COLUMN, new EffectiveColumnMetadata(
          Columns.TIME_COLUMN,
          ColumnType.LONG
      ));
      builder = RowSignature.builder()
          .add(Columns.TIME_COLUMN, ColumnType.LONG)
          .addAll(builder.build());
    }

    final PhysicalDatasourceMetadata mergedMetadata = new PhysicalDatasourceMetadata(
          new TableDataSource(name),
          builder.build(),
          false, // Cannot join to an empty table
          false // Cannot broadcast an empty table
    );
    return new DatasourceTable(
        mergedMetadata.getRowSignature(),
        mergedMetadata,
        new EffectiveMetadata(dsSpec, columns, true)
    );
  }

  private EffectiveColumnMetadata columnFromCatalog(ColumnSpec col, ColumnType physicalType)
  {
    ColumnType type = Columns.druidType(col);
    if (type != null) {
      // Use the type that the user provided.
    } else if (physicalType == null) {
      // Corner case: the user has defined a column in the catalog, has
      // not specified a type (meaning the user wants Druid to decide), but
      // there is no data at this moment. Guess String as the type for the
      // null values. If new segments appear between now and execution, we'll
      // convert the values to string, which is always safe.
      type = ColumnType.STRING;
    } else {
      type = physicalType;
    }
    return new EffectiveColumnMetadata(col.name(), type);
  }

  private DruidTable mergeDatasource(
      final PhysicalDatasourceMetadata dsMetadata,
      final DatasourceFacade dsSpec)
  {
    final RowSignature physicalSchema = dsMetadata.getRowSignature();
    Set<String> physicalCols = new HashSet<>(physicalSchema.getColumnNames());

    // Merge columns. All catalog-defined columns come first,
    // in the order defined in the catalog.
    final RowSignature.Builder builder = RowSignature.builder();
    Map<String, EffectiveColumnMetadata> columns = new HashMap<>();
    for (ColumnSpec col : dsSpec.columns()) {
      ColumnType physicalType = null;
      if (physicalCols.remove(col.name())) {
        physicalType = dsMetadata.getRowSignature().getColumnType(col.name()).get();
      }
      EffectiveColumnMetadata colMetadata = columnFromCatalog(col, physicalType);
      builder.add(col.name(), colMetadata.druidType());
      columns.put(col.name(), colMetadata);
    }

    // Mark any hidden columns. Assumes that the hidden columns are a disjoint set
    // from the defined columns.
    if (dsSpec.hiddenColumns() != null) {
      for (String colName : dsSpec.hiddenColumns()) {
        physicalCols.remove(colName);
      }
    }

    // Any remaining columns follow, if not marked as hidden
    // in the catalog.
    for (int i = 0; i < physicalSchema.size(); i++) {
      String colName = physicalSchema.getColumnName(i);
      if (!physicalCols.contains(colName)) {
        continue;
      }
      ColumnType physicalType = dsMetadata.getRowSignature().getColumnType(colName).get();
      EffectiveColumnMetadata colMetadata = EffectiveColumnMetadata.fromPhysical(colName, physicalType);
      columns.put(colName, colMetadata);
      builder.add(colName, physicalType);
    }

    EffectiveMetadata effectiveMetadata = new EffectiveMetadata(dsSpec, columns, false);
    return new DatasourceTable(builder.build(), dsMetadata, effectiveMetadata);
  }

  @Override
  public boolean ingestRequiresExistingTable()
  {
    return false;
  }

  @Override
  public Set<String> getTableNames(Set<String> datasourceNames)
  {
    Set<String> catalogTableNames = catalog.tableNames(TableId.DRUID_SCHEMA);
    if (catalogTableNames.isEmpty()) {
      return datasourceNames;
    }
    return ImmutableSet.<String>builder()
        .addAll(datasourceNames)
        .addAll(catalogTableNames)
        .build();
  }
}
