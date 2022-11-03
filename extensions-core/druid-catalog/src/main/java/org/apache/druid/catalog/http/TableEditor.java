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

package org.apache.druid.catalog.http;

import com.google.common.base.Strings;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.http.TableEditRequest.DropColumns;
import org.apache.druid.catalog.http.TableEditRequest.HideColumns;
import org.apache.druid.catalog.http.TableEditRequest.MoveColumn;
import org.apache.druid.catalog.http.TableEditRequest.MoveColumn.Position;
import org.apache.druid.catalog.http.TableEditRequest.UnhideColumns;
import org.apache.druid.catalog.http.TableEditRequest.UpdateColumns;
import org.apache.druid.catalog.http.TableEditRequest.UpdateProperties;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.AbstractDatasourceDefn;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.CollectionUtils;

import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Performs an incremental update of an existing table. Allows the user to
 * provide very specific "edit" commands without the need to download the
 * entire table spec to make a typical change.
 * <p>
 * The process is that the edit "request" is in the form of a subclass of
 * {@link TableEditRequest}. The DB layer starts a transaction, then calls
 * back to this class to perform the edit. The DB layer then updates the
 * table spec in the DB with the returned information. This ensures that
 * a) we use actual (non-optimistic) locking, and b) allows the edit
 * operation itself to reside here and not in the DB layer.
 */
public class TableEditor
{
  private final CatalogStorage catalog;
  private final TableId id;
  private final TableEditRequest editRequest;

  public TableEditor(
      final CatalogStorage catalog,
      final TableId id,
      final TableEditRequest editRequest
  )
  {
    this.catalog = catalog;
    this.id = id;
    this.editRequest = editRequest;
  }

  /**
   * Perform the edit operation.
   *
   * @return the updated table version, or 0 if the operation was not applied.
   *         the 0 value indicates, say, an empty or no-op edit request
   * @throws CatalogException for errors. Mapped to an HTTP response in the
   *         {@link CatalogResource} class
   */
  public long go() throws CatalogException
  {
    if (editRequest instanceof HideColumns) {
      return hideColumns(((HideColumns) editRequest).columns);
    } else if (editRequest instanceof UnhideColumns) {
      return unHideColumns(((UnhideColumns) editRequest).columns);
    } else if (editRequest instanceof DropColumns) {
      return dropColumns(((DropColumns) editRequest).columns);
    } else if (editRequest instanceof UpdateProperties) {
      return updateProperties(((UpdateProperties) editRequest).properties);
    } else if (editRequest instanceof UpdateColumns) {
      return updateColumns(((UpdateColumns) editRequest).columns);
    } else if (editRequest instanceof MoveColumn) {
      return moveColumn(((MoveColumn) editRequest));
    } else {
      // More of a server error: if we can deserialize the request,
      // we should know how to perform that request.
      throw CatalogException.badRequest(
          "Unknown edit request: %s",
          editRequest.getClass().getSimpleName()
      );
    }
  }

  private long hideColumns(List<String> columns) throws CatalogException
  {
    if (CollectionUtils.isNullOrEmpty(columns)) {
      return 0;
    }
    return catalog.tables().updateProperties(
        id,
        table -> applyHiddenColumns(table, columns)
    );
  }

  /**
   * Given the existing set of properties, which may contain a list of hidden columns,
   * perform the update action to add the requested new columns (if they don't yet exist).
   *
   * @return revised properties with the revised hidden columns list after applying
   * the requested changes
   */
  private TableSpec applyHiddenColumns(TableMetadata table, List<String> columns) throws CatalogException
  {
    if (!AbstractDatasourceDefn.isDatasource(table.spec().type())) {
      throw CatalogException.badRequest("hideColumns is supported only for data source specs");
    }
    TableSpec spec = table.spec();
    if (columns.isEmpty()) {
      return null;
    }
    Map<String, Object> props = spec.properties();
    @SuppressWarnings("unchecked")
    List<String> hiddenColumns = (List<String>) props.get(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY);
    if (hiddenColumns == null) {
      hiddenColumns = Collections.emptyList();
    }
    Set<String> existing = new HashSet<>(hiddenColumns);
    List<String> revised = new ArrayList<>(hiddenColumns);
    for (String col : columns) {
      if (!existing.contains(col)) {
        revised.add(col);
        existing.add(col);
      }
    }
    if (revised.size() == hiddenColumns.size()) {
      // Nothing changed
      return null;
    }
    Map<String, Object> revisedProps = new HashMap<>(props);
    revisedProps.put(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY, revised);
    return spec.withProperties(revisedProps);
  }

  private long unHideColumns(List<String> columns) throws CatalogException
  {
    if (CollectionUtils.isNullOrEmpty(columns)) {
      return 0;
    }
    return catalog.tables().updateProperties(
        id,
        table -> applyUnhideColumns(table, columns)
    );
  }

  /**
   * Given the existing set of properties, which may contain a list of hidden columns,
   * perform the update action remove the requested columns (if they exist).
   *
   * @return revised properties with the revised hidden columns list after applying
   * the requested changes
   */
  private TableSpec applyUnhideColumns(TableMetadata table, List<String> columns) throws CatalogException
  {
    final TableSpec existingSpec = table.spec();
    if (!AbstractDatasourceDefn.isDatasource(existingSpec.type())) {
      throw CatalogException.badRequest("hideColumns is supported only for data source specs");
    }

    final Map<String, Object> props = existingSpec.properties();
    @SuppressWarnings("unchecked")
    List<String> hiddenColumns = (List<String>) props.get(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY);
    if (hiddenColumns == null || columns.isEmpty()) {
      return null;
    }
    Set<String> removals = new HashSet<>(columns);
    List<String> revisedHiddenCols = new ArrayList<>();
    for (String col : hiddenColumns) {
      if (!removals.contains(col)) {
        revisedHiddenCols.add(col);
      }
    }
    if (revisedHiddenCols.size() == hiddenColumns.size() && !hiddenColumns.isEmpty()) {
      // Nothing changed
      return null;
    }
    final Map<String, Object> revisedProps = new HashMap<>(props);
    if (revisedHiddenCols.isEmpty()) {
      revisedProps.remove(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY);
    } else {
      revisedProps.put(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY, revisedHiddenCols);
    }
    return existingSpec.withProperties(revisedProps);
  }

  private long dropColumns(List<String> columnsToDrop) throws CatalogException
  {
    if (CollectionUtils.isNullOrEmpty(columnsToDrop)) {
      return 0;
    }
    return catalog.tables().updateColumns(
        id,
        table -> applyDropColumns(table, columnsToDrop)
    );
  }

  private TableSpec applyDropColumns(final TableMetadata table, final List<String> toDrop)
  {
    final TableSpec existingSpec = table.spec();
    List<ColumnSpec> existingColumns = existingSpec.columns();
    if (CollectionUtils.isNullOrEmpty(existingColumns)) {
      return null;
    }
    Set<String> drop = new HashSet<>(toDrop);
    List<ColumnSpec> revised = new ArrayList<>();
    for (ColumnSpec col : existingColumns) {
      if (!drop.contains(col.name())) {
        revised.add(col);
      }
    }
    if (revised.size() == existingColumns.size()) {
      // Nothing changed
      return null;
    }
    return existingSpec.withColumns(revised);
  }


  private long updateProperties(Map<String, Object> updates) throws CatalogException
  {
    if (updates == null || updates.isEmpty()) {
      return 0;
    }
    return catalog.tables().updateProperties(
        id,
        table -> applyUpdateProperties(table, updates)
    );
  }

  private TableSpec applyUpdateProperties(
      final TableMetadata table,
      final Map<String, Object> updates
  ) throws CatalogException
  {
    final TableSpec existingSpec = table.spec();
    final TableDefn defn = resolveDefn(existingSpec.type());
    final Map<String, Object> revised = defn.mergeProperties(
        existingSpec.properties(),
        updates
    );
    try {
      defn.validate(revised, catalog.jsonMapper());
    }
    catch (IAE e) {
      throw CatalogException.badRequest(e.getMessage());
    }
    return existingSpec.withProperties(revised);
  }

  private TableDefn resolveDefn(String tableType) throws CatalogException
  {
    TableDefn defn = catalog.tableRegistry().defnFor(tableType);
    if (defn == null) {
      throw new CatalogException(
          CatalogException.BAD_STATE,
          Response.Status.INTERNAL_SERVER_ERROR,
          "Table %s has an invalid type [%s]",
          id.sqlName(),
          tableType
      );
    }
    return defn;
  }

  private long updateColumns(final List<ColumnSpec> updates) throws CatalogException
  {
    if (CollectionUtils.isNullOrEmpty(updates)) {
      return 0;
    }
    return catalog.tables().updateColumns(
        id,
        table -> applyUpdateColumns(table, updates)
    );
  }

  private TableSpec applyUpdateColumns(
      final TableMetadata table,
      final List<ColumnSpec> updates
  ) throws CatalogException
  {
    final TableSpec existingSpec = table.spec();
    final TableDefn defn = resolveDefn(existingSpec.type());
    final List<ColumnSpec> revised = defn.mergeColumns(existingSpec.columns(), updates);
    try {
      defn.validateColumns(revised, catalog.jsonMapper());
    }
    catch (IAE e) {
      throw CatalogException.badRequest(e.getMessage());
    }
    return existingSpec.withColumns(revised);
  }

  private long moveColumn(MoveColumn moveColumn) throws CatalogException
  {
    if (Strings.isNullOrEmpty(moveColumn.column)) {
      throw CatalogException.badRequest("A column name is required");
    }
    if (moveColumn.where == null) {
      throw CatalogException.badRequest("A target location is required");
    }
    if ((moveColumn.where == Position.BEFORE || moveColumn.where == Position.AFTER) && Strings.isNullOrEmpty(moveColumn.anchor)) {
      throw CatalogException.badRequest("A anchor column is required for BEFORE or AFTER");
    }
    return catalog.tables().updateColumns(
        id,
        table -> applyMoveColumn(table, moveColumn)
    );
  }

  private TableSpec applyMoveColumn(
      final TableMetadata table,
      final MoveColumn moveColumn
  ) throws CatalogException
  {
    final TableSpec existingSpec = table.spec();
    final List<ColumnSpec> existingCols = existingSpec.columns();
    final List<ColumnSpec> revised = new ArrayList<>(existingCols);
    final int colPosn = findColumn(existingCols, moveColumn.column);
    if (colPosn == -1) {
      throw CatalogException.badRequest("Column [%s] is not defined", moveColumn.column);
    }
    int anchorPosn;
    if (moveColumn.where == Position.BEFORE || moveColumn.where == Position.AFTER) {
      anchorPosn = findColumn(existingCols, moveColumn.anchor);
      if (anchorPosn == -1) {
        throw CatalogException.badRequest("Anchor [%s] is not defined", moveColumn.column);
      }
      if (anchorPosn > colPosn) {
        anchorPosn--;
      }
    } else {
      anchorPosn = -1;
    }

    ColumnSpec col = revised.remove(colPosn);
    switch (moveColumn.where) {
      case FIRST:
        revised.add(0, col);
        break;
      case LAST:
        revised.add(col);
        break;
      case BEFORE:
        revised.add(anchorPosn, col);
        break;
      case AFTER:
        revised.add(anchorPosn + 1, col);
        break;
    }

    return existingSpec.withColumns(revised);
  }

  private static int findColumn(List<ColumnSpec> columns, String colName)
  {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).name().equals(colName)) {
        return i;
      }
    }
    return -1;
  }
}
