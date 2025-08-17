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

package org.apache.druid.sql.calcite.schema;

import com.google.inject.Inject;
import org.apache.calcite.schema.Table;
import org.apache.druid.catalog.MetadataCatalog;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.table.IndexingTemplateDefn;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.table.InlineTable;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Creates the {@link TableId#INDEXING_TEMPLATE_SCHEMA indexing template schema}
 * for Druid SQL.
 */
public class IndexingTemplateSchema extends AbstractTableSchema
{
  private static final RowSignature ROW_SIGNATURE =
      RowSignature.builder()
                  .add("type", ColumnType.STRING)
                  .add("payload", ColumnType.UNKNOWN_COMPLEX)
                  .build();

  private final MetadataCatalog catalog;

  @Inject
  public IndexingTemplateSchema(MetadataCatalog catalog)
  {
    this.catalog = catalog;
  }

  @Override
  @Nullable
  public Table getTable(String name)
  {
    final TableId tableId = TableId.of(TableId.INDEXING_TEMPLATE_SCHEMA, name);
    final ResolvedTable resolvedTable = catalog.resolveTable(tableId);
    if (resolvedTable == null) {
      return null;
    }

    final Map<String, Object> template
        = resolvedTable.mapProperty(IndexingTemplateDefn.PROPERTY_PAYLOAD);
    final InlineDataSource dataSource = InlineDataSource.fromIterable(
        Collections.singletonList(
            new Object[]{template.get("type"), template}
        ),
        ROW_SIGNATURE
    );
    return new InlineTable(dataSource);
  }

  @Override
  public Set<String> getTableNames()
  {
    return catalog.tableNames(TableId.INDEXING_TEMPLATE_SCHEMA);
  }
}
