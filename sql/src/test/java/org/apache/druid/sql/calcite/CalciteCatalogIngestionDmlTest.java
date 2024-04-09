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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DruidTable;

public class CalciteCatalogIngestionDmlTest extends CalciteIngestionDmlTest
{
  public ImmutableMap<String, DruidTable> resolvedTables = ImmutableMap.of(
      "hourDs", new DatasourceTable(
          RowSignature.builder().addTimeColumn().build(),
          new DatasourceTable.PhysicalDatasourceMetadata(
              new TableDataSource("hourDs"),
              RowSignature.builder().addTimeColumn().build(),
              false,
              false
          ),
          new DatasourceTable.EffectiveMetadata(
              new DatasourceFacade(new ResolvedTable(
                  new TableDefn(
                      "foo",
                      DatasourceDefn.TABLE_TYPE,
                      null,
                      null
                  ),
                  new TableSpec(
                      DatasourceDefn.TABLE_TYPE,
                      ImmutableMap.of(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "PT1H"),
                      ImmutableList.of(
                          new ColumnSpec("__time", Columns.TIME_COLUMN, null)
                      )
                  ),
                  MAPPER
              )),
              DatasourceTable.EffectiveMetadata.toEffectiveColumns(
                  RowSignature.builder()
                      .addTimeColumn()
                      .build()),
              false
          )
      ),
      "noPartitonedBy", new DatasourceTable(
          RowSignature.builder().addTimeColumn().build(),
          new DatasourceTable.PhysicalDatasourceMetadata(
              new TableDataSource("hourDs"),
              RowSignature.builder().addTimeColumn().build(),
              false,
              false
          ),
          new DatasourceTable.EffectiveMetadata(
              new DatasourceFacade(new ResolvedTable(
                  new TableDefn(
                      "foo",
                      DatasourceDefn.TABLE_TYPE,
                      null,
                      null
                  ),
                  new TableSpec(
                      DatasourceDefn.TABLE_TYPE,
                      ImmutableMap.of(),
                      ImmutableList.of(
                          new ColumnSpec("__time", Columns.TIME_COLUMN, null)
                      )
                  ),
                  MAPPER
              )),
              DatasourceTable.EffectiveMetadata.toEffectiveColumns(
                  RowSignature.builder()
                      .addTimeColumn()
                      .build()),
              false
          )
      ),
      "strictTableWithNoDefinedSchema", new DatasourceTable(
          RowSignature.builder().build(),
          new DatasourceTable.PhysicalDatasourceMetadata(
              new TableDataSource("strictTableWithNoDefinedSchema"),
              RowSignature.builder().build(),
              false,
              false
          ),
          new DatasourceTable.EffectiveMetadata(
              new DatasourceFacade(new ResolvedTable(
                  new TableDefn(
                      "strictTableWithNoDefinedSchema",
                      DatasourceDefn.TABLE_TYPE,
                      null,
                      null
                  ),
                  new TableSpec(DatasourceDefn.TABLE_TYPE, ImmutableMap.of(DatasourceDefn.SEALED_PROPERTY, true), null),
                  MAPPER
              )),
              DatasourceTable.EffectiveMetadata.toEffectiveColumns(RowSignature.builder().build()),
              false
          )
      ),
      "foo", new DatasourceTable(
          FOO_TABLE_SIGNATURE,
          new DatasourceTable.PhysicalDatasourceMetadata(
              new TableDataSource("foo"),
              FOO_TABLE_SIGNATURE,
              false,
              false
          ),
          new DatasourceTable.EffectiveMetadata(
              new DatasourceFacade(new ResolvedTable(
                  new TableDefn(
                      "foo",
                      DatasourceDefn.TABLE_TYPE,
                      null,
                      null
                  ),
                  new TableSpec(
                      DatasourceDefn.TABLE_TYPE,
                      ImmutableMap.of(),
                      ImmutableList.of(
                          new ColumnSpec("__time", Columns.TIME_COLUMN, null),
                          new ColumnSpec("dim1", Columns.STRING, null),
                          new ColumnSpec("dim2", Columns.STRING, null),
                          new ColumnSpec("dim3", Columns.STRING, null),
                          new ColumnSpec("cnt", Columns.LONG, null),
                          new ColumnSpec("m1", Columns.FLOAT, null),
                          new ColumnSpec("m2", Columns.DOUBLE, null)
                      )
                  ),
                  MAPPER
              )),
              DatasourceTable.EffectiveMetadata.toEffectiveColumns(FOO_TABLE_SIGNATURE),
              false
          )
      ),
      "fooSealed", new DatasourceTable(
          FOO_TABLE_SIGNATURE,
          new DatasourceTable.PhysicalDatasourceMetadata(
              new TableDataSource("foo"),
              FOO_TABLE_SIGNATURE,
              false,
              false
          ),
          new DatasourceTable.EffectiveMetadata(
              new DatasourceFacade(new ResolvedTable(
                  new TableDefn(
                      "foo",
                      DatasourceDefn.TABLE_TYPE,
                      null,
                      null
                  ),
                  new TableSpec(
                      DatasourceDefn.TABLE_TYPE,
                      ImmutableMap.of(DatasourceDefn.SEALED_PROPERTY, true),
                      ImmutableList.of(
                          new ColumnSpec("__time", Columns.TIME_COLUMN, null),
                          new ColumnSpec("dim1", Columns.STRING, null),
                          new ColumnSpec("dim2", Columns.STRING, null),
                          new ColumnSpec("dim3", Columns.STRING, null),
                          new ColumnSpec("cnt", Columns.LONG, null),
                          new ColumnSpec("m1", Columns.FLOAT, null),
                          new ColumnSpec("m2", Columns.DOUBLE, null)
                      )
                  ),
                  MAPPER
              )),
              DatasourceTable.EffectiveMetadata.toEffectiveColumns(FOO_TABLE_SIGNATURE),
              false
          )
      )
  );

  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Override
  public CatalogResolver createCatalogResolver()
  {
    return new CatalogResolver.NullCatalogResolver() {
      @Override
      public DruidTable resolveDatasource(
          final String tableName,
          final DatasourceTable.PhysicalDatasourceMetadata dsMetadata
      )
      {
        if (resolvedTables.get(tableName) != null) {
          return resolvedTables.get(tableName);
        }
        return dsMetadata == null ? null : new DatasourceTable(dsMetadata);
      }
    };
  }
}
