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

package org.apache.druid.testing.embedded.catalog;

import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that expect failures when ingestng data into catalog defined tables.
 */
public class CatalogIngestErrorTest extends CatalogTestBase
{
  private TestCatalogClient client;

  @BeforeAll
  public void initializeClient()
  {
    client = new TestCatalogClient(cluster);
  }

  /**
   * If the segment grain is absent in the catalog and absent in the PARTITIONED BY clause in the query, then
   * validation error.
   */
  @Test
  public void testInsertNoPartitonedByFromCatalogOrQuery()
  {
    TableMetadata table = new TableBuilder(TableId.datasource(dataSource), DatasourceDefn.TABLE_TYPE)
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("varchar_col", "VARCHAR")
        .column("bigint_col", "BIGINT")
        .column("float_col", "FLOAT")
        .build();

    client.createTable(table, true);
    String queryInline =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(a) AS __time,\n"
            + "  b AS varchar_col,\n"
            + "  c AS bigint_col,\n"
            + "  e AS float_col\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,\\\"20\\\",2.0,foo\"}',\n"
            + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\"]}'\n"
            + "  )\n"
            + ") "
            + "  EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e FLOAT, f VARCHAR)\n",
            dataSource
        );

    verifySubmitSqlTaskFailsWith400BadRequest(
        queryInline,
        "Operation [INSERT] requires a PARTITIONED BY to be explicitly defined, but none was found."
    );
  }

  /**
   * Adding a new column during ingestion that is not defined in a sealed table should fail with
   * proper validation error.
   */
  @Test
  public void testInsertNonDefinedColumnIntoSealedCatalogTable()
  {
    TableMetadata table = TableBuilder.datasource(dataSource, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("varchar_col", "VARCHAR")
        .column("bigint_col", "BIGINT")
        .column("float_col", "FLOAT")
        .property(DatasourceDefn.SEALED_PROPERTY, true)
        .build();

    client.createTable(table, true);
    String queryInline =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(a) AS __time,\n"
            + "  b AS varchar_col,\n"
            + "  c AS bigint_col,\n"
            + "  e AS float_col,\n"
            + "  c AS extra\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,\\\"20\\\",2.0,foo\"}',\n"
            + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\"]}'\n"
            + "  )\n"
            + ") "
            + "  EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e FLOAT, f VARCHAR)\n"
            + "PARTITIONED BY DAY\n",
            dataSource
        );

    final String expectedMessage = StringUtils.format(
        "Column [extra] is not defined in the target table [druid.%s] strict schema",
        dataSource
    );
    verifySubmitSqlTaskFailsWith400BadRequest(queryInline, expectedMessage);
  }

  /**
   * Assigning a column during ingestion, to an input type that is not compatible with the defined type of the
   * column, should result in a proper validation error.
   */
  @Test
  public void testInsertWithIncompatibleTypeAssignment()
  {
    TableMetadata table = TableBuilder.datasource(dataSource, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("varchar_col", "VARCHAR")
        .column("bigint_col", "BIGINT")
        .column("float_col", "FLOAT")
        .property(DatasourceDefn.SEALED_PROPERTY, true)
        .build();

    client.createTable(table, true);
    String queryInline =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(a) AS __time,\n"
            + "  ARRAY[b] AS varchar_col,\n"
            + "  c AS bigint_col,\n"
            + "  e AS float_col\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,\\\"20\\\",2.0,foo\"}',\n"
            + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\"]}'\n"
            + "  )\n"
            + ") "
            + "  EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e FLOAT, f VARCHAR)\n"
            + "PARTITIONED BY DAY\n",
            dataSource
        );

    verifySubmitSqlTaskFailsWith400BadRequest(
        queryInline,
        "Cannot assign to target field 'varchar_col' of type VARCHAR from source"
        + " field 'varchar_col' of type VARCHAR ARRAY (line [4], column [3])"
    );
  }

  /**
   * Assigning a complex type column during ingestion, to an input type that is not compatible with the defined type of
   * the column, should result in a proper validation error.
   */
  @Test
  public void testInsertGroupByWithIncompatibleTypeAssignment()
  {
    TableMetadata table = TableBuilder.datasource(dataSource, "P1D")
        .column(Columns.TIME_COLUMN, Columns.LONG)
        .column("varchar_col", "VARCHAR")
        .column("bigint_col", "BIGINT")
        .column("float_col", "FLOAT")
        .column("hll_col", "COMPLEX<hyperUnique>")
        .property(DatasourceDefn.SEALED_PROPERTY, true)
        .build();

    client.createTable(table, true);
    String queryInline =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(a) AS __time,\n"
            + "  b AS varchar_col,\n"
            + "  c AS bigint_col,\n"
            + "  e AS float_col,\n"
            + "  ARRAY[b] AS hll_col\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"inline\",\"data\":\"2022-12-26T12:34:56,extra,10,\\\"20\\\",2.0,foo\"}',\n"
            + "    '{\"type\":\"csv\",\"findColumnsFromHeader\":false,\"columns\":[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\"]}'\n"
            + "  )\n"
            + ") "
            + "  EXTEND (a VARCHAR, b VARCHAR, c BIGINT, d VARCHAR, e FLOAT, f VARCHAR)\n"
            + "PARTITIONED BY DAY\n",
            dataSource
        );

    verifySubmitSqlTaskFailsWith400BadRequest(
        queryInline,
        "Cannot assign to target field 'hll_col' of type COMPLEX<hyperUnique>"
        + " from source field 'hll_col' of type VARCHAR ARRAY (line [7], column [3])"
    );
  }
}
