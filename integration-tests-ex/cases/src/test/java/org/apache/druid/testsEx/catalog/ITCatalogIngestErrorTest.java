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

package org.apache.druid.testsEx.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testsEx.categories.Catalog;
import org.apache.druid.testsEx.cluster.CatalogClient;
import org.apache.druid.testsEx.cluster.DruidClusterClient;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

/**
 * Tests that expect failures when ingestng data into catalog defined tables.
 */
@RunWith(DruidTestRunner.class)
@Category(Catalog.class)
public class ITCatalogIngestErrorTest
{
  @Inject
  private MsqTestQueryHelper msqHelper;
  @Inject
  private DruidClusterClient clusterClient;
  private CatalogClient client;

  @Before
  public void initializeClient()
  {
    client = new CatalogClient(clusterClient);
  }

  /**
   * If the segment grain is absent in the catalog and absent in the PARTITIONED BY clause in the query, then
   * validation error.
   */
  @Test
  public void testInsertNoPartitonedByFromCatalogOrQuery() throws ExecutionException, InterruptedException
  {
    String tableName = "testInsertNoPartitonedByFromCatalogOrQuery";
    TableMetadata table = new TableBuilder(TableId.datasource(tableName), DatasourceDefn.TABLE_TYPE)
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
            tableName
        );

    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskWithExpectedStatusCode(sqlQueryFromString(queryInline), null, null, HttpResponseStatus.BAD_REQUEST);
    assertTrue(sqlTaskStatus.getError() != null && sqlTaskStatus.getError()
        .getUnderlyingException()
        .getMessage()
        .equals(
            "Operation [INSERT] requires a PARTITIONED BY to be explicitly defined, but none was found.")
    );
  }

  /**
   * Adding a new column during ingestion that is not defined in a sealed table should fail with
   * proper validation error.
   */
  @Test
  public void testInsertNonDefinedColumnIntoSealedCatalogTable() throws ExecutionException, InterruptedException
  {
    String tableName = "testInsertNonDefinedColumnIntoSealedCatalogTable";
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
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
            tableName
        );

    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskWithExpectedStatusCode(sqlQueryFromString(queryInline), null, null, HttpResponseStatus.BAD_REQUEST);
    assertTrue(sqlTaskStatus.getError() != null && sqlTaskStatus.getError()
        .getUnderlyingException()
        .getMessage()
        .equals(
            "Column [extra] is not defined in the target table [druid.testInsertNonDefinedColumnIntoSealedCatalogTable] strict schema")
    );
  }

  /**
   * Assigning a column during ingestion, to an input type that is not compatible with the defined type of the
   * column, should result in a proper validation error.
   */
  @Test
  public void testInsertWithIncompatibleTypeAssignment() throws ExecutionException, InterruptedException
  {
    String tableName = "testInsertWithIncompatibleTypeAssignment";
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
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
            tableName
        );

    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskWithExpectedStatusCode(sqlQueryFromString(queryInline), null, null, HttpResponseStatus.BAD_REQUEST);
    assertTrue(sqlTaskStatus.getError() != null && sqlTaskStatus.getError()
        .getUnderlyingException()
        .getMessage()
        .equals(
            "Cannot assign to target field 'varchar_col' of type VARCHAR from source field 'varchar_col' of type VARCHAR ARRAY (line [4], column [3])")
    );
  }

  /**
   * Assigning a complex type column during ingestion, to an input type that is not compatible with the defined type of
   * the column, should result in a proper validation error.
   */
  @Test
  public void testInsertGroupByWithIncompatibleTypeAssignment() throws ExecutionException, InterruptedException
  {
    String tableName = "testInsertGroupByWithIncompatibleTypeAssignment";
    TableMetadata table = TableBuilder.datasource(tableName, "P1D")
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
            tableName
        );

    SqlTaskStatus sqlTaskStatus = msqHelper.submitMsqTaskWithExpectedStatusCode(sqlQueryFromString(queryInline), null, null, HttpResponseStatus.BAD_REQUEST);
    assertTrue(sqlTaskStatus.getError() != null && sqlTaskStatus.getError()
        .getUnderlyingException()
        .getMessage()
        .equals(
            "Cannot assign to target field 'hll_col' of type COMPLEX<hyperUnique> from source field 'hll_col' of type VARCHAR ARRAY (line [7], column [3])")
    );
  }

  private static SqlQuery sqlQueryFromString(String queryString)
  {
    return new SqlQuery(queryString, null, false, false, false, ImmutableMap.of(), null);
  }
}
