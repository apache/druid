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

package org.apache.druid.sql.calcite.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Parser coverage for the catalog DDL statements: {@code CREATE TABLE} and {@code ALTER TABLE}.
 */
public class DruidSqlDdlParserTest
{
  @Test
  public void testCreateTableMinimal()
  {
    final DruidSqlCreateTable create = parseCreate("CREATE TABLE tbl (a VARCHAR, b BIGINT)");

    assertEquals("tbl", create.getName().toString());
    assertFalse(create.getReplace());
    assertFalse(create.isIfNotExists());
    assertNull(create.getPartitionedBy());
    assertNull(create.getClusteredBy());
    assertEquals("a VARCHAR, b BIGINT", columnsOf(create));
  }

  @Test
  public void testCreateTableWithSchemaQualifiedName()
  {
    final DruidSqlCreateTable create = parseCreate("CREATE TABLE \"druid\".tbl (a VARCHAR)");
    assertEquals("druid.tbl", create.getName().toString());
  }

  @Test
  public void testCreateTableWithoutColumns()
  {
    // A catalog entry that only carries properties is legal.
    final DruidSqlCreateTable create = parseCreate("CREATE TABLE tbl PARTITIONED BY DAY");
    assertEquals(0, create.getColumnList().size());
    assertEquals(Granularities.DAY, create.getPartitionedBy().getGranularity());
  }

  @Test
  public void testCreateTableAllClauses()
  {
    final DruidSqlCreateTable create = parseCreate(
        "CREATE OR REPLACE TABLE \"druid\".sales (\n"
        + "  __time TIMESTAMP,\n"
        + "  page VARCHAR NOT NULL,\n"
        + "  cnt BIGINT,\n"
        + "  vals DOUBLE ARRAY,\n"
        + "  usr TYPE('COMPLEX<hyperUnique>')\n"
        + ")\n"
        + "PARTITIONED BY HOUR\n"
        + "CLUSTERED BY page, cnt"
    );

    assertTrue(create.getReplace());
    assertFalse(create.isIfNotExists());
    // Calcite renders a user-defined type name as a quoted identifier; TYPE('...') round-trips correctly through
    // unparse, which is what actually matters (see testUnparseRoundTrip).
    assertEquals(
        "__time TIMESTAMP, page VARCHAR, cnt BIGINT, vals DOUBLE ARRAY, usr `COMPLEX<hyperUnique>`",
        columnsOf(create)
    );
    assertEquals(Granularities.HOUR, create.getPartitionedBy().getGranularity());
    assertEquals("`page`, `cnt`", create.getClusteredBy().toString());
  }

  @Test
  public void testCreateTableIfNotExists()
  {
    final DruidSqlCreateTable create = parseCreate("CREATE TABLE IF NOT EXISTS tbl (a VARCHAR)");
    assertTrue(create.isIfNotExists());
    assertFalse(create.getReplace());
  }

  @Test
  public void testCreateTableExpressionGranularity()
  {
    final DruidSqlCreateTable create = parseCreate("CREATE TABLE tbl (a VARCHAR) PARTITIONED BY FLOOR(__time TO HOUR)");
    assertEquals(Granularities.HOUR, create.getPartitionedBy().getGranularity());
  }

  @Test
  public void testAlterTableAddColumn()
  {
    final DruidSqlAlterTable.AddColumn alter = parseAlter(
        "ALTER TABLE tbl ADD COLUMN added DOUBLE",
        DruidSqlAlterTable.AddColumn.class
    );
    assertEquals("tbl", alter.getName().toString());
    assertEquals("added", alter.getColumn().getName().toString());
    assertEquals("DOUBLE", alter.getColumn().getDataType().toString());
  }

  @Test
  public void testAlterTableDropColumn()
  {
    final DruidSqlAlterTable.DropColumn alter = parseAlter(
        "ALTER TABLE tbl DROP COLUMN gone",
        DruidSqlAlterTable.DropColumn.class
    );
    assertEquals("gone", alter.getColumn().toString());
  }

  @Test
  public void testAlterTableAlterColumn()
  {
    final DruidSqlAlterTable.AlterColumn alter = parseAlter(
        "ALTER TABLE tbl ALTER COLUMN cnt SET DATA TYPE DOUBLE",
        DruidSqlAlterTable.AlterColumn.class
    );
    assertEquals("cnt", alter.getColumn().getName().toString());
    assertEquals("DOUBLE", alter.getColumn().getDataType().toString());
  }

  @Test
  public void testAlterTableAlterColumnToComplexType()
  {
    final DruidSqlAlterTable.AlterColumn alter = parseAlter(
        "ALTER TABLE tbl ALTER COLUMN payload SET DATA TYPE TYPE('COMPLEX<json>')",
        DruidSqlAlterTable.AlterColumn.class
    );
    assertEquals("COMPLEX<json>", alter.getColumn().getDataType().getTypeName().toString());
  }

  @Test
  public void testAlterTableSetProperties()
  {
    final DruidSqlAlterTable.SetProperties alter = parseAlter(
        "ALTER TABLE tbl SET PROPERTIES (targetSegmentRows = 3000000, sealed = TRUE, description = NULL)",
        DruidSqlAlterTable.SetProperties.class
    );
    assertEquals(3, alter.getProperties().size());
    assertEquals(
        "targetSegmentRows = 3000000, sealed = TRUE, description = NULL",
        alter.getProperties()
             .stream()
             .map(p -> {
               final DruidSqlPropertyAssignment assignment = (DruidSqlPropertyAssignment) p;
               return assignment.getKey() + " = " + assignment.getValue();
             })
             .collect(Collectors.joining(", "))
    );
  }

  @Test
  public void testCreateTableWithProjection()
  {
    final DruidSqlCreateTable create = parseCreate(
        "CREATE TABLE events (\n"
        + "  __time TIMESTAMP,\n"
        + "  user_id VARCHAR,\n"
        + "  pages_visited BIGINT,\n"
        + "  PROJECTION daily_visits AS (\n"
        + "    SELECT TIME_FLOOR(__time, 'P1D'), user_id, SUM(pages_visited) AS total\n"
        + "    WHERE user_id IS NOT NULL\n"
        + "    GROUP BY 1, 2\n"
        + "  )\n"
        + ")"
    );

    assertEquals("__time TIMESTAMP, user_id VARCHAR, pages_visited BIGINT", columnsOf(create));
    assertEquals(1, create.getProjectionList().size());

    final SqlProjectionSpec projection = (SqlProjectionSpec) create.getProjectionList().get(0);
    assertEquals("daily_visits", projection.getName().toString());
    assertNull(projection.getBody().getFrom(), "projection body must have no FROM clause");
    assertEquals(3, projection.getBody().getSelectList().size());
    assertNotNull(projection.getBody().getWhere());
    assertEquals(2, projection.getBody().getGroup().size());
  }

  @Test
  public void testCreateTableProjectionWithoutAs()
  {
    // ClickHouse spells this without AS; both are accepted.
    final DruidSqlCreateTable create = parseCreate(
        "CREATE TABLE t (a VARCHAR, PROJECTION p (SELECT a, COUNT(*) AS c GROUP BY a))"
    );
    assertEquals(1, create.getProjectionList().size());
    assertEquals("a VARCHAR", columnsOf(create));
  }

  @Test
  public void testCreateTableMultipleProjections()
  {
    final DruidSqlCreateTable create = parseCreate(
        "CREATE TABLE t (a VARCHAR, b BIGINT,"
        + " PROJECTION p1 AS (SELECT a, SUM(b) AS s GROUP BY a),"
        + " PROJECTION p2 AS (SELECT b, COUNT(*) AS c GROUP BY b))"
    );
    assertEquals(2, create.getProjectionList().size());
    assertEquals("a VARCHAR, b BIGINT", columnsOf(create));
  }

  /**
   * A column may be named "projection": the keyword is non-reserved, and a projection definition is told apart by
   * its third token, which is always '(' or AS.
   */
  @Test
  public void testColumnNamedProjection()
  {
    assertEquals("projection VARCHAR", columnsOf(parseCreate("CREATE TABLE t (projection VARCHAR)")));
    // A bare-identifier type is the case two tokens of lookahead could not resolve. Calcite renders such a type as
    // a quoted identifier.
    assertEquals("projection `LONG`", columnsOf(parseCreate("CREATE TABLE t (projection LONG)")));
    assertEquals(
        "a VARCHAR, projection `LONG`",
        columnsOf(parseCreate("CREATE TABLE t (a VARCHAR, projection LONG)"))
    );

    final DruidSqlCreateTable both = parseCreate(
        "CREATE TABLE t (projection LONG, PROJECTION projection AS (SELECT projection GROUP BY projection))"
    );
    assertEquals("projection `LONG`", columnsOf(both));
    assertEquals(1, both.getProjectionList().size());
  }

  @Test
  public void testAlterTableAddProjection()
  {
    final DruidSqlAlterTable.AddProjection alter = parseAlter(
        "ALTER TABLE t ADD PROJECTION p AS (SELECT a, SUM(b) AS s GROUP BY a)",
        DruidSqlAlterTable.AddProjection.class
    );
    assertEquals("t", alter.getName().toString());
    assertEquals("p", alter.getProjection().getName().toString());
    assertFalse(alter.isIfNotExists());
  }

  @Test
  public void testAlterTableAddProjectionIfNotExists()
  {
    final DruidSqlAlterTable.AddProjection alter = parseAlter(
        "ALTER TABLE t ADD IF NOT EXISTS PROJECTION p AS (SELECT a GROUP BY a)",
        DruidSqlAlterTable.AddProjection.class
    );
    assertTrue(alter.isIfNotExists());
  }

  @Test
  public void testAlterTableDropProjection()
  {
    final DruidSqlAlterTable.DropProjection alter = parseAlter(
        "ALTER TABLE t DROP PROJECTION p",
        DruidSqlAlterTable.DropProjection.class
    );
    assertEquals("p", alter.getProjectionName().toString());
    assertFalse(alter.isIfExists());

    assertTrue(
        parseAlter("ALTER TABLE t DROP PROJECTION IF EXISTS p", DruidSqlAlterTable.DropProjection.class)
            .isIfExists()
    );
  }

  /**
   * A projection has no way to express ordering or limits, so the grammar excludes them rather than validating them
   * away later.
   */
  @Test
  public void testProjectionBodyRejectsUnsupportedClauses()
  {
    // Each entry pairs an illegal body with the token the parser's error must name: the grammar produces the
    // rejection, so naming the offending clause is all the user gets to locate the problem.
    final String[][] cases = {
        {"SELECT a GROUP BY a ORDER BY a", "ORDER"},
        {"SELECT a GROUP BY a LIMIT 10", "LIMIT"},
        {"SELECT a GROUP BY a HAVING COUNT(*) > 1", "HAVING"},
        {"SELECT a FROM other GROUP BY a", "FROM"},
        {"SELECT a GROUP BY a UNION ALL SELECT b GROUP BY b", "UNION"},
    };
    for (String[] c : cases) {
      final DruidException e = assertThrows(
          DruidException.class,
          () -> parse("CREATE TABLE t (a VARCHAR, PROJECTION p AS (" + c[0] + "))"),
          c[0]
      );
      assertTrue(e.getMessage().contains(c[1]), c[0] + " -> " + e.getMessage());
    }
  }

  @Test
  public void testCreateTableWithBaseProjectionAndSealed()
  {
    final DruidSqlCreateTable create = parseCreate(
        "CREATE TABLE t SEALED (\n"
        + "  tenant VARCHAR,\n"
        + "  bucket BIGINT,\n"
        + "  __time TIMESTAMP,\n"
        + "  PROJECTION __base AS (\n"
        + "    SELECT tenant, ABS(user_id) AS bucket, __time\n"
        + "    CLUSTERED BY tenant, bucket\n"
        + "  )\n"
        + ") PARTITIONED BY DAY"
    );

    assertTrue(create.isSealed());
    assertEquals(1, create.getProjectionList().size());

    final SqlProjectionSpec base = (SqlProjectionSpec) create.getProjectionList().get(0);
    assertEquals("__base", base.getName().toString());
    assertEquals("`tenant`, `bucket`", base.getClusteredBy().toString());
    assertNull(base.getBody().getGroup());
  }

  @Test
  public void testSealedWithoutProjection()
  {
    assertTrue(parseCreate("CREATE TABLE t SEALED (a VARCHAR)").isSealed());
    assertFalse(parseCreate("CREATE TABLE t (a VARCHAR)").isSealed());
  }

  /**
   * SEALED binds to the column list, so a table declaring no columns cannot be sealed: the odd statement is
   * unparseable rather than accepted.
   */
  @Test
  public void testSealedWithoutColumnListIsRejected()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> parse("CREATE TABLE tbl SEALED PARTITIONED BY DAY")
    );
    assertTrue(e.getMessage().contains("PARTITIONED"), e.getMessage());
  }

  /**
   * SEALED is a non-reserved keyword, so it remains usable as an identifier.
   */
  @Test
  public void testSealedUsableAsIdentifier()
  {
    assertEquals("sealed VARCHAR", columnsOf(parseCreate("CREATE TABLE t (sealed VARCHAR)")));
    // A table named 'sealed' followed by the SEALED keyword: the identifier and the keyword coexist.
    assertTrue(parseCreate("CREATE TABLE sealed SEALED (a VARCHAR)").isSealed());
  }

  @Test
  public void testAlterTableAddBaseProjection()
  {
    final DruidSqlAlterTable.AddProjection alter = parseAlter(
        "ALTER TABLE t ADD PROJECTION __base AS (SELECT a, __time CLUSTERED BY a)",
        DruidSqlAlterTable.AddProjection.class
    );
    assertEquals("__base", alter.getProjection().getName().toString());
    assertEquals("`a`", alter.getProjection().getClusteredBy().toString());
  }

  /**
   * DDL nodes must round-trip through {@link SqlNode#unparse}, which is what makes them safe to log and re-print.
   */
  @Test
  public void testUnparseRoundTrip()
  {
    assertUnparseRoundTrips("CREATE TABLE \"tbl\" (\"a\" VARCHAR, \"b\" BIGINT)");
    // A table may be declared with no columns at all; the element list is omitted rather than printed empty, since
    // the grammar has no way to read "()" back.
    assertUnparseRoundTrips("CREATE TABLE \"tbl\" PARTITIONED BY DAY");
    assertUnparseRoundTrips("CREATE OR REPLACE TABLE \"tbl\" (\"a\" VARCHAR)");
    assertUnparseRoundTrips("CREATE TABLE IF NOT EXISTS \"tbl\" (\"a\" VARCHAR)");
    assertUnparseRoundTrips("CREATE TABLE \"tbl\" (\"a\" VARCHAR) PARTITIONED BY DAY");
    assertUnparseRoundTrips("CREATE TABLE \"tbl\" (\"a\" VARCHAR) PARTITIONED BY DAY CLUSTERED BY \"a\"");
    assertUnparseRoundTrips("CREATE TABLE \"tbl\" (\"p\" TYPE('COMPLEX<json>'))");
    assertUnparseRoundTrips("ALTER TABLE \"tbl\" ADD COLUMN \"a\" DOUBLE");
    assertUnparseRoundTrips("ALTER TABLE \"tbl\" DROP COLUMN \"a\"");
    assertUnparseRoundTrips("ALTER TABLE \"tbl\" ALTER COLUMN \"a\" SET DATA TYPE BIGINT");
    assertUnparseRoundTrips("ALTER TABLE \"tbl\" SET PROPERTIES (\"sealed\" = TRUE)");
    assertUnparseRoundTrips("CREATE TABLE \"tbl\" SEALED (\"a\" VARCHAR)");
    assertUnparseRoundTrips(
        "CREATE TABLE \"tbl\" (\"a\" VARCHAR, \"c\" BIGINT,"
        + " PROJECTION \"p\" AS (SELECT \"a\", SUM(\"c\") AS \"total\" GROUP BY \"a\"))"
    );
    assertUnparseRoundTrips(
        "CREATE TABLE \"tbl\" (\"a\" VARCHAR, \"__time\" TIMESTAMP,"
        + " PROJECTION \"__base\" AS (SELECT \"a\", \"__time\" CLUSTERED BY \"a\"))"
    );
    assertUnparseRoundTrips("ALTER TABLE \"tbl\" DROP PROJECTION \"p\"");
  }

  /**
   * Calcite clones a node by asking its operator to rebuild it from its operand list, which is how shuttles rewrite
   * a statement. The operand order is hand-written per node, so a round trip is what proves it is right.
   */
  @Test
  public void testCloneRoundTrip()
  {
    for (String sql : new String[]{
        "CREATE TABLE \"tbl\" (\"a\" VARCHAR, \"b\" BIGINT)",
        "CREATE OR REPLACE TABLE \"tbl\" (\"a\" VARCHAR)",
        "CREATE TABLE IF NOT EXISTS \"tbl\" (\"a\" VARCHAR)",
        "CREATE TABLE \"tbl\" (\"a\" VARCHAR) PARTITIONED BY DAY CLUSTERED BY \"a\"",
        "CREATE TABLE \"tbl\" SEALED (\"a\" VARCHAR)",
        "CREATE TABLE \"tbl\" (\"a\" VARCHAR, PROJECTION \"p\" AS (SELECT \"a\" GROUP BY \"a\"))",
        "CREATE TABLE \"tbl\" SEALED (\"a\" VARCHAR, PROJECTION \"__base\" AS (SELECT \"a\" CLUSTERED BY \"a\"))",
        "ALTER TABLE \"tbl\" ADD COLUMN \"a\" DOUBLE",
        "ALTER TABLE \"tbl\" DROP COLUMN \"a\"",
        "ALTER TABLE \"tbl\" ALTER COLUMN \"a\" SET DATA TYPE BIGINT",
        "ALTER TABLE \"tbl\" ADD PROJECTION \"p\" AS (SELECT \"a\" GROUP BY \"a\")",
        "ALTER TABLE \"tbl\" ADD IF NOT EXISTS PROJECTION \"p\" AS (SELECT \"a\" GROUP BY \"a\")",
        "ALTER TABLE \"tbl\" DROP PROJECTION \"p\"",
        "ALTER TABLE \"tbl\" DROP PROJECTION IF EXISTS \"p\"",
        "ALTER TABLE \"tbl\" SET PROPERTIES (\"sealed\" = TRUE)"
    }) {
      final SqlNode node = parse(sql);
      final SqlNode clone = node.clone(node.getParserPosition());
      assertEquals(
          node.toSqlString(CalciteSqlDialect.DEFAULT).getSql(),
          clone.toSqlString(CalciteSqlDialect.DEFAULT).getSql(),
          sql
      );
    }
  }

  @Test
  public void testDdlAfterSetStatement()
  {
    final SqlNode node = parse("SET sqlQueryId = 'abc'; CREATE TABLE tbl (a VARCHAR)");
    assertInstanceOf(DruidSqlCreateTable.class, node);
  }

  @Test
  public void testDdlWithTrailingSemicolon()
  {
    assertInstanceOf(DruidSqlCreateTable.class, parse("CREATE TABLE tbl (a VARCHAR);"));
    assertInstanceOf(DruidSqlAlterTable.AddColumn.class, parse("ALTER TABLE tbl ADD COLUMN a VARCHAR;"));
  }

  @Test
  public void testDdlBeforeAnotherStatementIsRejected()
  {
    final DruidException e = assertThrows(
        DruidException.class,
        () -> parse("CREATE TABLE tbl (a VARCHAR); SELECT 1")
    );
    assertTrue(e.getMessage().contains("Only SET statements can appear before the final statement"));
  }

  /**
   * {@code ALTER SYSTEM}/{@code ALTER SESSION} must keep working: {@code ALTER TABLE} is dispatched by a two-token
   * lookahead ahead of Calcite's stock {@code SqlAlter()} production.
   */
  @Test
  public void testAlterSystemStillParses() throws SqlParseException
  {
    // Parsed directly rather than through DruidSqlParser.parse, which folds SET options into the query context and
    // then requires a non-SET statement to execute.
    assertInstanceOf(SqlSetOption.class, parseStatementList("ALTER SYSTEM SET \"a\" = 1").get(0));
    assertInstanceOf(SqlSetOption.class, parseStatementList("ALTER SESSION SET \"a\" = 1").get(0));
  }

  /**
   * {@code IF} and {@code PROPERTIES} are added as non-reserved keywords, so they must remain usable as identifiers.
   */
  @Test
  public void testNewKeywordsRemainUsableAsIdentifiers()
  {
    final DruidSqlCreateTable create = parseCreate("CREATE TABLE properties (if VARCHAR, properties BIGINT)");
    assertEquals("properties", create.getName().toString());
    assertEquals("if VARCHAR, properties BIGINT", columnsOf(create));
  }

  @Test
  public void testExplainOfDdlIsRejected()
  {
    assertThrows(DruidException.class, () -> parse("EXPLAIN PLAN FOR CREATE TABLE tbl (a VARCHAR)"));
  }

  @Test
  public void testCreateTableWithoutTypeIsRejected()
  {
    assertThrows(DruidException.class, () -> parse("CREATE TABLE tbl (a)"));
  }

  @Test
  public void testAlterTableWithoutOperationIsRejected()
  {
    assertThrows(DruidException.class, () -> parse("ALTER TABLE tbl"));
  }

  @Test
  public void testDropTableIsNotSupported()
  {
    // DROP TABLE is deliberately unclaimed; it must not silently parse as something else.
    assertThrows(DruidException.class, () -> parse("DROP TABLE tbl"));
  }

  private static void assertUnparseRoundTrips(String sql)
  {
    final SqlNode node = parse(sql);
    assertEquals(sql, StringUtils.replace(node.toSqlString(CalciteSqlDialect.DEFAULT).getSql(), "\n", " "));
  }

  private static String columnsOf(DruidSqlCreateTable create)
  {
    return create.getColumnList()
                 .stream()
                 .map(c -> {
                   final DruidSqlColumnDeclaration column = (DruidSqlColumnDeclaration) c;
                   return column.getName() + " " + column.getDataType();
                 })
                 .collect(Collectors.joining(", "));
  }

  private static SqlNode parse(String sql)
  {
    return DruidSqlParser.parse(sql, true).getMainStatement();
  }

  private static SqlNodeList parseStatementList(String sql) throws SqlParseException
  {
    return (SqlNodeList) SqlParser.create(sql, DruidSqlParser.PARSER_CONFIG).parseStmtList();
  }

  private static DruidSqlCreateTable parseCreate(String sql)
  {
    return assertInstanceOf(DruidSqlCreateTable.class, parse(sql));
  }

  private static <T extends DruidSqlAlterTable> T parseAlter(String sql, Class<T> clazz)
  {
    return assertInstanceOf(clazz, parse(sql));
  }
}
