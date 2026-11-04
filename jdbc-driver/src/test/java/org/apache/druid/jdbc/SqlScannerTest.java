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

package org.apache.druid.jdbc;

import org.apache.druid.jdbc.sql.SetStatement;
import org.apache.druid.jdbc.sql.SqlScanner;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;


public class SqlScannerTest
{
  @Test
  public void testNullOrEmptyInputYieldsNoStatement() throws SQLException
  {
    for (final String sql : new String[]{null, ""}) {
      final SqlScanner result = SqlScanner.scan(sql);
      Assertions.assertEquals(0, result.getParameterCount());
      Assertions.assertTrue(result.getSetStatements().isEmpty());
      Assertions.assertNull(result.getSqlStatement());
    }
  }

  /**
   * Only a bare ? outside a string literal, quoted identifier, or comment is a parameter placeholder.
   */
  @Test
  public void testParameterCount() throws SQLException
  {
    assertParameterCount("SELECT 1", 0);
    assertParameterCount("SELECT * FROM t WHERE x = ? AND y = ?", 2);
    // A ? inside a string literal is not a parameter.
    assertParameterCount("SELECT 'are you sure?'", 0);
    assertParameterCount("SELECT * FROM t WHERE s = 'what?' AND x = ?", 1);
    assertParameterCount("SELECT * FROM t WHERE s = 'a?b?c' AND x = ? AND y = ?", 2);
    assertParameterCount("SELECT * FROM t WHERE s = 'it''s a question?' AND x = ?", 1);
    assertParameterCount("SELECT * FROM t WHERE s = 'say \"hello?\"' AND x = ?", 1);
    // A ? inside a quoted identifier is not a parameter.
    assertParameterCount("SELECT \"column?\" FROM t WHERE x = ?", 1);
    assertParameterCount("SELECT \"col\"\"name?\" FROM t WHERE x = ?", 1);
    // A ? inside a comment is not a parameter.
    assertParameterCount("SELECT * FROM t -- is this a param?\nWHERE x = ?", 1);
    assertParameterCount("SELECT * FROM t // is this a param?\nWHERE x = ?", 1);
    assertParameterCount("SELECT * FROM t /* param? */ WHERE x = ?", 1);
    assertParameterCount("SELECT * FROM t WHERE s = 'what?' /* why? */ AND x = ? -- really?\n AND y = ?", 2);
    // Prefixed literal forms: unicode, national character, and hex.
    assertParameterCount("SELECT * FROM t WHERE s = U&'what?' AND x = ?", 1);
    assertParameterCount("SELECT * FROM t WHERE s = u&'what?' AND x = ?", 1);
    assertParameterCount("SELECT * FROM t WHERE s = U&'it''s a question?' AND x = ?", 1);
    // The UESCAPE clause has its own string literal.
    assertParameterCount("SELECT * FROM t WHERE s = U&'what?' UESCAPE '?' AND x = ?", 1);
    assertParameterCount("SELECT * FROM t WHERE s = N'what?' AND x = ?", 1);
    assertParameterCount("SELECT * FROM t WHERE s = X'3F' AND x = ?", 1);
  }

  @Test
  public void testPlannerHintPreserved() throws SQLException
  {
    final String sql = "SELECT /*+ sort_merge */ a.x FROM a JOIN b ON a.x = b.y";
    Assertions.assertEquals(sql, SqlScanner.scan(sql).getSqlStatement());
  }

  @Test
  public void testCommentOnlyStatementIgnored() throws SQLException
  {
    Assertions.assertNull(SqlScanner.scan("/* nothing to see here */").getSqlStatement());
    Assertions.assertNull(SqlScanner.scan("// nothing to see here").getSqlStatement());
    Assertions.assertEquals("SELECT 1", SqlScanner.scan("/* lead */;SELECT 1").getSqlStatement());
  }

  @Test
  public void testSlashSlashLineComment() throws SQLException
  {
    // Semicolons and apostrophes inside a "//" comment are inert.
    final String sql = "SELECT 1 // note; don't ask";
    Assertions.assertEquals(sql, SqlScanner.scan(sql).getSqlStatement());

    // A lone "/" does not start a comment.
    Assertions.assertEquals("SELECT 4 / 2", SqlScanner.scan("SELECT 4 / 2").getSqlStatement());
  }

  @Test
  public void testUnterminatedInputRejected()
  {
    assertScanFails("SELECT * FROM t WHERE x = 1 /* AND y = 2", "Unterminated block comment");
    assertScanFails("SELECT * FROM t WHERE s = 'abc", "Unterminated string literal");
    assertScanFails("SELECT \"col FROM t", "Unterminated quoted identifier");
  }

  @Test
  public void testSemicolonSplitsStatements() throws SQLException
  {
    final SqlScanner result = SqlScanner.scan("SET x = 1; SET y = 2");
    Assertions.assertNull(result.getSqlStatement());

    final List<SetStatement> setStatements = result.getSetStatements();
    Assertions.assertEquals(2, setStatements.size());
    Assertions.assertEquals(new SetStatement("x", 1L), setStatements.get(0));
    Assertions.assertEquals(new SetStatement("y", 2L), setStatements.get(1));
  }

  @Test
  public void testTrailingSemicolon() throws SQLException
  {
    final SqlScanner result = SqlScanner.scan("SELECT 1;");
    Assertions.assertEquals("SELECT 1", result.getSqlStatement());
  }

  /**
   * String literals and quoted identifiers reach the server verbatim, escaped quotes and semicolons included.
   */
  @Test
  public void testQuotedTextPreservedInOutput() throws SQLException
  {
    final String[] statements = {
        "SELECT * FROM t WHERE s = 'hello'",
        "SELECT \"column?\" FROM t WHERE x = ?",
        "SELECT \"col\"\"name?\" FROM t WHERE x = ?",
        "SELECT * FROM t WHERE s = 'it''s a question?' AND x = ?",
        "SELECT * FROM t WHERE s = 'a;b'"
    };

    for (final String sql : statements) {
      Assertions.assertEquals(sql, SqlScanner.scan(sql).getSqlStatement());
    }
  }

  private static void assertParameterCount(final String sql, final int expectedCount) throws SQLException
  {
    Assertions.assertEquals(expectedCount, SqlScanner.scan(sql).getParameterCount(), sql);
  }

  private static void assertScanFails(final String sql, final String expectedMessage)
  {
    final SQLException e = Assertions.assertThrows(SQLException.class, () -> SqlScanner.scan(sql), sql);
    Assertions.assertEquals(expectedMessage, e.getMessage());
  }
}
