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

package org.apache.druid.jdbc.sql;

import org.apache.druid.jdbc.BooleanUtils;
import org.apache.druid.jdbc.DruidJdbcException;
import org.apache.druid.jdbc.StringUtils;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lightweight SQL scanner that splits statements on semicolons, picks out {@code SET} statements, and counts
 * parameter placeholders ({@code ?}).
 */
public class SqlScanner
{
  private static final Pattern PATTERN_SET_STATEMENT = Pattern.compile(
      "^\\s*SET\\s+(\\w+)\\s*=\\s*(.*?)\\s*$|^\\s*SET\\b.*",
      Pattern.DOTALL | Pattern.CASE_INSENSITIVE
  );
  private final List<SetStatement> setStatements;
  @Nullable
  private final String sqlStatement;
  private final int sqlStatementParameterCount;

  private SqlScanner(
      final List<SetStatement> setStatements,
      @Nullable final String sqlStatement,
      final int sqlStatementParameterCount
  )
  {
    this.setStatements = setStatements;
    this.sqlStatement = sqlStatement;
    this.sqlStatementParameterCount = sqlStatementParameterCount;
  }

  /**
   * Scans some SQL, which may potentially be multi-statement.
   */
  public static SqlScanner scan(final String sql) throws SQLException
  {
    if (sql == null || sql.isEmpty()) {
      return new SqlScanner(List.of(), null, 0);
    }

    final List<ScannedStatement> statements = new ArrayList<>();
    final StringBuilder current = new StringBuilder();
    int currentStart = 0;
    int parameterCount = 0;

    boolean inStringLiteral = false;
    boolean inDoubleQuotedIdentifier = false;
    boolean inLineComment = false;
    boolean inBlockComment = false;

    for (int i = 0; i < sql.length(); i++) {
      final char c = sql.charAt(i);
      final char next = (i + 1 < sql.length()) ? sql.charAt(i + 1) : '\0';

      if (inStringLiteral) {
        current.append(c);
        if (c == '\'' && next == '\'') {
          // Escaped single quote
          current.append(next);
          i++;
        } else if (c == '\'') {
          inStringLiteral = false;
        }
      } else if (inDoubleQuotedIdentifier) {
        current.append(c);
        if (c == '"' && next == '"') {
          // Escaped double quote
          current.append(next);
          i++;
        } else if (c == '"') {
          inDoubleQuotedIdentifier = false;
        }
      } else if (inLineComment) {
        if (c == '\n' || c == '\r') {
          inLineComment = false;
          current.append(c);
        }
        // else skip
      } else if (inBlockComment) {
        if (c == '*' && next == '/') {
          inBlockComment = false;
          i++;
          // Leave a space behind, so the tokens on either side of the comment stay separate.
          current.append(' ');
        }
        // else skip
      } else {
        // Normal code
        if (c == '\'') {
          inStringLiteral = true;
          current.append(c);
        } else if (c == '"') {
          inDoubleQuotedIdentifier = true;
          current.append(c);
        } else if ((c == '-' && next == '-') || (c == '/' && next == '/')) {
          inLineComment = true;
          i++;
        } else if (c == '/' && next == '*') {
          inBlockComment = true;
          i++;
        } else if (c == ';') {
          statements.add(new ScannedStatement(sql.substring(currentStart, i), current.toString()));
          current.setLength(0);
          currentStart = i + 1;
        } else if (c == '?') {
          parameterCount++;
          current.append(c);
        } else {
          current.append(c);
        }
      }
    }

    // Reject SQL that ends with an unterminated string literal, identifier, or block comment.
    if (inStringLiteral) {
      throw new DruidJdbcException("Unterminated string literal");
    } else if (inDoubleQuotedIdentifier) {
      throw new DruidJdbcException("Unterminated quoted identifier");
    } else if (inBlockComment) {
      throw new DruidJdbcException("Unterminated block comment");
    }

    // Add trailing statement if non-empty.
    if (!current.isEmpty()) {
      statements.add(new ScannedStatement(sql.substring(currentStart), current.toString()));
    }

    // Split statements into setStatements and sqlStatement.
    final List<SetStatement> setStatements = new ArrayList<>();
    String sqlStatement = null;

    for (final ScannedStatement statement : statements) {
      if (statement.withoutComments().isBlank()) {
        continue;
      }

      if (sqlStatement != null) {
        throw new DruidJdbcException("Cannot execute more than one regular (non-SET) statement");
      }

      final Matcher m = PATTERN_SET_STATEMENT.matcher(statement.withoutComments());
      if (!m.matches()) {
        sqlStatement = statement.text();
      } else if (m.group(1) == null) {
        throw new DruidJdbcException("SET syntax invalid: %s", statement.withoutComments().trim());
      } else {
        setStatements.add(new SetStatement(m.group(1), parseSetValue(m.group(2))));
      }
    }

    return new SqlScanner(setStatements, sqlStatement, parameterCount);
  }

  /**
   * Returns the {@code SET} statements found in the SQL, in the order they appeared.
   */
  public List<SetStatement> getSetStatements()
  {
    return setStatements;
  }

  /**
   * Returns a non-SET statement that was found, as written. There can be at most one. Returns null if there is none.
   */
  @Nullable
  public String getSqlStatement()
  {
    return sqlStatement;
  }

  /**
   * Returns the number of {@code ?} parameter placeholders in the {@link #getSqlStatement()}.
   */
  public int getParameterCount()
  {
    return sqlStatementParameterCount;
  }

  @Nullable
  private static Object parseSetValue(final String valueStr) throws SQLException
  {
    if (valueStr.isEmpty()) {
      throw new DruidJdbcException("SET value missing");
    }

    // Handle SQL string literals (single quotes only).
    if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
      if (valueStr.length() < 2) {
        throw new DruidJdbcException("SET value invalid: %s", valueStr);
      }
      // Handle escaped single quotes within the string
      final String content = valueStr.substring(1, valueStr.length() - 1);
      return StringUtils.replace(content, "''", "'");
    }

    // Handle NULL literal.
    if ("null".equalsIgnoreCase(valueStr) || "unknown".equalsIgnoreCase(valueStr)) {
      return null;
    }

    // Handle plain booleans.
    if (BooleanUtils.isBooleanTrue(valueStr)) {
      return Boolean.TRUE;
    }

    if (BooleanUtils.isBooleanFalse(valueStr)) {
      return Boolean.FALSE;
    }

    // Handle plain longs.
    try {
      return Long.parseLong(valueStr);
    }
    catch (NumberFormatException e) {
      // Continue.
    }

    // Handle plain doubles.
    try {
      return Double.parseDouble(valueStr);
    }
    catch (NumberFormatException e) {
      // Continue.
    }

    throw new DruidJdbcException("SET value invalid: %s", valueStr);
  }

  /**
   * One statement found by {@link #scan}. {@link #text} is the statement as written, which is what we send to Druid.
   * {@link #withoutComments} has comments elided, and is only used to recognize {@code SET} statements, which lets
   * comments appear anywhere within one.
   */
  private record ScannedStatement(String text, String withoutComments)
  {
  }
}
