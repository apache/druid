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

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.StringReader;

public class DruidSqlParserImplTokenManagerTest
{
  @Test
  public void testBeforeTableNameActionsAllowHyphenatedIdentifier()
  {
    final String[] keywords = {
        "DELETE",
        "DESCRIBE",
        "FROM",
        "INSERT",
        "JOIN",
        "MERGE",
        "TABLE",
        "UPDATE"
    };
    final int[] tokenKinds = {
        DruidSqlParserImplConstants.DELETE,
        DruidSqlParserImplConstants.DESCRIBE,
        DruidSqlParserImplConstants.FROM,
        DruidSqlParserImplConstants.INSERT,
        DruidSqlParserImplConstants.JOIN,
        DruidSqlParserImplConstants.MERGE,
        DruidSqlParserImplConstants.TABLE,
        DruidSqlParserImplConstants.UPDATE
    };

    for (int i = 0; i < keywords.length; i++) {
      final DruidSqlParserImplTokenManager tokenManager = createBigQueryTokenManager(
          keywords[i] + " foo-bar abcxyz"
      );

      assertNextToken(tokenManager, tokenKinds[i], DruidSqlParserImplConstants.BQHID);
      assertNextToken(
          tokenManager,
          DruidSqlParserImplConstants.HYPHENATED_IDENTIFIER,
          DruidSqlParserImplConstants.BQID
      );
      assertNextToken(tokenManager, DruidSqlParserImplConstants.IDENTIFIER, DruidSqlParserImplConstants.BQID);
      assertNextToken(tokenManager, DruidSqlParserImplConstants.EOF, DruidSqlParserImplConstants.BQID);
      Assertions.assertTrue(tokenManager.lexicalStateStack.isEmpty());
    }
  }

  @Test
  public void testAfterTableNameActionsRestoreBigQueryIdentifierState()
  {
    final String[] keywords = {"SELECT", "SET", "VALUES"};
    final int[] tokenKinds = {
        DruidSqlParserImplConstants.SELECT,
        DruidSqlParserImplConstants.SET,
        DruidSqlParserImplConstants.VALUES
    };

    for (int i = 0; i < keywords.length; i++) {
      final DruidSqlParserImplTokenManager tokenManager = createBigQueryTokenManager(
          "FROM " + keywords[i] + " abcxyz"
      );

      assertNextToken(tokenManager, DruidSqlParserImplConstants.FROM, DruidSqlParserImplConstants.BQHID);
      assertNextToken(tokenManager, tokenKinds[i], DruidSqlParserImplConstants.BQID);
      assertNextToken(tokenManager, DruidSqlParserImplConstants.IDENTIFIER, DruidSqlParserImplConstants.BQID);
      Assertions.assertTrue(tokenManager.lexicalStateStack.isEmpty());
    }
  }

  @Test
  public void testCommentPreservesTableNameState()
  {
    final DruidSqlParserImplTokenManager tokenManager = createBigQueryTokenManager(
        "FROM /* comment */ foo-bar"
    );

    assertNextToken(tokenManager, DruidSqlParserImplConstants.FROM, DruidSqlParserImplConstants.BQHID);
    final Token tableName = assertNextToken(
        tokenManager,
        DruidSqlParserImplConstants.HYPHENATED_IDENTIFIER,
        DruidSqlParserImplConstants.BQID
    );

    Assertions.assertNotNull(tableName.specialToken);
    Assertions.assertEquals(DruidSqlParserImplConstants.MULTI_LINE_COMMENT, tableName.specialToken.kind);
    Assertions.assertTrue(tokenManager.lexicalStateStack.isEmpty());
  }

  @Test
  public void testBigQueryHyphenatedTableNameParses() throws SqlParseException
  {
    final SqlParser.Config parserConfig = DruidSqlParser.PARSER_CONFIG.withQuoting(Quoting.BACK_TICK_BACKSLASH);
    final SqlSelect select = (SqlSelect) SqlParser.create("SELECT * FROM foo-bar", parserConfig).parseStmt();

    Assertions.assertEquals("foo-bar", ((SqlIdentifier) select.getFrom()).getSimple());
  }

  private static DruidSqlParserImplTokenManager createBigQueryTokenManager(final String input)
  {
    return new DruidSqlParserImplTokenManager(
        new SimpleCharStream(new StringReader(input)),
        DruidSqlParserImplConstants.BQID
    );
  }

  private static Token assertNextToken(
      final DruidSqlParserImplTokenManager tokenManager,
      final int expectedKind,
      final int expectedLexicalState
  )
  {
    final Token token = tokenManager.getNextToken();
    Assertions.assertEquals(expectedKind, token.kind);
    Assertions.assertEquals(expectedLexicalState, tokenManager.curLexState);
    return token;
  }
}
