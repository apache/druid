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

import com.google.common.base.Joiner;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.SourceStringReader;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.query.QueryContext;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidConformance;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Contains the utility function {@link #parse(String, boolean)}, for parsing Druid SQL statements.
 */
public class DruidSqlParser
{
  public static final SqlParser.Config PARSER_CONFIG = SqlParser
      .config()
      .withCaseSensitive(true)
      .withUnquotedCasing(Casing.UNCHANGED)
      .withQuotedCasing(Casing.UNCHANGED)
      .withQuoting(Quoting.DOUBLE_QUOTE)
      .withConformance(DruidConformance.instance())
      .withParserFactory(new DruidSqlParserImplFactory());

  private static final Joiner SPACE_JOINER = Joiner.on(" ");
  private static final Joiner COMMA_JOINER = Joiner.on(", ");

  private DruidSqlParser()
  {
    // No instantiation.
  }

  public static StatementAndSetContext parse(final String sql, final boolean allowSetStatements)
  {
    try {
      SqlParser parser = SqlParser.create(new SourceStringReader(sql), PARSER_CONFIG);
      SqlNode sqlNode = parser.parseStmtList();
      return processStatementList(sqlNode, allowSetStatements);
    }
    catch (SqlParseException e) {
      throw translateParseException(e);
    }
  }

  /**
   * If an {@link SqlNode} is a {@link SqlNodeList}, it must consist of 0 or more {@link SqlSetOption} followed by a
   * single {@link SqlNode} which is NOT a {@link SqlSetOption}. All {@link SqlSetOption} will be converted into a
   * context parameters {@link Map} and added to the {@link PlannerContext} with
   * {@link PlannerContext#addAllToQueryContext(Map)}. The final {@link SqlNode} of the {@link SqlNodeList} is returned
   * by this method as the {@link SqlNode} which should actually be validated and executed, and will have access to the
   * modified query context through the {@link PlannerContext}. {@link SqlSetOption} override any existing query
   * context parameter values.
   */
  private static StatementAndSetContext processStatementList(
      SqlNode root,
      final boolean allowSetStatements
  )
  {
    if (root instanceof SqlNodeList) {
      final Map<String, Object> setContext = new LinkedHashMap<>();
      final SqlNodeList nodeList = (SqlNodeList) root;
      if (!allowSetStatements && nodeList.size() > 1) {
        throw InvalidSqlInput.exception("SQL query string must contain only a single statement");
      }
      boolean isMissingDruidStatementNode = true;
      // convert 0 or more SET statements into a Map of stuff to add to the query context
      for (int i = 0; i < nodeList.size(); i++) {
        SqlNode sqlNode = nodeList.get(i);
        if (sqlNode instanceof SqlSetOption) {
          final SqlSetOption sqlSetOption = (SqlSetOption) sqlNode;
          if (!(sqlSetOption.getValue() instanceof SqlLiteral)) {
            throw InvalidSqlInput.exception(
                "Assigned value must be a literal for SET statement[%s]",
                sqlSetOption.toSqlString(CalciteSqlDialect.DEFAULT)
            );
          }
          setContext.put(
              sqlSetOption.getName().getSimple(),
              sqlLiteralToContextValue((SqlLiteral) sqlSetOption.getValue())
          );
        } else if (i < nodeList.size() - 1) {
          // only SET statements can appear before the last statement
          throw InvalidSqlInput.exception(
              "Only SET statements can appear before the final statement in a statement list, but found non-SET statement[%s]",
              sqlNode.toSqlString(CalciteSqlDialect.DEFAULT)
          );
        } else {
          // last SqlNode
          root = sqlNode;
          isMissingDruidStatementNode = false;
        }
      }
      if (isMissingDruidStatementNode) {
        throw InvalidSqlInput.exception("Statement list is missing a non-SET statement to execute");
      }

      return new StatementAndSetContext(root, setContext);
    } else {
      return new StatementAndSetContext(root, Collections.emptyMap());
    }
  }

  /**
   * Coerces a SQL literal from a SET statement to a form acceptable for {@link QueryContext}.
   */
  @Nullable
  static Object sqlLiteralToContextValue(final SqlLiteral literal)
  {
    if (SqlUtil.isNullLiteral(literal, false)) {
      return null;
    } else if (SqlTypeName.CHAR_TYPES.contains(literal.getTypeName())) {
      return ((NlsString) literal.getValue()).getValue();
    } else if (SqlTypeName.BOOLEAN_TYPES.contains(literal.getTypeName())) {
      return literal.getValue();
    } else if (SqlTypeName.NUMERIC_TYPES.contains(literal.getTypeName())) {
      final Number number = (Number) literal.getValue();
      if (number instanceof BigDecimal && number.equals(BigDecimal.valueOf(number.longValue()))) {
        return number.longValue();
      } else if (number instanceof BigInteger && number.equals(BigInteger.valueOf(number.longValue()))) {
        return number.longValue();
      } else if (number instanceof BigDecimal && number.equals(BigDecimal.valueOf(number.doubleValue()))) {
        return number.doubleValue();
      } else {
        return number.toString();
      }
    } else if (literal.getTypeName() == SqlTypeName.DATE) {
      return Calcites.CALCITE_DATE_PARSER.parse(literal.getValue().toString()).toString();
    } else if (literal.getTypeName() == SqlTypeName.TIMESTAMP) {
      return Calcites.CALCITE_TIMESTAMP_PARSER.parse(literal.getValue().toString()).toString();
    } else {
      throw InvalidSqlInput.exception("Unsupported type for SET[%s]", literal.getTypeName());
    }
  }

  /**
   * Constructs a user-friendly {@link DruidException} from a Calcite {@link SqlParseException}.
   */
  private static DruidException translateParseException(SqlParseException e)
  {
    final Throwable cause = e.getCause();
    if (cause instanceof DruidException) {
      return (DruidException) cause;
    }

    if (cause instanceof ParseException) {
      ParseException parseException = (ParseException) cause;
      final SqlParserPos failurePosition = e.getPos();
      // When calcite catches a syntax error at the top level
      // expected token sequences can be null.
      // In such a case return the syntax error to the user
      // wrapped in a DruidException with invalid input
      if (parseException.expectedTokenSequences == null) {
        return DruidException.forPersona(DruidException.Persona.USER)
                             .ofCategory(DruidException.Category.INVALID_INPUT)
                             .withErrorCode("invalidInput")
                             .build(e, "%s", e.getMessage())
                             .withContext("sourceType", "sql");
      } else {
        final String theUnexpectedToken = getUnexpectedTokenString(parseException);

        final String[] tokenDictionary = e.getTokenImages();
        final int[][] expectedTokenSequences = e.getExpectedTokenSequences();
        final ArrayList<String> expectedTokens = new ArrayList<>(expectedTokenSequences.length);
        for (int[] expectedTokenSequence : expectedTokenSequences) {
          String[] strings = new String[expectedTokenSequence.length];
          for (int i = 0; i < expectedTokenSequence.length; ++i) {
            strings[i] = tokenDictionary[expectedTokenSequence[i]];
          }
          expectedTokens.add(SPACE_JOINER.join(strings));
        }

        return InvalidSqlInput
            .exception(
                e,
                "Received an unexpected token [%s] (line [%s], column [%s]), acceptable options: [%s]",
                theUnexpectedToken,
                failurePosition.getLineNum(),
                failurePosition.getColumnNum(),
                COMMA_JOINER.join(expectedTokens)
            )
            .withContext("line", failurePosition.getLineNum())
            .withContext("column", failurePosition.getColumnNum())
            .withContext("endLine", failurePosition.getEndLineNum())
            .withContext("endColumn", failurePosition.getEndColumnNum())
            .withContext("token", theUnexpectedToken)
            .withContext("expected", expectedTokens);

      }
    }

    return InvalidSqlInput.exception(e.getMessage());
  }

  /**
   * Grabs the unexpected token string.  This code is borrowed with minimal adjustments from
   * {@link ParseException#getMessage()}.  It is possible that if that code changes, we need to also
   * change this code to match it.
   *
   * @param parseException the parse exception to extract from
   *
   * @return the String representation of the unexpected token string
   */
  private static String getUnexpectedTokenString(ParseException parseException)
  {
    int maxSize = 0;
    for (int[] ints : parseException.expectedTokenSequences) {
      if (maxSize < ints.length) {
        maxSize = ints.length;
      }
    }

    StringBuilder bob = new StringBuilder();
    Token tok = parseException.currentToken.next;
    for (int i = 0; i < maxSize; i++) {
      if (i != 0) {
        bob.append(" ");
      }
      if (tok.kind == 0) {
        bob.append("<EOF>");
        break;
      }
      char ch;
      for (int i1 = 0; i1 < tok.image.length(); i1++) {
        switch (tok.image.charAt(i1)) {
          case 0:
            continue;
          case '\b':
            bob.append("\\b");
            continue;
          case '\t':
            bob.append("\\t");
            continue;
          case '\n':
            bob.append("\\n");
            continue;
          case '\f':
            bob.append("\\f");
            continue;
          case '\r':
            bob.append("\\r");
            continue;
          case '\"':
            bob.append("\\\"");
            continue;
          case '\'':
            bob.append("\\\'");
            continue;
          case '\\':
            bob.append("\\\\");
            continue;
          default:
            if ((ch = tok.image.charAt(i1)) < 0x20 || ch > 0x7e) {
              String s = "0000" + Integer.toString(ch, 16);
              bob.append("\\u").append(s.substring(s.length() - 4, s.length()));
            } else {
              bob.append(ch);
            }
        }
      }
      tok = tok.next;
    }
    return bob.toString();
  }
}
