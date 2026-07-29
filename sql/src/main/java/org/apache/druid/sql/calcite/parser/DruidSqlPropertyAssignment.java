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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A single {@code <key> = <value>} pair within {@code ALTER TABLE ... SET PROPERTIES (...)}. The value is a literal;
 * a {@code NULL} literal means "remove this property".
 */
public class DruidSqlPropertyAssignment extends SqlCall
{
  public static final SqlOperator OPERATOR = new DruidSqlPropertyAssignmentOperator();

  private final SqlIdentifier key;
  private final SqlNode value;

  public DruidSqlPropertyAssignment(SqlParserPos pos, SqlIdentifier key, SqlNode value)
  {
    super(pos);
    this.key = key;
    this.value = value;
  }

  public SqlIdentifier getKey()
  {
    return key;
  }

  /**
   * The assigned value. Normally a {@link SqlLiteral}, but a multi-line string literal parses as a concatenation
   * call, so callers must handle a non-literal node rather than assume the cast succeeds.
   */
  public SqlNode getValue()
  {
    return value;
  }

  @Nonnull
  @Override
  public SqlOperator getOperator()
  {
    return OPERATOR;
  }

  @Nonnull
  @Override
  public List<SqlNode> getOperandList()
  {
    return ImmutableNullableList.of(key, value);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    key.unparse(writer, 0, 0);
    writer.keyword("=");
    value.unparse(writer, 0, 0);
  }

  private static class DruidSqlPropertyAssignmentOperator extends SqlSpecialOperator
  {
    public DruidSqlPropertyAssignmentOperator()
    {
      super("PROPERTY_ASSIGNMENT", SqlKind.OTHER);
    }

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands)
    {
      return new DruidSqlPropertyAssignment(pos, (SqlIdentifier) operands[0], operands[1]);
    }
  }
}
