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
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A single {@code <name> <type>} column declaration within a Druid DDL statement, such as the column list of
 * {@code CREATE TABLE} or the target of {@code ALTER TABLE ... ADD COLUMN}.
 */
public class DruidSqlColumnDeclaration extends SqlCall
{
  public static final SqlOperator OPERATOR = new DruidSqlColumnDeclarationOperator();

  /**
   * The {@code TYPE('...')} escape hatch used to name Druid native types that have no SQL spelling, defined by the
   * {@code DruidType()} production in {@code common.ftl}.
   */
  public static final String COMPLEX_TYPE_FUNCTION = "TYPE";

  private final SqlIdentifier name;
  private final SqlDataTypeSpec dataType;

  public DruidSqlColumnDeclaration(
      SqlParserPos pos,
      SqlIdentifier name,
      SqlDataTypeSpec dataType
  )
  {
    super(pos);
    this.name = name;
    this.dataType = dataType;
  }

  public SqlIdentifier getName()
  {
    return name;
  }

  public SqlDataTypeSpec getDataType()
  {
    return dataType;
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
    return ImmutableNullableList.of(name, dataType);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    name.unparse(writer, 0, 0);
    unparseDataType(writer, dataType);
  }

  /**
   * Unparse a column type. Types named by {@link #COMPLEX_TYPE_FUNCTION} (the {@code TYPE('...')} escape hatch used
   * for Druid native types such as {@code COMPLEX<json>}) are written back in that form, since
   * {@link SqlUserDefinedTypeNameSpec} would otherwise emit the bare name, which does not parse.
   */
  public static void unparseDataType(SqlWriter writer, SqlDataTypeSpec dataType)
  {
    if (dataType.getTypeNameSpec() instanceof SqlUserDefinedTypeNameSpec) {
      writer.keyword(COMPLEX_TYPE_FUNCTION);
      final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      SqlLiteral.createCharString(dataType.getTypeName().toString(), dataType.getParserPosition())
                .unparse(writer, 0, 0);
      writer.endList(frame);
    } else {
      dataType.unparse(writer, 0, 0);
    }
  }

  private static class DruidSqlColumnDeclarationOperator extends SqlSpecialOperator
  {
    public DruidSqlColumnDeclarationOperator()
    {
      super("COLUMN_DECL", SqlKind.COLUMN_DECL);
    }

    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands
    )
    {
      return new DruidSqlColumnDeclaration(pos, (SqlIdentifier) operands[0], (SqlDataTypeSpec) operands[1]);
    }
  }
}
