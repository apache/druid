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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * {@code ALTER TABLE <name> <operation>}, which edits the catalog metadata of an existing table.
 * <p>
 * Each concrete subclass corresponds to exactly one catalog edit operation, so that a single statement is always a
 * single atomic change on the Coordinator. See {@link org.apache.druid.sql.calcite.planner.CatalogDdlHandler}.
 */
public abstract class DruidSqlAlterTable extends SqlCall
{
  private final SqlIdentifier name;

  protected DruidSqlAlterTable(SqlParserPos pos, SqlIdentifier name)
  {
    super(pos);
    this.name = name;
  }

  public SqlIdentifier getName()
  {
    return name;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.keyword("ALTER TABLE");
    name.unparse(writer, leftPrec, rightPrec);
    unparseOperation(writer, leftPrec, rightPrec);
  }

  /**
   * Unparse the portion of the statement following the table name.
   */
  protected abstract void unparseOperation(SqlWriter writer, int leftPrec, int rightPrec);

  /**
   * {@code ALTER TABLE <name> ADD COLUMN <column> <type>}.
   */
  public static class AddColumn extends DruidSqlAlterTable
  {
    public static final SqlOperator OPERATOR = new Operator("ALTER TABLE ADD COLUMN");

    private final DruidSqlColumnDeclaration column;

    public AddColumn(SqlParserPos pos, SqlIdentifier name, DruidSqlColumnDeclaration column)
    {
      super(pos, name);
      this.column = column;
    }

    public DruidSqlColumnDeclaration getColumn()
    {
      return column;
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
      return ImmutableNullableList.of(getName(), column);
    }

    @Override
    protected void unparseOperation(SqlWriter writer, int leftPrec, int rightPrec)
    {
      writer.keyword("ADD COLUMN");
      column.unparse(writer, 0, 0);
    }

    private static class Operator extends SqlSpecialOperator
    {
      Operator(String name)
      {
        super(name, SqlKind.ALTER_TABLE);
      }

      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands)
      {
        return new AddColumn(pos, (SqlIdentifier) operands[0], (DruidSqlColumnDeclaration) operands[1]);
      }
    }
  }

  /**
   * {@code ALTER TABLE <name> DROP COLUMN <column>}. Removes the column from the catalog spec; existing segments are
   * unaffected.
   */
  public static class DropColumn extends DruidSqlAlterTable
  {
    public static final SqlOperator OPERATOR = new Operator("ALTER TABLE DROP COLUMN");

    private final SqlIdentifier column;

    public DropColumn(SqlParserPos pos, SqlIdentifier name, SqlIdentifier column)
    {
      super(pos, name);
      this.column = column;
    }

    public SqlIdentifier getColumn()
    {
      return column;
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
      return ImmutableNullableList.of(getName(), column);
    }

    @Override
    protected void unparseOperation(SqlWriter writer, int leftPrec, int rightPrec)
    {
      writer.keyword("DROP COLUMN");
      column.unparse(writer, 0, 0);
    }

    private static class Operator extends SqlSpecialOperator
    {
      Operator(String name)
      {
        super(name, SqlKind.ALTER_TABLE);
      }

      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands)
      {
        return new DropColumn(pos, (SqlIdentifier) operands[0], (SqlIdentifier) operands[1]);
      }
    }
  }

  /**
   * {@code ALTER TABLE <name> ALTER COLUMN <column> SET DATA TYPE <type>}.
   */
  public static class AlterColumn extends DruidSqlAlterTable
  {
    public static final SqlOperator OPERATOR = new Operator("ALTER TABLE ALTER COLUMN");

    private final DruidSqlColumnDeclaration column;

    public AlterColumn(SqlParserPos pos, SqlIdentifier name, DruidSqlColumnDeclaration column)
    {
      super(pos, name);
      this.column = column;
    }

    public DruidSqlColumnDeclaration getColumn()
    {
      return column;
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
      return ImmutableNullableList.of(getName(), column);
    }

    @Override
    protected void unparseOperation(SqlWriter writer, int leftPrec, int rightPrec)
    {
      writer.keyword("ALTER COLUMN");
      column.getName().unparse(writer, 0, 0);
      writer.keyword("SET DATA TYPE");
      DruidSqlColumnDeclaration.unparseDataType(writer, column.getDataType());
    }

    private static class Operator extends SqlSpecialOperator
    {
      Operator(String name)
      {
        super(name, SqlKind.ALTER_TABLE);
      }

      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands)
      {
        return new AlterColumn(pos, (SqlIdentifier) operands[0], (DruidSqlColumnDeclaration) operands[1]);
      }
    }
  }

  /**
   * {@code ALTER TABLE <name> ADD [IF NOT EXISTS] PROJECTION <name> AS ( ... )}.
   */
  public static class AddProjection extends DruidSqlAlterTable
  {
    public static final SqlOperator OPERATOR = new Operator("ALTER TABLE ADD PROJECTION");

    private final SqlProjectionSpec projection;
    private final boolean ifNotExists;

    public AddProjection(
        SqlParserPos pos,
        SqlIdentifier name,
        SqlProjectionSpec projection,
        boolean ifNotExists
    )
    {
      super(pos, name);
      this.projection = projection;
      this.ifNotExists = ifNotExists;
    }

    public SqlProjectionSpec getProjection()
    {
      return projection;
    }

    public boolean isIfNotExists()
    {
      return ifNotExists;
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
      return ImmutableNullableList.of(
          getName(),
          projection,
          SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO)
      );
    }

    @Override
    protected void unparseOperation(SqlWriter writer, int leftPrec, int rightPrec)
    {
      writer.keyword("ADD");
      if (ifNotExists) {
        writer.keyword("IF NOT EXISTS");
      }
      projection.unparse(writer, 0, 0);
    }

    private static class Operator extends SqlSpecialOperator
    {
      Operator(String name)
      {
        super(name, SqlKind.ALTER_TABLE);
      }

      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands)
      {
        return new AddProjection(
            pos,
            (SqlIdentifier) operands[0],
            (SqlProjectionSpec) operands[1],
            ((SqlLiteral) operands[2]).booleanValue()
        );
      }
    }
  }

  /**
   * {@code ALTER TABLE <name> DROP PROJECTION [IF EXISTS] <name>}. Existing segments keep whatever projections they
   * were built with; this only stops future ingestion from building it.
   */
  public static class DropProjection extends DruidSqlAlterTable
  {
    public static final SqlOperator OPERATOR = new Operator("ALTER TABLE DROP PROJECTION");

    private final SqlIdentifier projectionName;
    private final boolean ifExists;

    public DropProjection(
        SqlParserPos pos,
        SqlIdentifier name,
        SqlIdentifier projectionName,
        boolean ifExists
    )
    {
      super(pos, name);
      this.projectionName = projectionName;
      this.ifExists = ifExists;
    }

    public SqlIdentifier getProjectionName()
    {
      return projectionName;
    }

    public boolean isIfExists()
    {
      return ifExists;
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
      return ImmutableNullableList.of(
          getName(),
          projectionName,
          SqlLiteral.createBoolean(ifExists, SqlParserPos.ZERO)
      );
    }

    @Override
    protected void unparseOperation(SqlWriter writer, int leftPrec, int rightPrec)
    {
      writer.keyword("DROP PROJECTION");
      if (ifExists) {
        writer.keyword("IF EXISTS");
      }
      projectionName.unparse(writer, 0, 0);
    }

    private static class Operator extends SqlSpecialOperator
    {
      Operator(String name)
      {
        super(name, SqlKind.ALTER_TABLE);
      }

      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands)
      {
        return new DropProjection(
            pos,
            (SqlIdentifier) operands[0],
            (SqlIdentifier) operands[1],
            ((SqlLiteral) operands[2]).booleanValue()
        );
      }
    }
  }

  /**
   * {@code ALTER TABLE <name> SET PROPERTIES (<key> = <value>, ...)}. A {@code NULL} value removes the property.
   */
  public static class SetProperties extends DruidSqlAlterTable
  {
    public static final SqlOperator OPERATOR = new Operator("ALTER TABLE SET PROPERTIES");

    private final SqlNodeList properties;

    public SetProperties(SqlParserPos pos, SqlIdentifier name, SqlNodeList properties)
    {
      super(pos, name);
      this.properties = properties;
    }

    /**
     * The property assignments, each a {@link DruidSqlPropertyAssignment}.
     */
    public SqlNodeList getProperties()
    {
      return properties;
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
      return ImmutableNullableList.of(getName(), properties);
    }

    @Override
    protected void unparseOperation(SqlWriter writer, int leftPrec, int rightPrec)
    {
      writer.keyword("SET PROPERTIES");
      final SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode property : properties) {
        writer.sep(",");
        property.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }

    private static class Operator extends SqlSpecialOperator
    {
      Operator(String name)
      {
        super(name, SqlKind.ALTER_TABLE);
      }

      @Override
      public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands)
      {
        return new SetProperties(pos, (SqlIdentifier) operands[0], (SqlNodeList) operands[1]);
      }
    }
  }
}
