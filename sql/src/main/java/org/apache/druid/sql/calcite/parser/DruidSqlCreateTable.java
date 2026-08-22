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
import org.apache.calcite.sql.SqlCreate;
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
import javax.annotation.Nullable;
import java.util.List;

/**
 * {@code CREATE [OR REPLACE] TABLE [IF NOT EXISTS] <name> (<columns>) [PARTITIONED BY <granularity>]
 * [CLUSTERED BY <columns>]}, which defines a table in the Druid catalog.
 * <p>
 * This statement writes catalog metadata only; it neither creates segments nor otherwise touches data. See
 * {@link org.apache.druid.sql.calcite.planner.CatalogDdlHandler} for the execution side.
 */
public class DruidSqlCreateTable extends SqlCreate
{
  public static final SqlOperator OPERATOR = new DruidSqlCreateTableOperator();

  private final SqlIdentifier name;
  private final SqlNodeList columnList;
  private final SqlNodeList projectionList;
  @Nullable
  private final SqlGranularityLiteral partitionedBy;
  @Nullable
  private final SqlNodeList clusteredBy;
  private final boolean sealed;

  public DruidSqlCreateTable(
      SqlParserPos pos,
      boolean replace,
      boolean ifNotExists,
      SqlIdentifier name,
      SqlNodeList columnList,
      SqlNodeList projectionList,
      @Nullable SqlGranularityLiteral partitionedBy,
      @Nullable SqlNodeList clusteredBy,
      boolean sealed
  )
  {
    super(OPERATOR, pos, replace, ifNotExists);
    this.sealed = sealed;
    this.name = name;
    this.columnList = columnList;
    this.projectionList = projectionList;
    this.partitionedBy = partitionedBy;
    this.clusteredBy = clusteredBy;
  }

  public SqlIdentifier getName()
  {
    return name;
  }

  /**
   * The declared columns, each a {@link DruidSqlColumnDeclaration}. Order is significant: it is the order columns are
   * recorded in the catalog table spec.
   */
  public SqlNodeList getColumnList()
  {
    return columnList;
  }

  /**
   * The declared projections, each a {@link SqlProjectionSpec}.
   */
  public SqlNodeList getProjectionList()
  {
    return projectionList;
  }

  @Nullable
  public SqlGranularityLiteral getPartitionedBy()
  {
    return partitionedBy;
  }

  @Nullable
  public SqlNodeList getClusteredBy()
  {
    return clusteredBy;
  }

  public boolean isIfNotExists()
  {
    return ifNotExists;
  }

  /**
   * Whether the statement declared SEALED, which requires every ingested column to be declared.
   */
  public boolean isSealed()
  {
    return sealed;
  }

  @Nonnull
  @Override
  public List<SqlNode> getOperandList()
  {
    // The replace and ifNotExists flags travel as operands so that createCall() can rebuild an equivalent node.
    return ImmutableNullableList.of(
        name,
        columnList,
        projectionList,
        partitionedBy,
        clusteredBy,
        SqlLiteral.createBoolean(getReplace(), SqlParserPos.ZERO),
        SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO),
        SqlLiteral.createBoolean(sealed, SqlParserPos.ZERO)
    );
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.keyword("CREATE");
    if (getReplace()) {
      writer.keyword("OR REPLACE");
    }
    writer.keyword("TABLE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);

    // The element list is optional and, when present, must hold at least one element, so a table declaring nothing
    // prints no parentheses at all: "()" is not something the grammar can read back.
    if (!columnList.isEmpty() || !projectionList.isEmpty()) {
      final SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode column : columnList) {
        writer.sep(",");
        column.unparse(writer, 0, 0);
      }
      for (SqlNode projection : projectionList) {
        writer.sep(",");
        projection.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }

    if (partitionedBy != null) {
      writer.keyword("PARTITIONED BY");
      partitionedBy.unparse(writer, 0, 0);
    }

    if (clusteredBy != null) {
      writer.keyword("CLUSTERED BY");
      final SqlWriter.Frame clusterFrame = writer.startList("", "");
      for (SqlNode clusterByOpts : clusteredBy.getList()) {
        writer.sep(",");
        clusterByOpts.unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(clusterFrame);
    }

    if (sealed) {
      writer.keyword("SEALED");
    }
  }

  private static class DruidSqlCreateTableOperator extends SqlSpecialOperator
  {
    public DruidSqlCreateTableOperator()
    {
      super("CREATE TABLE", SqlKind.CREATE_TABLE);
    }

    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands
    )
    {
      return new DruidSqlCreateTable(
          pos,
          ((SqlLiteral) operands[5]).booleanValue(),
          ((SqlLiteral) operands[6]).booleanValue(),
          (SqlIdentifier) operands[0],
          (SqlNodeList) operands[1],
          (SqlNodeList) operands[2],
          (SqlGranularityLiteral) operands[3],
          (SqlNodeList) operands[4],
          ((SqlLiteral) operands[7]).booleanValue()
      );
    }
  }
}
