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
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * A {@code PROJECTION <name> AS ( SELECT ... )} clause, which defines a projection of the table it appears in.
 * <p>
 * The body is a {@link SqlSelect} with no FROM clause: the table is implicit, and the grammar admits only a select
 * list, an optional WHERE and an optional GROUP BY. ORDER BY, LIMIT, HAVING, joins and set operations are
 * structurally excluded rather than validated away, because a projection cannot express them: its ordering is
 * derived from its grouping columns.
 */
public class SqlProjectionSpec extends SqlCall
{
  public static final SqlOperator OPERATOR = new SqlProjectionSpecOperator();

  private final SqlIdentifier name;
  @Nullable
  private final SqlNodeList clusteredBy;
  private final SqlSelect body;

  public SqlProjectionSpec(
      SqlParserPos pos,
      SqlIdentifier name,
      @Nullable SqlNodeList clusteredBy,
      SqlSelect body
  )
  {
    super(pos);
    this.name = name;
    this.clusteredBy = clusteredBy;
    this.body = body;
  }

  /**
   * The columns segments are clustered on, meaningful only for the reserved base-table projection: an aggregate
   * projection is ordered by its grouping columns and has nothing to choose.
   */
  @Nullable
  public SqlNodeList getClusteredBy()
  {
    return clusteredBy;
  }

  public SqlIdentifier getName()
  {
    return name;
  }

  public SqlSelect getBody()
  {
    return body;
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
    return ImmutableNullableList.of(name, clusteredBy, body);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.keyword("PROJECTION");
    name.unparse(writer, 0, 0);
    writer.keyword("AS");
    final SqlWriter.Frame frame = writer.startList("(", ")");
    // Unparse through the operator rather than the node: a SqlSelect in this position parenthesizes itself, which
    // together with the frame above would print a second set the grammar cannot read back. The parentheses belong to
    // the projection, not the body, because CLUSTERED BY goes inside them.
    body.getOperator().unparse(writer, body, 0, 0);
    if (clusteredBy != null) {
      writer.keyword("CLUSTERED BY");
      final SqlWriter.Frame clusterFrame = writer.startList("", "");
      for (SqlNode column : clusteredBy) {
        writer.sep(",");
        column.unparse(writer, 0, 0);
      }
      writer.endList(clusterFrame);
    }
    writer.endList(frame);
  }

  private static class SqlProjectionSpecOperator extends SqlSpecialOperator
  {
    public SqlProjectionSpecOperator()
    {
      super("PROJECTION", SqlKind.OTHER);
    }

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands)
    {
      return new SqlProjectionSpec(
          pos,
          (SqlIdentifier) operands[0],
          (SqlNodeList) operands[1],
          (SqlSelect) operands[2]
      );
    }
  }
}
