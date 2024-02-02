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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;

import java.util.Set;

public class DruidSqlIngestOperator extends SqlSpecialOperator implements AuthorizableOperator
{
  public static final SqlSpecialOperator INSERT_OPERATOR =
      new DruidSqlInsertOperator();
  public static final SqlSpecialOperator REPLACE_OPERATOR =
      new DruidSqlReplaceOperator();

  public static class DruidSqlInsertOperator extends DruidSqlIngestOperator
  {
    public DruidSqlInsertOperator()
    {
      super("INSERT");
    }

    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands
    )
    {
      return new DruidSqlInsert(
          pos,
          // Must match SqlInsert.getOperandList()
          (SqlNodeList) operands[0],
          operands[1],
          operands[2],
          (SqlNodeList) operands[3],
          // Must match DruidSqlIngest.getOperandList()
          operands[4],
          (SqlNodeList) operands[5]
      );
    }
  }

  public static class DruidSqlReplaceOperator extends DruidSqlIngestOperator
  {
    public DruidSqlReplaceOperator()
    {
      super("REPLACE");
    }

    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands
    )
    {
      return new DruidSqlReplace(
          pos,
          // Must match SqlInsert.getOperandList()
          (SqlNodeList) operands[0],
          operands[1],
          operands[2],
          (SqlNodeList) operands[3],
          // Must match DruidSqlIngest.getOperandList()
          operands[4],
          (SqlNodeList) operands[5],
          // Must match DruidSqlReplace.getOperandList()
          operands[6]
      );
    }
  }

  public DruidSqlIngestOperator(String name)
  {
    super(name, SqlKind.INSERT);
  }

  @Override
  public Set<ResourceAction> computeResources(SqlCall call, final boolean inputSourceTypeSecurityEnabled)
  {
    DruidSqlIngest ingestNode = (DruidSqlIngest) call;
    final SqlIdentifier tableIdentifier = (SqlIdentifier) ingestNode.getTargetTable();
    String targetDatasource = tableIdentifier.names.get(tableIdentifier.names.size() - 1);
    return ImmutableSet.of(new ResourceAction(new Resource(targetDatasource, ResourceType.DATASOURCE), Action.WRITE));
  }
}
