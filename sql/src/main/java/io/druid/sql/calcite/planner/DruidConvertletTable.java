/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.planner;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;

public class DruidConvertletTable implements SqlRexConvertletTable
{
  private static final DruidConvertletTable INSTANCE = new DruidConvertletTable();

  private DruidConvertletTable()
  {
  }

  public static DruidConvertletTable instance()
  {
    return INSTANCE;
  }

  @Override
  public SqlRexConvertlet get(SqlCall call)
  {
    if (call.getKind() == SqlKind.EXTRACT && call.getOperandList().get(1).getKind() != SqlKind.LITERAL) {
      return new SqlRexConvertlet()
      {
        @Override
        public RexNode convertCall(SqlRexContext cx, SqlCall call)
        {
          return StandardConvertletTable.INSTANCE.convertCall(cx, call);
        }
      };
    } else {
      return StandardConvertletTable.INSTANCE.get(call);
    }
  }
}
