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

package org.apache.druid.sql.calcite.external;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCollectionTableOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.druid.java.util.common.ISE;

/**
 * Druid-specific implementation of the EXTEND operator in Calcite, which
 * is said to have been added for Apache Phoenix, and which we repurpose to
 * supply a schema for an ingest input table.
 *
 * @see {@link UserDefinedTableMacroFunction} for details
 */
public class ExtendOperator extends SqlInternalOperator
{
  //  private static final TableMacro macro = new ExtendsMacroWrapper();
  public static final ExtendOperator EXTEND = new ExtendOperator();

  ExtendOperator()
  {
    super("EXTEND", SqlKind.EXTEND, MDX_PRECEDENCE);
  }

  /**
   * Rewrite the EXTEND node (which, in Druid, has a structure different
   * than what Calcite expects), into a table macro, with the schema
   * squirreled away in an ad-hoc instance of the macro. We must do it
   * this way because we can't change Calcite to define a new node type
   * that holds onto the schema.
   */
  @Override
  public SqlNode rewriteCall(SqlValidator validator, SqlCall call)
  {
    SqlBasicCall tableOpCall = (SqlBasicCall) call.operand(0);
    if (!(tableOpCall.getOperator() instanceof SqlCollectionTableOperator)) {
      throw new ISE("First argument to EXTEND must be a table function");
    }
    SqlBasicCall tableFnCall = (SqlBasicCall) tableOpCall.operand(0);
    if (!(tableFnCall.getOperator() instanceof UserDefinedTableMacroFunction)) {
      // May be an unresolved function.
      return call;
    }
    UserDefinedTableMacroFunction macro = (UserDefinedTableMacroFunction) tableFnCall.getOperator();

    SqlNodeList schema = (SqlNodeList) call.operand(1);
    SqlCall newCall = macro.rewriteCall(tableFnCall, schema);
    return SqlStdOperatorTable.COLLECTION_TABLE.createCall(call.getParserPosition(), newCall);
  }
}
