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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

/**
 * Druid-specific implementation of the EXTEND operator in Calcite, which
 * is said to have been added for Apache Phoenix, and which we repurpose to
 * supply a schema for an ingest input table.
 *
 * @see {@link SchemaAwareUserDefinedTableMacro} for details
 */
public class ExtendOperator extends SqlInternalOperator
{
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
   * <p>
   * The node structure is:<pre>{@code
   * EXTEND(
   *   TABLE(
   *     <table fn>(
   *       <table macro>
   *     )
   *   ),
   *   <schema>
   * )}</pre>
   * <p>
   * Note that the table macro is not an operand: it is an implicit
   * member of the table macro function operator.
   */
  @Override
  public SqlNode rewriteCall(SqlValidator validator, SqlCall call)
  {
    SqlBasicCall tableOpCall = call.operand(0);
    if (!(tableOpCall.getOperator() instanceof SqlCollectionTableOperator)) {
      throw new ISE("First argument to EXTEND must be TABLE");
    }

    // The table function must be a Druid-defined table macro function
    // which is aware of the EXTEND schema.
    SqlBasicCall tableFnCall = tableOpCall.operand(0);
    if (!(tableFnCall.getOperator() instanceof SchemaAwareUserDefinedTableMacro)) {
      // May be an unresolved function.
      throw new IAE(
          "Function %s does not accept an EXTEND clause (or a schema list)",
          tableFnCall.getOperator().getName()
      );
    }

    // Move the schema from the second operand of EXTEND into a member
    // function of a shim table macro.
    SchemaAwareUserDefinedTableMacro tableFn = (SchemaAwareUserDefinedTableMacro) tableFnCall.getOperator();
    SqlNodeList schema = call.operand(1);
    SqlCall newCall = tableFn.rewriteCall(tableFnCall, schema);

    // Create a new TABLE(table_fn) node to replace the EXTEND node. After this,
    // the table macro function acts just like a standard Calcite version.
    return SqlStdOperatorTable.COLLECTION_TABLE.createCall(call.getParserPosition(), newCall);
  }
}
