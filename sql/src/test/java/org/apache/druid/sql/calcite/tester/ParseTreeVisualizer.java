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

package org.apache.druid.sql.calcite.tester;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlVisitor;

import java.util.List;

/**
 * Calcite SqlVisitor to visualize a parse tree for use in
 * a test case.
 */
public class ParseTreeVisualizer implements SqlVisitor<Void>
{
  private final ParseTreeSerializer out = new ParseTreeSerializer();

  @Override
  public Void visit(SqlLiteral literal)
  {
    out.node(literal, literal.toString());
    return null;
  }

  @Override
  public Void visit(SqlCall call)
  {
    switch (call.getKind()) {
      case SELECT:
        expandSelect((SqlSelect) call);
        break;
      default:
        out.node(call, null);
        if (call.getOperandList() != null) {
          visit(call.getOperandList());
        }
    }
    return null;
  }

  private void expandSelect(SqlSelect node)
  {
    // Node and keywords
    out.node(node, node.getOperandList().get(0).toString());
    out.push();
    prefixed("SELECT", node.getSelectList());
    prefixed("FROM", node.getFrom());
    prefixed("WHERE", node.getWhere());
    prefixed("GROUP BY", node.getGroup());
    prefixed("HAVING", node.getHaving());
    prefixed("WINDOW", node.getWindowList());
    prefixed("ORDER BY", node.getOrderList());
    prefixed("OFFSET", node.getOffset());
    prefixed("FETCH", node.getFetch());
    out.pop();
  }

  private void prefixed(String prefix, SqlNode node)
  {
    if (node == null) {
      return;
    }
    out.prefix(prefix);
    node.accept(this);
  }

  @Override
  public Void visit(SqlNodeList nodeList)
  {
    if (nodeList.getList().isEmpty()) {
      out.prefix(null);
    } else {
      out.text("(");
      visit(nodeList.getList());
      out.text(")");
    }
    return null;
  }

  public void visit(List<SqlNode> nodeList)
  {
    out.push();
    for (SqlNode node : nodeList) {
      if (node == null) {
        out.text("<null>");
      } else {
        node.accept(this);
      }
    }
    out.pop();
  }

  @Override
  public Void visit(SqlIdentifier id)
  {
    out.node(id, id.toString());
    return null;
  }

  @Override
  public Void visit(SqlDataTypeSpec type)
  {
    out.node(type, type.toString());
    return null;
  }

  @Override
  public Void visit(SqlDynamicParam param)
  {
    out.node(param, param.toString());
    return null;
  }

  @Override
  public Void visit(SqlIntervalQualifier intervalQualifier)
  {
    out.node(intervalQualifier, intervalQualifier.toString());
    return null;
  }

  public String result()
  {
    return out.result();
  }
}
