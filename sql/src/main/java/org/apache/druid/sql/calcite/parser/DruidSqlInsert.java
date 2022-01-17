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

import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Extends the Insert call to hold custom paramaters specific to druid i.e. PARTITION BY and CLUSTER BY
 * This class extends the {@link SqlInsert} so that the node can be used for further conversion
 */
public class DruidSqlInsert extends SqlInsert
{
  // Unsure if this should be kept as is, but this allows reusing super.unparse
  public static final SqlOperator OPERATOR = SqlInsert.OPERATOR;

  final private SqlNode partitionBy;
  final private SqlNodeList clusterBy;

  public DruidSqlInsert(
      @Nonnull SqlInsert insertNode,
      @Nullable SqlNode partitionBy,
      @Nullable SqlNodeList clusterBy
  )
  {
    super(
        insertNode.getParserPosition(),
        (SqlNodeList) insertNode.getOperandList().get(0), // No better getter to extract this
        insertNode.getTargetTable(),
        insertNode.getSource(),
        insertNode.getTargetColumnList()
    );
    this.partitionBy = partitionBy;
    this.clusterBy = clusterBy;
  }

  @Nullable
  public SqlNodeList getClusterBy()
  {
    return clusterBy;
  }

  @Nonnull
  @Override
  public SqlOperator getOperator()
  {
    return OPERATOR;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    super.unparse(writer, leftPrec, rightPrec);
    if (partitionBy != null) {
      writer.keyword("PARTITION");
      writer.keyword("BY");
      writer.keyword(SqlLiteral.stringValue(partitionBy));
    }
    if (clusterBy != null) {
      writer.sep("CLUSTER BY");
      SqlWriter.Frame frame = writer.startList("", "");
      for (SqlNode clusterByOpts : clusterBy.getList()) {
        clusterByOpts.unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(frame);
    }
  }

}
