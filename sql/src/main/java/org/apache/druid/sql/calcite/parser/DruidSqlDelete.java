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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.java.util.common.granularity.Granularity;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;

public class DruidSqlDelete extends DruidSqlIngest
{
  public static final SqlOperator OPERATOR = new SqlSpecialOperator("DELETE", SqlKind.OTHER);

  public static DruidSqlDelete create(
      SqlParserPos pos,
      SqlNode targetTable,
      SqlNode source,
      @Nullable Granularity partitionedBy,
      @Nullable String partitionedByStringForUnparse,
      @Nullable SqlNodeList clusteredBy
  )
  {
    SqlBasicCall invertedSource = new SqlBasicCall(SqlStdOperatorTable.NOT, new SqlNode[] {source}, pos);
    SqlSelect sqlSelect = new SqlSelect(
        pos,
        SqlNodeList.EMPTY,
        new SqlNodeList(Collections.singleton(SqlIdentifier.star(pos)), pos),
        targetTable,
        invertedSource,
        null,
        null,
        null,
        null,
        null,
        null
    );
    return new DruidSqlDelete(pos, targetTable, sqlSelect, partitionedBy, partitionedByStringForUnparse, clusteredBy);
  }

  public DruidSqlDelete(SqlParserPos pos,
                        SqlNode targetTable,
                        SqlNode source,
                        @Nullable Granularity partitionedBy,
                        @Nullable String partitionedByStringForUnparse,
                        @Nullable SqlNodeList clusteredBy
  )
  {
    super(
        pos,
        SqlNodeList.EMPTY,
        targetTable,
        source,
        null,
        partitionedBy,
        partitionedByStringForUnparse,
        clusteredBy
    );
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
    writer.keyword("PARTITIONED BY");
    writer.keyword(partitionedByStringForUnparse);
    if (getClusteredBy() != null) {
      writer.keyword("CLUSTERED BY");
      SqlWriter.Frame frame = writer.startList("", "");
      for (SqlNode clusterByOpts : getClusteredBy().getList()) {
        clusterByOpts.unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(frame);
    }
  }
}
