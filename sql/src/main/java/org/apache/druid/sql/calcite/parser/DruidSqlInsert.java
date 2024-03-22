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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.sql.calcite.planner.DruidSqlIngestOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Extends the 'insert' call to hold custom parameters specific to Druid i.e. PARTITIONED BY and CLUSTERED BY
 * This class extends the {@link DruidSqlIngest} so that this SqlNode can be used in
 * {@link org.apache.calcite.sql2rel.SqlToRelConverter} for getting converted into RelNode, and further processing
 */
public class DruidSqlInsert extends DruidSqlIngest
{
  public static final String SQL_INSERT_SEGMENT_GRANULARITY = "sqlInsertSegmentGranularity";

  // This allows reusing super.unparse
  public static final SqlOperator OPERATOR = DruidSqlIngestOperator.INSERT_OPERATOR;

  public static DruidSqlInsert create(
      @Nonnull SqlInsert insertNode,
      @Nullable SqlGranularityLiteral partitionedBy,
      @Nullable SqlNodeList clusteredBy,
      @Nullable SqlIdentifier exportFileFormat
  )
  {
    return new DruidSqlInsert(
        insertNode.getParserPosition(),
        (SqlNodeList) insertNode.getOperandList().get(0), // No better getter to extract this
        insertNode.getTargetTable(),
        insertNode.getSource(),
        insertNode.getTargetColumnList(),
        partitionedBy,
        clusteredBy,
        exportFileFormat
    );
  }

  /**
   * While partitionedBy can be null as arguments to the constructor, this is disallowed (semantically) and
   * {@link org.apache.druid.sql.calcite.planner.IngestHandler#validate()} performs checks to ensure that. This helps
   * in producing friendly errors when the PARTITIONED BY custom clause is not present, and keeps its error separate
   * from JavaCC/Calcite's custom errors which can be cryptic when someone accidentally forgets to explicitly specify
   * the PARTITIONED BY clause
   */
  public DruidSqlInsert(
      SqlParserPos pos,
      SqlNodeList keywords,
      SqlNode targetTable,
      SqlNode source,
      SqlNodeList columnList,
      @Nullable SqlGranularityLiteral partitionedBy,
      @Nullable SqlNodeList clusteredBy,
      @Nullable SqlIdentifier exportFileFormat
  )
  {
    super(
        pos,
        keywords,
        targetTable,
        source,
        columnList,
        partitionedBy,
        clusteredBy,
        exportFileFormat
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
    writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.sep(isUpsert() ? "UPSERT INTO" : "INSERT INTO");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    getTargetTable().unparse(writer, opLeft, opRight);
    if (getTargetColumnList() != null) {
      getTargetColumnList().unparse(writer, opLeft, opRight);
    }
    writer.newlineAndIndent();
    if (getExportFileFormat() != null) {
      writer.keyword("AS");
      writer.print(getExportFileFormat().toString());
      writer.newlineAndIndent();
    }
    getSource().unparse(writer, 0, 0);
    writer.newlineAndIndent();

    if (getPartitionedBy() != null) {
      writer.keyword("PARTITIONED BY");
      getPartitionedBy().unparse(writer, leftPrec, rightPrec);
    }

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
