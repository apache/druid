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
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.druid.sql.calcite.planner.DruidSqlIngestOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Extends the 'replace' call to hold custom parameters specific to Druid i.e. PARTITIONED BY and the PARTITION SPECS
 * This class extends the {@link SqlInsert} so that this SqlNode can be used in
 * {@link org.apache.calcite.sql2rel.SqlToRelConverter} for getting converted into RelNode, and further processing
 */
public class DruidSqlReplace extends DruidSqlIngest
{
  public static final String SQL_REPLACE_TIME_CHUNKS = "sqlReplaceTimeChunks";

  public static final SqlOperator OPERATOR = DruidSqlIngestOperator.REPLACE_OPERATOR;

  private final SqlNode replaceTimeQuery;

  public static DruidSqlReplace create(
      @Nonnull SqlInsert insertNode,
      @Nullable SqlGranularityLiteral partitionedBy,
      @Nullable SqlNodeList clusteredBy,
      @Nullable SqlIdentifier exportFileFormat,
      @Nullable SqlNode replaceTimeQuery
  )
  {
    return new DruidSqlReplace(
        insertNode.getParserPosition(),
        (SqlNodeList) insertNode.getOperandList().get(0), // No better getter to extract this
        insertNode.getTargetTable(),
        insertNode.getSource(),
        insertNode.getTargetColumnList(),
        partitionedBy,
        clusteredBy,
        exportFileFormat,
        replaceTimeQuery
    );
  }

  /**
   * While partitionedBy can be null as arguments to the constructor, this is disallowed (semantically) and
   * {@link org.apache.druid.sql.calcite.planner.IngestHandler#validate()} performs checks to ensure that. This helps
   * in producing friendly errors when the PARTITIONED BY custom clause is not present, and keeps its error separate
   * from JavaCC/Calcite's custom errors which can be cryptic when someone accidentally forgets to explicitly specify
   * the PARTITIONED BY clause
   */
  public DruidSqlReplace(
      SqlParserPos pos,
      SqlNodeList keywords,
      SqlNode targetTable,
      SqlNode source,
      SqlNodeList columnList,
      @Nullable SqlGranularityLiteral partitionedBy,
      @Nullable SqlNodeList clusteredBy,
      @Nullable SqlIdentifier exportFileFormat,
      @Nullable SqlNode replaceTimeQuery
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

    this.replaceTimeQuery = replaceTimeQuery;
  }

  public SqlNode getReplaceTimeQuery()
  {
    return replaceTimeQuery;
  }

  @Nonnull
  @Override
  public SqlOperator getOperator()
  {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList()
  {
    return ImmutableNullableList.<SqlNode>builder()
        .addAll(super.getOperandList())
        .add(replaceTimeQuery)
        .build();
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.sep("REPLACE INTO");
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

    writer.keyword("OVERWRITE");
    if (replaceTimeQuery instanceof SqlLiteral) {
      writer.keyword("ALL");
    } else {
      replaceTimeQuery.unparse(writer, leftPrec, rightPrec);
    }
    writer.newlineAndIndent();

    getSource().unparse(writer, 0, 0);
    writer.newlineAndIndent();

    if (getPartitionedBy() != null) {
      writer.keyword("PARTITIONED BY");
      getPartitionedBy().unparse(writer, 0, 0);
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
