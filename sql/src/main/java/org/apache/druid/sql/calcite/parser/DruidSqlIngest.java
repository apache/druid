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
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Common base class to the two Druid "ingest" statements: INSERT and REPLACE.
 * Allows Planner code to work with these two statements generically where they
 * share common clauses.
 */
public abstract class DruidSqlIngest extends SqlInsert
{
  public static final String SQL_EXPORT_FILE_FORMAT = "__exportFileFormat";

  @Nullable
  protected final SqlGranularityLiteral partitionedBy;

  @Nullable
  protected final SqlNodeList clusteredBy;
  @Nullable
  private final SqlIdentifier exportFileFormat;

  public DruidSqlIngest(
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
    super(pos, keywords, targetTable, source, columnList);

    this.partitionedBy = partitionedBy;
    this.clusteredBy = clusteredBy;
    this.exportFileFormat = exportFileFormat;
  }

  @Nullable
  public SqlGranularityLiteral getPartitionedBy()
  {
    return partitionedBy;
  }

  @Nullable
  public SqlNodeList getClusteredBy()
  {
    return clusteredBy;
  }

  @Nullable
  public SqlIdentifier getExportFileFormat()
  {
    return exportFileFormat;
  }

  @Override
  public List<SqlNode> getOperandList()
  {
    return ImmutableNullableList.<SqlNode>builder()
        .addAll(super.getOperandList())
        .add(partitionedBy)
        .add(clusteredBy)
        .add(exportFileFormat)
        .build();
  }
}
