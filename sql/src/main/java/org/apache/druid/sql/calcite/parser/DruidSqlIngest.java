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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.java.util.common.granularity.Granularity;

import javax.annotation.Nullable;

/**
 * Common base class to the two Druid "ingest" statements: INSERT and REPLACE.
 * Allows Planner code to work with these two statements generically where they
 * share common clauses.
 */
public abstract class DruidSqlIngest extends SqlInsert
{
  protected final Granularity partitionedBy;

  // Used in the unparse function to generate the original query since we convert the string to an enum
  protected final String partitionedByStringForUnparse;

  @Nullable
  protected final SqlNodeList clusteredBy;

  public DruidSqlIngest(SqlParserPos pos,
      SqlNodeList keywords,
      SqlNode targetTable,
      SqlNode source,
      SqlNodeList columnList,
      @Nullable Granularity partitionedBy,
      @Nullable String partitionedByStringForUnparse,
      @Nullable SqlNodeList clusteredBy
  )
  {
    super(pos, keywords, targetTable, source, columnList);

    this.partitionedByStringForUnparse = partitionedByStringForUnparse;
    this.partitionedBy = partitionedBy;
    this.clusteredBy = clusteredBy;
  }

  public Granularity getPartitionedBy()
  {
    return partitionedBy;
  }

  @Nullable
  public SqlNodeList getClusteredBy()
  {
    return clusteredBy;
  }
}
