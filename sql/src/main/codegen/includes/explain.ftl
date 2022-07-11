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

/**
 * Parses an EXPLAIN PLAN statement. Allows for custom druid's statements as well.
 * The main change from SqlExplain() rule is that the statements that can occur in front of the explain's can now be
 * custom druid statements as well reflected in the DruidQueryOrSqlQueryOrDml() production rule
 *
 * Since this copies directly from SqlExplain(), this would need to be modified while updating Calcite to allow for
 * any changes and improvements (e.g. adding another format apart from json or xml in which one can
 * specify the explain plan output)
 */
SqlNode DruidSqlExplain() :
{
  SqlNode stmt;
  SqlExplainLevel detailLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
  SqlExplain.Depth depth;
  final SqlExplainFormat format;
}
{
  <EXPLAIN> <PLAN>
  [ detailLevel = ExplainDetailLevel() ]
  depth = ExplainDepth()
  (
    LOOKAHEAD(2)
    <AS> <XML> { format = SqlExplainFormat.XML; }
  |
    <AS> <JSON> { format = SqlExplainFormat.JSON; }
  |
    { format = SqlExplainFormat.TEXT; }
  )
  <FOR> stmt = DruidQueryOrSqlQueryOrDml() {
    return new SqlExplain(getPos(),
      stmt,
      detailLevel.symbol(SqlParserPos.ZERO),
      depth.symbol(SqlParserPos.ZERO),
      format.symbol(SqlParserPos.ZERO),
      nDynamicParams);
  }
}

SqlNode DruidQueryOrSqlQueryOrDml() :
{
  SqlNode stmt;
}
{
  (
    stmt = DruidSqlInsertEof()
  |
    stmt = DruidSqlReplaceEof()
  |
    stmt = SqlQueryOrDml()
  )
  {
    return stmt;
  }
}
