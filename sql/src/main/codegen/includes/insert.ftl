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
 * Parses an INSERT statement. This function is copied from SqlInsert in core/src/main/codegen/templates/Parser.jj,
 * with some changes to allow a custom error message if an OVERWRITE clause is present.
 */
SqlNode DruidSqlInsert() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    final SqlIdentifier tableName;
    SqlNode tableRef;
    SqlNode source;
    final SqlNodeList columnList;
    final Span s;
    final Pair<SqlNodeList, SqlNodeList> p;
}
{
    (
        <INSERT>
    |
        <UPSERT> { keywords.add(SqlInsertKeyword.UPSERT.symbol(getPos())); }
    )
    { s = span(); }
    SqlInsertKeywords(keywords) {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
    <INTO> tableName = CompoundTableIdentifier()
    ( tableRef = TableHints(tableName) | { tableRef = tableName; } )
    [ LOOKAHEAD(5) tableRef = ExtendTable(tableRef) ]
    (
        LOOKAHEAD(2)
        p = ParenthesizedCompoundIdentifierList() {
            if (p.right.size() > 0) {
                tableRef = extend(tableRef, p.right);
            }
            if (p.left.size() > 0) {
                columnList = p.left;
            } else {
                columnList = null;
            }
        }
    |   { columnList = null; }
    )
    (
    <OVERWRITE>
    {
        throw org.apache.druid.sql.calcite.parser.DruidSqlParserUtils.problemParsing(
            "An OVERWRITE clause is not allowed with INSERT statements. Use REPLACE statements if overwriting existing segments is required or remove the OVERWRITE clause."
        );
    }
    |
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new SqlInsert(s.end(source), keywordList, tableRef, source,
            columnList);
    }
    )
}

// Using fully qualified name for Pair class, since Calcite also has a same class name being used in the Parser.jj
SqlNode DruidSqlInsertEof() :
{
  SqlNode insertNode;
  org.apache.druid.java.util.common.Pair<Granularity, String> partitionedBy = new org.apache.druid.java.util.common.Pair(null, null);
  SqlNodeList clusteredBy = null;
}
{
  insertNode = DruidSqlInsert()
  // PARTITIONED BY is necessary, but is kept optional in the grammar. It is asserted that it is not missing in the
  // DruidSqlInsert constructor so that we can return a custom error message.
  [
    <PARTITIONED> <BY>
    partitionedBy = PartitionGranularity()
  ]
  [
    clusteredBy = ClusteredBy()
  ]
  {
      if (clusteredBy != null && partitionedBy.lhs == null) {
        throw org.apache.druid.sql.calcite.parser.DruidSqlParserUtils.problemParsing(
          "CLUSTERED BY found before PARTITIONED BY, CLUSTERED BY must come after the PARTITIONED BY clause"
        );
      }
  }
  // EOF is also present in SqlStmtEof but EOF is a special case and a single EOF can be consumed multiple times.
  // The reason for adding EOF here is to ensure that we create a DruidSqlInsert node after the syntax has been
  // validated and throw SQL syntax errors before performing validations in the DruidSqlInsert which can overshadow the
  // actual error message.
  <EOF>
  {
    if (!(insertNode instanceof SqlInsert)) {
      // This shouldn't be encountered, but done as a defensive practice. SqlInsert() always returns a node of type
      // SqlInsert
      return insertNode;
    }
    SqlInsert sqlInsert = (SqlInsert) insertNode;
    return new DruidSqlInsert(sqlInsert, partitionedBy.lhs, partitionedBy.rhs, clusteredBy);
  }
}
