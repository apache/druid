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
// Using fully qualified name for Pair class, since Calcite also has a same class name being used in the Parser.jj
SqlNode DruidSqlInsertEof() :
{
  SqlNode insertNode;
  final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
  final SqlNodeList keywordList;
  final SqlIdentifier destination;
  SqlNode tableRef = null;
  SqlNode source;
  final SqlNodeList columnList;
  final Span s;
  final Pair<SqlNodeList, SqlNodeList> p;
  SqlGranularityLiteral partitionedBy = null;
  SqlNodeList clusteredBy = null;
  SqlIdentifier exportFileFormat = null;
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
  <INTO>
  (
    LOOKAHEAD(2)
    <EXTERN> <LPAREN> destination = ExternalDestination() <RPAREN>
    |
    destination = CompoundTableIdentifier()
    ( tableRef = TableHints(destination) | { tableRef = destination; } )
    [ LOOKAHEAD(5) tableRef = ExtendTable(tableRef) ]
  )
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
    | { columnList = null; }
  )
  [
    <AS> exportFileFormat = FileFormat()
  ]
  (
    <OVERWRITE>
    {
      throw org.apache.druid.sql.calcite.parser.DruidSqlParserUtils.problemParsing(
          "An OVERWRITE clause is not allowed with INSERT statements. Use REPLACE statements if overwriting existing segments is required or remove the OVERWRITE clause."
      );
    }
    |
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
  )
  // PARTITIONED BY is necessary, but is kept optional in the grammar. It is asserted that it is not missing in the
  // IngestHandler#validate() so that we can return a custom error message.
  [
    <PARTITIONED> <BY>
    partitionedBy = PartitionGranularity()
  ]
  [
    clusteredBy = ClusteredBy()
  ]
  {
      if (clusteredBy != null && partitionedBy == null) {
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
    insertNode = new SqlInsert(s.end(source), keywordList, destination, source, columnList);
    if (!(insertNode instanceof SqlInsert)) {
      // This shouldn't be encountered, but done as a defensive practice. SqlInsert() always returns a node of type
      // SqlInsert
      return insertNode;
    }
    SqlInsert sqlInsert = (SqlInsert) insertNode;
    return DruidSqlInsert.create(sqlInsert, partitionedBy, clusteredBy, exportFileFormat);
  }
}
